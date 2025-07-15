use futures_util::stream::iter;
use futures_util::StreamExt;
use ractor::concurrency::{sleep, Duration, Instant, JoinHandle};
use ractor::{
    call, Actor, ActorCell, ActorName, ActorProcessingErr, ActorRef, RpcReplyPort, SpawnErr,
    SupervisionEvent,
};
use std::collections::HashMap;

use crate::core::{
    ChildFailureState, ChildSpec, CoreSupervisorOptions, RestartLog, SupervisorCore,
    SupervisorError,
};
use crate::ExitReason;

#[derive(Debug, Clone)]
pub struct DynamicSupervisorOptions {
    pub max_children: Option<usize>,
    pub max_restarts: usize,
    pub max_window: Duration,
    pub reset_after: Option<Duration>,
}

#[derive(Clone, Debug)]
pub struct DynamicSupervisorState {
    pub child_failure_state: HashMap<String, ChildFailureState>,
    pub restart_log: Vec<RestartLog>,
    pub options: DynamicSupervisorOptions,
    pub active_children: HashMap<String, ActiveChild>,
}

#[derive(Clone, Debug)]
pub struct ActiveChild {
    pub spec: ChildSpec,
    pub cell: ActorCell,
}

pub enum DynamicSupervisorMsg {
    SpawnChild {
        spec: ChildSpec,
        reply: Option<RpcReplyPort<Result<(), ActorProcessingErr>>>,
    },
    TerminateChild {
        child_id: String,
        reply: Option<RpcReplyPort<()>>,
    },
    InspectState(RpcReplyPort<DynamicSupervisorState>),
}

impl CoreSupervisorOptions<()> for DynamicSupervisorOptions {
    fn max_restarts(&self) -> usize {
        self.max_restarts
    }

    fn max_window(&self) -> Duration {
        self.max_window
    }

    fn reset_after(&self) -> Option<Duration> {
        self.reset_after
    }

    fn strategy(&self) {}
}

impl SupervisorCore for DynamicSupervisorState {
    type Message = DynamicSupervisorMsg;
    type Options = DynamicSupervisorOptions;
    type Strategy = (); // Uses implicit OneForOne

    fn child_failure_state(&mut self) -> &mut HashMap<String, ChildFailureState> {
        &mut self.child_failure_state
    }

    fn restart_log(&mut self) -> &mut Vec<RestartLog> {
        &mut self.restart_log
    }

    fn options(&self) -> &DynamicSupervisorOptions {
        &self.options
    }

    fn restart_msg(
        &self,
        child_spec: &ChildSpec,
        _strategy: (),
        _myself: ActorRef<Self::Message>,
    ) -> Self::Message {
        DynamicSupervisorMsg::SpawnChild {
            spec: child_spec.clone(),
            reply: None,
        }
    }
}

type DynamicSupervisorArguments = DynamicSupervisorOptions;

#[cfg_attr(feature = "async-trait", ractor::async_trait)]
impl Actor for DynamicSupervisor {
    type Msg = DynamicSupervisorMsg;
    type State = DynamicSupervisorState;
    type Arguments = DynamicSupervisorArguments;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        options: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        log::trace!("starting...");
        Ok(DynamicSupervisorState {
            child_failure_state: HashMap::new(),
            restart_log: Vec::new(),
            active_children: HashMap::new(),
            options,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        msg: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let res = match msg {
            DynamicSupervisorMsg::SpawnChild { spec, reply } => {
                log::trace!("received message: SpawnChild({spec:?}, ...)");
                let mut res = self
                    .handle_spawn_child(&spec, reply.is_some(), state, myself.clone())
                    .await;

                if let Some(reply) = reply {
                    reply.send(res)?;
                    res = Ok(()); // Clear the error for the main handler
                }
                res
            }
            DynamicSupervisorMsg::TerminateChild { child_id, reply } => {
                log::trace!("received message: TerminateChild({child_id:?}, ...)");
                self.handle_terminate_child(&child_id, state, myself.clone())
                    .await;
                if let Some(reply) = reply {
                    reply.send(())?;
                }
                Ok(())
            }
            DynamicSupervisorMsg::InspectState(reply) => {
                log::trace!("received message: InspectState(...)");
                reply.send(state.clone())?;
                Ok(())
            }
        };

        #[cfg(test)]
        {
            store_final_state(myself, state).await;
        }

        res
    }

    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        evt: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        log::trace!("received supervisor event: {evt:?}");
        match evt {
            SupervisionEvent::ActorStarted(cell) => {
                let child_id = cell
                    .get_name()
                    .ok_or(SupervisorError::ChildNameNotSet { pid: cell.get_id() })?;
                log::info!("Started child: {}", child_id);
                if state.active_children.contains_key(&child_id) {
                    // This is a child we know about, so we track it
                    state
                        .child_failure_state
                        .entry(child_id.clone())
                        .or_insert_with(|| ChildFailureState {
                            restart_count: 0,
                            last_fail_instant: Instant::now(),
                        });
                }
            }
            SupervisionEvent::ActorTerminated(cell, _final_state, reason) => {
                self.handle_child_restart(cell, false, state, myself, &ExitReason::Reason(reason))?;
            }
            SupervisionEvent::ActorFailed(cell, err) => {
                self.handle_child_restart(cell, true, state, myself, &ExitReason::Error(err))?;
            }
            SupervisionEvent::ProcessGroupChanged(_group) => {}
        }
        Ok(())
    }

    /// Called if the supervisor stops normally (e.g. `.stop(None)`).
    /// For meltdown stops, we skip this, but we still store final state for testing.
    async fn post_stop(
        &self,
        myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        log::trace!("stopping...");
        #[cfg(test)]
        {
            store_final_state(_myself, _state).await;
        }

        // Stop or kill all children.
        iter(&myself.get_children())
            .for_each_concurrent(None, |cell| {
                let myself = myself.clone();
                async move {
                    log::debug!("stopping child {cell:?}");

                    // Must unlink to prevent confusion with them receiving further messages.
                    cell.unlink(myself.get_cell());

                    // Allow the children to gracefully exit, murder them if they don't comply.
                    if cell
                        .stop_and_wait(None, Some(Duration::from_millis(100)))
                        .await
                        .is_err()
                    {
                        log::warn!("failed to stop child {cell:?}, killing...");
                        cell.kill();
                    }
                }
            })
            .await;

        log::trace!("stopped.");
        Ok(())
    }
}

pub struct DynamicSupervisor;

impl DynamicSupervisor {
    pub async fn spawn(
        name: ActorName,
        startup_args: DynamicSupervisorArguments,
    ) -> Result<(ActorRef<DynamicSupervisorMsg>, JoinHandle<()>), SpawnErr> {
        Actor::spawn(Some(name), DynamicSupervisor, startup_args).await
    }

    pub async fn spawn_linked<T: Actor>(
        name: ActorName,
        handler: T,
        startup_args: T::Arguments,
        supervisor: ActorCell,
    ) -> Result<(ActorRef<T::Msg>, JoinHandle<()>), SpawnErr> {
        Actor::spawn_linked(Some(name), handler, startup_args, supervisor).await
    }

    pub async fn spawn_child(
        sup_ref: ActorRef<DynamicSupervisorMsg>,
        spec: ChildSpec,
    ) -> Result<(), ActorProcessingErr> {
        call!(sup_ref, |reply| {
            DynamicSupervisorMsg::SpawnChild {
                spec,
                reply: Some(reply),
            }
        })?
    }

    pub async fn terminate_child(
        sup_ref: ActorRef<DynamicSupervisorMsg>,
        child_id: String,
    ) -> Result<(), ActorProcessingErr> {
        call!(sup_ref, |reply| {
            DynamicSupervisorMsg::TerminateChild {
                child_id,
                reply: Some(reply),
            }
        })?;

        Ok(())
    }

    async fn handle_spawn_child(
        &self,
        spec: &ChildSpec,
        first_start: bool,
        state: &mut DynamicSupervisorState,
        myself: ActorRef<DynamicSupervisorMsg>,
    ) -> Result<(), ActorProcessingErr> {
        if !first_start {
            state.track_global_restart(&spec.id)?;
            sleep(Duration::from_millis(10)).await;
        }

        // Check max children
        if let Some(max) = state.options.max_children {
            if state.active_children.len() >= max {
                return Err(SupervisorError::Meltdown {
                    reason: "max_children exceeded".to_string(),
                }
                .into());
            }
        }

        // Spawn child
        let result = spec
            .spawn_fn
            .call(myself.get_cell().clone(), spec.id.clone())
            .await
            .map_err(|e| SupervisorError::ChildSpawnError {
                child_id: spec.id.clone(),
                reason: e.to_string(),
            });

        match result {
            Ok(child_cell) => {
                // (1) Track the child in `active_children`
                state.active_children.insert(
                    spec.id.clone(),
                    ActiveChild {
                        cell: child_cell.clone(),
                        spec: spec.clone(),
                    },
                );

                // (2) ALSO populate child_failure_state, so meltdown logic works
                state
                    .child_failure_state
                    .entry(spec.id.clone())
                    .or_insert_with(|| ChildFailureState {
                        restart_count: 0,
                        last_fail_instant: Instant::now(),
                    });
            }
            Err(err) => {
                state
                    .handle_child_restart(spec, true, myself, &ExitReason::Error(err.into()))
                    .map_err(|e| SupervisorError::ChildSpawnError {
                        child_id: spec.id.clone(),
                        reason: e.to_string(),
                    })?;
            }
        }

        Ok(())
    }

    async fn handle_terminate_child(
        &self,
        child_id: &str,
        state: &mut DynamicSupervisorState,
        myself: ActorRef<DynamicSupervisorMsg>,
    ) {
        if let Some(child) = state.active_children.remove(child_id) {
            log::trace!("stopping child {child:?}");
            child.cell.unlink(myself.get_cell());
            if child
                .cell
                .stop_and_wait(None, Some(Duration::from_millis(100)))
                .await
                .is_err()
            {
                log::warn!("failed to stop child {child:?}, killing...");
                child.cell.kill();
            }
        }
    }

    fn handle_child_restart(
        &self,
        cell: ActorCell,
        abnormal: bool,
        state: &mut DynamicSupervisorState,
        myself: ActorRef<DynamicSupervisorMsg>,
        reason: &ExitReason,
    ) -> Result<(), ActorProcessingErr> {
        let child_id = cell
            .get_name()
            .ok_or(SupervisorError::ChildNameNotSet { pid: cell.get_id() })?;

        let child =
            state
                .active_children
                .remove(&child_id)
                .ok_or(SupervisorError::ChildNotFound {
                    child_id: child_id.clone(),
                })?;

        state.handle_child_restart(&child.spec, abnormal, myself, reason)?;

        Ok(())
    }
}

/// A global map for test usage, storing final states after each handle call and in `post_stop`.
#[cfg(test)]
static SUPERVISOR_FINAL: std::sync::OnceLock<
    tokio::sync::Mutex<HashMap<String, DynamicSupervisorState>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
async fn store_final_state(myself: ActorRef<DynamicSupervisorMsg>, state: &DynamicSupervisorState) {
    let mut map = SUPERVISOR_FINAL
        .get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
        .lock()
        .await;
    if let Some(name) = myself.get_name() {
        map.insert(name, state.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ChildBackoffFn, ChildSpec, Restart};
    use crate::SpawnFn;
    use ractor::concurrency::{sleep, Duration, Instant};
    use ractor::{call_t, Actor, ActorCell, ActorRef, ActorStatus};
    use serial_test::serial;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    #[cfg(test)]
    static ACTOR_CALL_COUNT: std::sync::OnceLock<
        tokio::sync::Mutex<std::collections::HashMap<String, u64>>,
    > = std::sync::OnceLock::new();

    async fn before_each() {
        // Clear the final supervisor state map (test usage)
        if let Some(map) = SUPERVISOR_FINAL.get() {
            let mut map = map.lock().await;
            map.clear();
        }
        // Clear our actor call counts
        if let Some(map) = ACTOR_CALL_COUNT.get() {
            let mut map = map.lock().await;
            map.clear();
        }
        sleep(Duration::from_millis(10)).await;
    }

    async fn increment_actor_count(child_id: &str) {
        let mut map = ACTOR_CALL_COUNT
            .get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
            .lock()
            .await;
        *map.entry(child_id.to_string()).or_default() += 1;
    }

    /// Utility to read the final state of a named supervisor after it stops.
    async fn read_final_supervisor_state(sup_name: &str) -> DynamicSupervisorState {
        let map = SUPERVISOR_FINAL
            .get()
            .expect("SUPERVISOR_FINAL not initialized!")
            .lock()
            .await;
        map.get(sup_name)
            .cloned()
            .unwrap_or_else(|| panic!("No final state for supervisor '{sup_name}'"))
    }

    async fn read_actor_call_count(child_id: &str) -> u64 {
        let map = ACTOR_CALL_COUNT
            .get()
            .expect("ACTOR_CALL_COUNT not initialized!")
            .lock()
            .await;
        *map.get(child_id)
            .unwrap_or_else(|| panic!("No actor call count for child '{child_id}'"))
    }

    // Child behaviors for tests
    #[derive(Clone)]
    pub enum ChildBehavior {
        DelayedFail {
            ms: u64,
        },
        DelayedNormal {
            ms: u64,
        },
        ImmediateFail,
        ImmediateNormal,
        CountedFails {
            delay_ms: u64,
            fail_count: u64,
            current: Arc<AtomicU64>,
        },
        FailWaitFail {
            initial_fails: u64,
            wait_ms: u64,
            final_fails: u64,
            current: Arc<AtomicU64>,
        },
    }

    pub struct TestChild;

    #[cfg_attr(feature = "async-trait", ractor::async_trait)]
    impl Actor for TestChild {
        type Msg = ();
        type State = ChildBehavior;
        type Arguments = ChildBehavior;

        async fn pre_start(
            &self,
            myself: ActorRef<Self::Msg>,
            arg: Self::Arguments,
        ) -> Result<Self::State, ractor::ActorProcessingErr> {
            // Track how many times this particular child-id was started
            increment_actor_count(myself.get_name().unwrap().as_str()).await;
            match arg {
                ChildBehavior::DelayedFail { ms } => {
                    myself.send_after(Duration::from_millis(ms), || ());
                }
                ChildBehavior::DelayedNormal { ms } => {
                    myself.send_after(Duration::from_millis(ms), || ());
                }
                ChildBehavior::ImmediateFail => panic!("Immediate fail => ActorFailed"),
                ChildBehavior::ImmediateNormal => myself.stop(None),
                ChildBehavior::CountedFails { delay_ms, .. } => {
                    myself.send_after(Duration::from_millis(delay_ms), || ());
                }
                ChildBehavior::FailWaitFail { .. } => {
                    // Kick off our chain of fails by sending a first message
                    myself.cast(())?;
                }
            }
            Ok(arg)
        }

        async fn handle(
            &self,
            myself: ActorRef<Self::Msg>,
            _msg: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ractor::ActorProcessingErr> {
            match state {
                ChildBehavior::DelayedFail { .. } => panic!("Delayed fail => ActorFailed"),
                ChildBehavior::DelayedNormal { .. } => myself.stop(None),
                ChildBehavior::ImmediateFail => panic!("ImmediateFail => ActorFailed"),
                ChildBehavior::ImmediateNormal => myself.stop(None),
                ChildBehavior::CountedFails {
                    fail_count,
                    current,
                    ..
                } => {
                    let old = current.fetch_add(1, Ordering::SeqCst);
                    let newv = old + 1;
                    if newv <= *fail_count {
                        panic!("CountedFails => fail #{newv}");
                    }
                }
                ChildBehavior::FailWaitFail {
                    initial_fails,
                    wait_ms,
                    final_fails,
                    current,
                } => {
                    let so_far = current.fetch_add(1, Ordering::SeqCst) + 1;
                    if so_far <= *initial_fails {
                        panic!("FailWaitFail => initial fail #{so_far}");
                    } else if so_far == *initial_fails + 1 {
                        // wait some ms => schedule next message => final fails
                        myself.send_after(Duration::from_millis(*wait_ms), || ());
                    } else {
                        let n = so_far - (*initial_fails + 1);
                        if n <= *final_fails {
                            panic!("FailWaitFail => final fail #{n}");
                        }
                    }
                }
            }
            Ok(())
        }
    }

    fn get_running_children(
        sup_ref: &ActorRef<DynamicSupervisorMsg>,
    ) -> HashMap<String, ActorCell> {
        sup_ref
            .get_children()
            .into_iter()
            .filter_map(|c| {
                if c.get_status() == ActorStatus::Running {
                    c.get_name().map(|n| (n, c))
                } else {
                    None
                }
            })
            .collect()
    }

    // Helper for spawning our test child
    async fn spawn_test_child(
        sup_cell: ActorCell,
        id: String,
        behavior: ChildBehavior,
    ) -> Result<ActorCell, SpawnErr> {
        let (ch_ref, _join) =
            DynamicSupervisor::spawn_linked(id, TestChild, behavior, sup_cell).await?;
        Ok(ch_ref.get_cell())
    }

    // Reusable child spec
    fn make_child_spec(id: &str, restart: Restart, behavior: ChildBehavior) -> ChildSpec {
        ChildSpec {
            id: id.to_string(),
            restart,
            spawn_fn: SpawnFn::new(move |sup_cell, child_id| {
                spawn_test_child(sup_cell, child_id, behavior.clone())
            }),
            backoff_fn: None, // by default no per-child backoff
            reset_after: None,
        }
    }

    /// Helper to read the dynamic supervisor's state via `InspectState`.
    async fn inspect_supervisor(
        sup_ref: &ActorRef<DynamicSupervisorMsg>,
    ) -> DynamicSupervisorState {
        call_t!(sup_ref, DynamicSupervisorMsg::InspectState, 500).unwrap()
    }

    /// Basic test: spawn a child that exits normally (`DelayedNormal`).
    /// No meltdown, no restarts.
    #[ractor::concurrency::test]
    #[serial]
    async fn test_transient_child_normal_exit() -> Result<(), ActorProcessingErr> {
        before_each().await;
        let options = DynamicSupervisorOptions {
            max_children: None,
            max_restarts: 5,
            max_window: Duration::from_secs(5),
            reset_after: None,
        };
        let (sup_ref, sup_handle) =
            DynamicSupervisor::spawn("test_dynamic_normal_exit".into(), options).await?;
        // Spawn a child that will exit after 200ms
        let child_spec = make_child_spec(
            "normal-dynamic",
            Restart::Transient,
            ChildBehavior::DelayedNormal { ms: 200 },
        );
        DynamicSupervisor::spawn_child(sup_ref.clone(), child_spec).await?;
        // Let the child exit
        sleep(Duration::from_millis(300)).await;
        // Confirm child is gone; no meltdown triggered
        let sup_state = inspect_supervisor(&sup_ref).await;
        assert_eq!(
            sup_ref.get_status(),
            ActorStatus::Running,
            "Supervisor still running"
        );
        assert!(
            sup_state.active_children.is_empty(),
            "Child should have exited normally"
        );
        assert!(
            get_running_children(&sup_ref).is_empty(),
            "No children running"
        );
        assert!(sup_state.restart_log.is_empty(), "No restarts expected");
        // Stop supervisor
        sup_ref.stop(None);
        let _ = sup_handle.await;
        // Validate final state
        let final_st = read_final_supervisor_state("test_dynamic_normal_exit").await;
        assert!(final_st.restart_log.is_empty());
        assert_eq!(
            read_actor_call_count("normal-dynamic").await,
            1,
            "Spawned exactly once"
        );
        Ok(())
    }

    /// If a permanent child fails, it should restart until `max_restarts` is exceeded,
    /// at which point meltdown stops the supervisor.
    #[ractor::concurrency::test]
    #[serial]
    async fn test_permanent_child_meltdown() -> Result<(), ActorProcessingErr> {
        before_each().await;
        let options = DynamicSupervisorOptions {
            // meltdown after 1 (the second) fail in <= 2s
            max_children: None,
            max_restarts: 1,
            max_window: Duration::from_secs(2),
            reset_after: None,
        };
        let (sup_ref, sup_handle) =
            DynamicSupervisor::spawn("test_permanent_child_meltdown".into(), options).await?;
        let fail_child_spec = make_child_spec(
            "fail-child",
            Restart::Permanent,
            ChildBehavior::ImmediateFail,
        );
        DynamicSupervisor::spawn_child(sup_ref.clone(), fail_child_spec).await?;
        let _ = sup_handle.await;
        assert_eq!(
            sup_ref.get_status(),
            ActorStatus::Stopped,
            "Supervisor meltdown expected"
        );
        // Final checks
        let final_state = read_final_supervisor_state("test_permanent_child_meltdown").await;
        // meltdown => 2 fails in the log
        assert!(
            final_state.restart_log.len() == 2,
            "Expected at least 2 restarts leading to meltdown"
        );
        // The child was started twice
        assert_eq!(read_actor_call_count("fail-child").await, 2);
        Ok(())
    }

    /// If a `Temporary` child fails, it should never restart.
    /// The supervisor remains running (no meltdown) unless the number of restarts is already at meltdown threshold.
    #[ractor::concurrency::test]
    #[serial]
    async fn test_temporary_child_fail_no_restart() -> Result<(), ActorProcessingErr> {
        before_each().await;
        let options = DynamicSupervisorOptions {
            max_children: None,
            max_restarts: 10,
            max_window: Duration::from_secs(10),
            reset_after: None,
        };
        let (sup_ref, sup_handle) =
            DynamicSupervisor::spawn("test_temporary_child_fail_no_restart".into(), options)
                .await?;
        let temp_child = make_child_spec(
            "temp-fail",
            Restart::Temporary,
            ChildBehavior::DelayedFail { ms: 200 },
        );
        DynamicSupervisor::spawn_child(sup_ref.clone(), temp_child).await?;
        // Let the child fail
        sleep(Duration::from_millis(400)).await;
        // Supervisor still running, child gone
        assert_eq!(sup_ref.get_status(), ActorStatus::Running);
        let sup_state = inspect_supervisor(&sup_ref).await;
        assert!(
            sup_state.active_children.is_empty(),
            "Temporary child not restarted"
        );
        assert!(
            get_running_children(&sup_ref).is_empty(),
            "No children running"
        );
        sup_ref.stop(None);
        let _ = sup_handle.await;
        let final_st = read_final_supervisor_state("test_temporary_child_fail_no_restart").await;
        assert_eq!(final_st.restart_log.len(), 0, "No restarts occurred");
        assert_eq!(read_actor_call_count("temp-fail").await, 1);
        Ok(())
    }

    /// Ensure that if a meltdown threshold is set (max_restarts + max_window),
    /// multiple quick fails in that window cause meltdown.
    #[ractor::concurrency::test]
    #[serial]
    async fn test_max_restarts_in_time_window() -> Result<(), ActorProcessingErr> {
        before_each().await;
        // meltdown on 3 fails in <=1s => max_restarts=2 => meltdown on the 3rd
        let options = DynamicSupervisorOptions {
            max_children: None,
            max_restarts: 2,
            max_window: Duration::from_secs(1),
            reset_after: None,
        };
        let (sup_ref, sup_handle) =
            DynamicSupervisor::spawn("test_dynamic_max_restarts_in_time_window".into(), options)
                .await?;
        let child_spec =
            make_child_spec("fastfail", Restart::Permanent, ChildBehavior::ImmediateFail);
        // This child fails immediately on start => triggers multiple restarts quickly
        DynamicSupervisor::spawn_child(sup_ref.clone(), child_spec).await?;
        // Wait for meltdown
        let _ = sup_handle.await;
        assert_eq!(sup_ref.get_status(), ActorStatus::Stopped);
        let final_state =
            read_final_supervisor_state("test_dynamic_max_restarts_in_time_window").await;
        assert_eq!(
            final_state.restart_log.len(),
            3,
            "Should see 3 fails in meltdown window"
        );
        assert_eq!(read_actor_call_count("fastfail").await, 3);
        Ok(())
    }

    /// Tests the optional `reset_after` at the **supervisor** level,
    /// ensuring that if there's a quiet period, the meltdown log is cleared.
    #[ractor::concurrency::test]
    #[serial]
    async fn test_supervisor_restart_counter_reset_after() -> Result<(), ActorProcessingErr> {
        before_each().await;
        // meltdown on 3 fails in <= 10s (max_restarts=2 => meltdown on #3)
        // but if quiet >=2s => meltdown log is cleared
        let options = DynamicSupervisorOptions {
            max_children: None,
            max_restarts: 2,
            max_window: Duration::from_secs(10),
            reset_after: Some(Duration::from_secs(2)),
        };
        let (sup_ref, sup_handle) = DynamicSupervisor::spawn(
            "test_supervisor_restart_counter_reset_after".into(),
            options,
        )
        .await?;
        // We'll use a child that fails 2 times quickly, then is quiet 3s, then fails again
        // The meltdown log should get cleared after 3s => final fail sees meltdown log=1 => no meltdown
        let behavior = ChildBehavior::FailWaitFail {
            initial_fails: 2,
            wait_ms: 3000,
            final_fails: 1,
            current: Arc::new(AtomicU64::new(0)),
        };
        let child_spec = make_child_spec("reset-test", Restart::Permanent, behavior);
        DynamicSupervisor::spawn_child(sup_ref.clone(), child_spec).await?;
        // Let the child do its thing: 2 fails quickly => then quiet => final fail
        sleep(Duration::from_secs(4)).await;
        // The supervisor should still be alive
        assert_eq!(sup_ref.get_status(), ActorStatus::Running);
        sup_ref.stop(None);
        let _ = sup_handle.await;
        // meltdown log is reset after the quiet period => only the final fail is logged
        let final_st =
            read_final_supervisor_state("test_supervisor_restart_counter_reset_after").await;
        assert_eq!(
            final_st.restart_log.len(),
            1,
            "Only the final fail is in meltdown log"
        );
        // The child started 4 times total
        assert_eq!(read_actor_call_count("reset-test").await, 4);
        Ok(())
    }

    /// Tests the child-level `reset_after`, ensuring that a quiet period
    /// resets that **child's** fail count. The result is no meltdown if a subsequent fail
    /// happens after the quiet window.
    #[ractor::concurrency::test]
    #[serial]
    async fn test_child_level_restart_counter_reset_after() -> Result<(), ActorProcessingErr> {
        before_each().await;
        // We'll set a large meltdown threshold so we don't meltdown unless the child's restarts remain consecutive.
        let options = DynamicSupervisorOptions {
            max_children: None,
            max_restarts: 5,
            max_window: Duration::from_secs(30),
            reset_after: None,
        };
        let (sup_ref, sup_handle) = DynamicSupervisor::spawn(
            "test_dynamic_child_level_restart_counter_reset_after".into(),
            options,
        )
        .await?;
        // The child fails 2 times quickly => quiet 3s => final fail => from that child's perspective, it starts from 0 again
        let behavior = ChildBehavior::FailWaitFail {
            initial_fails: 2,
            wait_ms: 3000,
            final_fails: 1,
            current: Arc::new(AtomicU64::new(0)),
        };
        let mut child_spec = make_child_spec("child-reset", Restart::Permanent, behavior);
        // **Per-child** reset
        child_spec.reset_after = Some(Duration::from_secs(2));
        DynamicSupervisor::spawn_child(sup_ref.clone(), child_spec).await?;
        // Wait for child to do the fails & quiet period
        sleep(Duration::from_secs(5)).await;
        // No meltdown => supervisor still up
        assert_eq!(sup_ref.get_status(), ActorStatus::Running);
        sup_ref.stop(None);
        let _ = sup_handle.await;
        let final_st =
            read_final_supervisor_state("test_dynamic_child_level_restart_counter_reset_after")
                .await;
        let cfs = final_st.child_failure_state.get("child-reset").unwrap();
        // The final fail saw a reset => the child's restart_count ended up at 1
        assert_eq!(cfs.restart_count, 1);
        // total spawns = 4
        assert_eq!(read_actor_call_count("child-reset").await, 4);
        Ok(())
    }

    /// Tests that a child-level backoff function can delay restarts.
    #[ractor::concurrency::test]
    #[serial]
    async fn test_child_level_backoff_fn_delays_restart() -> Result<(), ActorProcessingErr> {
        before_each().await;
        // meltdown on 2nd fail => max_restarts=1 => meltdown on fail #2
        // but we do a 1-second child-level backoff for the second spawn
        let options = DynamicSupervisorOptions {
            max_children: None,
            max_restarts: 1,
            max_window: Duration::from_secs(10),
            reset_after: None,
        };
        let (sup_ref, sup_handle) =
            DynamicSupervisor::spawn("test_dynamic_child_backoff".to_string(), options).await?;
        // Our backoff function that returns Some(1s) after the first restart
        let backoff_fn: ChildBackoffFn = ChildBackoffFn::new(|_id, count, _last, _child_reset| {
            if count <= 1 {
                None
            } else {
                Some(Duration::from_secs(1))
            }
        });
        let mut child_spec = make_child_spec(
            "backoff-child",
            Restart::Permanent,
            ChildBehavior::ImmediateFail,
        );
        child_spec.backoff_fn = Some(backoff_fn);
        let start_instant = Instant::now();
        DynamicSupervisor::spawn_child(sup_ref.clone(), child_spec).await?;
        // Wait for meltdown
        let _ = sup_handle.await;
        let elapsed = start_instant.elapsed();
        // Because meltdown occurs on the second fail => we must have waited 1s for that second attempt
        assert!(
            elapsed >= Duration::from_secs(1),
            "Expected at least 1s of backoff before meltdown"
        );
        let final_st = read_final_supervisor_state("test_dynamic_child_backoff").await;
        assert_eq!(final_st.restart_log.len(), 2, "two fails => meltdown on #2");
        assert_eq!(read_actor_call_count("backoff-child").await, 2);
        Ok(())
    }

    /// Tests dynamic supervisor's `max_children`: attempting to spawn more children
    /// than allowed should return an immediate `Meltdown` error (the supervisor stops).
    #[ractor::concurrency::test]
    #[serial]
    async fn test_exceed_max_children() -> Result<(), ActorProcessingErr> {
        before_each().await;
        let options = DynamicSupervisorOptions {
            max_children: Some(1), // only 1 child allowed
            max_restarts: 999,
            max_window: Duration::from_secs(999),
            reset_after: None,
        };
        let (sup_ref, sup_handle) =
            DynamicSupervisor::spawn("test_exceed_max_children".to_string(), options).await?;
        let spec_1 = make_child_spec(
            "allowed-child",
            Restart::Permanent,
            ChildBehavior::DelayedNormal { ms: 500 },
        );
        let spec_2 = make_child_spec(
            "unallowed-child",
            Restart::Permanent,
            ChildBehavior::DelayedNormal { ms: 500 },
        );
        // first child is fine
        DynamicSupervisor::spawn_child(sup_ref.clone(), spec_1).await?;
        // second child => should fail
        let result = DynamicSupervisor::spawn_child(sup_ref.clone(), spec_2).await;
        assert!(
            result.is_err(),
            "Spawning a second child should fail due to max_children=1"
        );
        let final_st = read_final_supervisor_state("test_exceed_max_children").await;
        assert_eq!(
            final_st.active_children.len(),
            1,
            "Second child should not have been spawned"
        );
        assert!(
            get_running_children(&sup_ref).len() == 1,
            "Only one child running"
        );
        sup_ref.stop(None);
        let _ = sup_handle.await;
        Ok(())
    }

    /// Tests that `TerminateChild` kills a running child; no meltdown or restarts.
    #[ractor::concurrency::test]
    #[serial]
    async fn test_terminate_child() -> Result<(), ActorProcessingErr> {
        before_each().await;
        let options = DynamicSupervisorOptions {
            max_children: None,
            max_restarts: 10,
            max_window: Duration::from_secs(10),
            reset_after: None,
        };
        let (sup_ref, sup_handle) =
            DynamicSupervisor::spawn("test_terminate_child".into(), options).await?;
        let child_spec = make_child_spec(
            "kill-me",
            Restart::Permanent,
            ChildBehavior::DelayedNormal { ms: 9999 },
        );
        DynamicSupervisor::spawn_child(sup_ref.clone(), child_spec).await?;
        // Confirm child is present
        let st_before = inspect_supervisor(&sup_ref).await;
        assert!(st_before.active_children.contains_key("kill-me"));
        assert!(
            get_running_children(&sup_ref).len() == 1,
            "Child is running"
        );
        // Terminate
        DynamicSupervisor::terminate_child(sup_ref.clone(), "kill-me".to_string()).await?;
        // Check that child is gone, no meltdown
        let st_after = inspect_supervisor(&sup_ref).await;
        assert!(!st_after.active_children.contains_key("kill-me"));
        assert!(
            get_running_children(&sup_ref).is_empty(),
            "No child is running"
        );
        assert_eq!(sup_ref.get_status(), ActorStatus::Running);
        // Stop the supervisor
        sup_ref.stop(None);
        let _ = sup_handle.await;
        let final_st = read_final_supervisor_state("test_terminate_child").await;
        // No restarts
        assert!(final_st.restart_log.is_empty());
        // Only started once
        assert_eq!(read_actor_call_count("kill-me").await, 1);
        Ok(())
    }
}
