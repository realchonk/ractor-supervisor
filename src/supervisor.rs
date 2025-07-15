use crate::core::{
    ChildFailureState, ChildSpec, CoreSupervisorOptions, RestartLog, SupervisorCore,
    SupervisorError,
};
use crate::{ExitReason, GRACEFUL_STOP_TIME};
use futures_util::{stream::iter, StreamExt};
use ractor::concurrency::{sleep, Duration, JoinHandle};
use ractor::{
    Actor, ActorCell, ActorName, ActorProcessingErr, ActorRef, RpcReplyPort, SpawnErr,
    SupervisionEvent,
};
use std::collections::HashMap;

actor_log!(= "Supervisor");

/// The supervision strategy for this supervisor’s children.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorStrategy {
    /// Only the failing child is restarted.
    OneForOne,
    /// If *any* child fails, all children are stopped and restarted.
    OneForAll,
    /// If one child fails, that child and all subsequently started children are stopped and restarted.
    RestForOne,
}

/// Supervisor-level meltdown policy.
///
/// - If more than `max_restarts` occur within `max_window`, meltdown occurs (supervisor stops abnormally).
/// - If `reset_after` is set, we clear the meltdown log if no restarts occur in that span.
///
/// # Timing
/// - `max_window`: Meltdown tracking window duration  
/// - `reset_after`: Supervisor-level reset duration
#[derive(Clone)]
pub struct SupervisorOptions {
    /// One of OneForOne, OneForAll, or RestForOne
    pub strategy: SupervisorStrategy,
    /// The meltdown threshold for restarts.
    pub max_restarts: usize,
    /// The meltdown time window.
    pub max_window: Duration,
    /// Optional: if no restarts occur for this duration, we clear the meltdown log.
    pub reset_after: Option<Duration>,
}

impl CoreSupervisorOptions<SupervisorStrategy> for SupervisorOptions {
    fn max_restarts(&self) -> usize {
        self.max_restarts
    }

    fn max_window(&self) -> Duration {
        self.max_window
    }

    fn reset_after(&self) -> Option<Duration> {
        self.reset_after
    }

    fn strategy(&self) -> SupervisorStrategy {
        self.strategy
    }
}

/// Internal messages that instruct the supervisor to spawn a child, triggered by its meltdown logic.
pub enum SupervisorMsg {
    /// (OneForOne) Re-spawn just this child
    OneForOneSpawn { child_id: String },
    /// (OneForAll) Stop all children, re-spawn them
    OneForAllSpawn { child_id: String },
    /// (RestForOne) Stop this child and all subsequent children, re-spawn them
    RestForOneSpawn { child_id: String },
    /// Return the current state snapshot (for debugging/tests).
    InspectState(RpcReplyPort<SupervisorState>),
}

/// The arguments needed to spawn the supervisor.
pub struct SupervisorArguments {
    /// The list of children to supervise.
    pub child_specs: Vec<ChildSpec>,
    /// Supervisor meltdown config + strategy.
    pub options: SupervisorOptions,
}

/// Holds the supervisor’s live state: which children are running, how many times each child has failed, etc.
///
/// # Important
/// The `child_specs` vector maintains the **startup order** of children which is critical for
/// strategies like `RestForOne` that rely on child ordering.
#[derive(Clone)]
pub struct SupervisorState {
    /// The original child specs (each child’s config).
    pub child_specs: Vec<ChildSpec>,

    /// Tracks how many times each child has failed and the last time it failed.
    pub child_failure_state: HashMap<String, ChildFailureState>,

    /// Rolling log of all restarts in the meltdown window.
    pub restart_log: Vec<RestartLog>,

    /// Supervisor meltdown options.
    pub options: SupervisorOptions,
}

impl SupervisorCore for SupervisorState {
    type Message = SupervisorMsg;
    type Options = SupervisorOptions;
    type Strategy = SupervisorStrategy;

    fn child_failure_state(&mut self) -> &mut HashMap<String, ChildFailureState> {
        &mut self.child_failure_state
    }

    fn restart_log(&mut self) -> &mut Vec<RestartLog> {
        &mut self.restart_log
    }

    fn options(&self) -> &SupervisorOptions {
        &self.options
    }

    fn restart_msg(
        &self,
        child_spec: &ChildSpec,
        strategy: SupervisorStrategy,
        _myself: ActorRef<SupervisorMsg>,
    ) -> SupervisorMsg {
        let child_id = child_spec.id.clone();
        match strategy {
            SupervisorStrategy::OneForOne => SupervisorMsg::OneForOneSpawn { child_id },
            SupervisorStrategy::OneForAll => SupervisorMsg::OneForAllSpawn { child_id },
            SupervisorStrategy::RestForOne => SupervisorMsg::RestForOneSpawn { child_id },
        }
    }
}

impl SupervisorState {
    /// Create a new [`SupervisorState`], from user-supplied [`SupervisorArguments`].
    fn new(args: SupervisorArguments) -> Self {
        Self {
            child_specs: args.child_specs,
            child_failure_state: HashMap::new(),
            restart_log: Vec::new(),
            options: args.options,
        }
    }

    pub async fn spawn_child(
        &mut self,
        child_spec: &ChildSpec,
        myself: ActorRef<SupervisorMsg>,
    ) -> Result<(), ActorProcessingErr> {
        let result = child_spec
            .spawn_fn
            .call(myself.get_cell().clone(), child_spec.id.clone())
            .await
            .map_err(|e| SupervisorError::ChildSpawnError {
                child_id: child_spec.id.clone(),
                reason: e.to_string(),
            });

        // Important: Spawn failures (including pre_start errors)
        // trigger restart logic and meltdown checks
        if let Err(err) = result {
            self.handle_child_restart(
                child_spec,
                true,
                myself.clone(),
                &ExitReason::Error(err.into()),
            )?;
        }

        Ok(())
    }

    /// Spawn all children in the order they were defined in [`SupervisorArguments::child_specs`].
    pub async fn spawn_all_children(
        &mut self,
        myself: ActorRef<SupervisorMsg>,
    ) -> Result<(), ActorProcessingErr> {
        // Temporarily take ownership of child_specs to avoid holding an immutable borrow
        let child_specs = std::mem::take(&mut self.child_specs);
        for spec in &child_specs {
            self.spawn_child(spec, myself.clone()).await?;
        }
        // Restore the child_specs after spawning
        self.child_specs = child_specs;
        Ok(())
    }

    /// OneForOne: meltdown-check first, then spawn just the failing child.
    pub async fn perform_one_for_one_spawn(
        &mut self,
        child_id: &str,
        myself: ActorRef<SupervisorMsg>,
    ) -> Result<(), ActorProcessingErr> {
        self.track_global_restart(child_id)?;
        // Temporarily take ownership of child_specs to avoid holding an immutable borrow
        let child_specs = std::mem::take(&mut self.child_specs);
        if let Some(spec) = child_specs.iter().find(|s| s.id == child_id) {
            self.spawn_child(spec, myself.clone()).await?;
        }
        // Restore the child_specs after spawning
        self.child_specs = child_specs;
        Ok(())
    }

    /// OneForAll: meltdown-check first, then stop all children, re-spawn them all.
    pub async fn perform_one_for_all_spawn(
        &mut self,
        child_id: &str,
        myself: ActorRef<SupervisorMsg>,
    ) -> Result<(), ActorProcessingErr> {
        actor_log!(myself, trace, "stopping all children...");
        self.track_global_restart(child_id)?;
        // Stop or kill all children.
        iter(&myself.get_children())
            .for_each_concurrent(None, |cell| {
                let myself = myself.clone();
                async move {
                    actor_log!(myself, debug, "stopping child {cell:?}");

                    // Must unlink to prevent confusion with them receiving further messages.
                    cell.unlink(myself.get_cell());

                    // Allow the children to gracefully exit, murder them if they don't comply.
                    if cell
                        .stop_and_wait(None, Some(GRACEFUL_STOP_TIME))
                        .await
                        .is_err()
                    {
                        actor_log!(myself, warn, "failed to stop child {cell:?}, killing...");
                        cell.kill();
                    }
                }
            })
            .await;

        // A short delay to allow the old children to fully unregister (avoid name collisions).
        sleep(Duration::from_millis(10)).await;
        self.spawn_all_children(myself.clone()).await?;
        actor_log!(myself, trace, "stopped all children.");
        Ok(())
    }

    /// RestForOne: meltdown-check first, then stop the failing child and all subsequent children, re-spawn them.
    pub async fn perform_rest_for_one_spawn(
        &mut self,
        child_id: &str,
        myself: ActorRef<SupervisorMsg>,
    ) -> Result<(), ActorProcessingErr> {
        self.track_global_restart(child_id)?;
        // Temporarily take ownership of child_specs to avoid holding an immutable borrow
        let child_specs = std::mem::take(&mut self.child_specs);
        let children = myself.get_children();
        let child_cell_by_name: HashMap<String, &ActorCell> = children
            .iter()
            .filter_map(|cell| cell.get_name().map(|name| (name, cell)))
            .collect();
        if let Some(i) = child_specs.iter().position(|s| s.id == child_id) {
            // Kill children from i..end
            for spec in child_specs.iter().skip(i) {
                if let Some(cell) = child_cell_by_name.get(&spec.id) {
                    cell.unlink(myself.get_cell());
                    cell.kill();
                }
            }
            // Short delay so old names get unregistered
            sleep(Duration::from_millis(10)).await;
            // Re-spawn children from i..end
            for spec in child_specs.iter().skip(i) {
                self.spawn_child(spec, myself.clone()).await?;
            }
        }
        // Restore the child_specs after spawning
        self.child_specs = child_specs;
        Ok(())
    }
}

/// The supervisor actor itself.  
/// Spawns its children in `post_start`, listens for child failures, and restarts them if needed.  
/// If meltdown occurs, it returns an error to end abnormally (thus skipping `post_stop`).
pub struct Supervisor;

impl Supervisor {
    pub async fn spawn_linked<T: Actor>(
        name: ActorName,
        handler: T,
        startup_args: T::Arguments,
        supervisor: ActorCell,
    ) -> Result<(ActorRef<T::Msg>, JoinHandle<()>), SpawnErr> {
        Actor::spawn_linked(Some(name), handler, startup_args, supervisor).await
    }

    pub async fn spawn(
        name: ActorName,
        startup_args: SupervisorArguments,
    ) -> Result<(ActorRef<SupervisorMsg>, JoinHandle<()>), SpawnErr> {
        Actor::spawn(Some(name), Supervisor, startup_args).await
    }
}

/// A global map for test usage, storing final states after each handle call and in `post_stop`.
#[cfg(test)]
static SUPERVISOR_FINAL: std::sync::OnceLock<tokio::sync::Mutex<HashMap<String, SupervisorState>>> =
    std::sync::OnceLock::new();

#[cfg_attr(feature = "async-trait", ractor::async_trait)]
impl Actor for Supervisor {
    type Msg = SupervisorMsg;
    type State = SupervisorState;
    type Arguments = SupervisorArguments;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        actor_log!(myself, trace, "starting...");
        Ok(SupervisorState::new(args))
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut SupervisorState,
    ) -> Result<(), ActorProcessingErr> {
        actor_log!(myself, trace, "started.");
        // Spawn all children initially
        state.spawn_all_children(myself).await?;
        Ok(())
    }

    /// The main message handler: we respond to “spawn child X” or “inspect state”.
    /// Each time we finish, we store final state in a global map (test usage only).
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        msg: SupervisorMsg,
        state: &mut SupervisorState,
    ) -> Result<(), ActorProcessingErr> {
        let result = match msg {
            SupervisorMsg::OneForOneSpawn { child_id } => {
                actor_log!(
                    myself,
                    trace,
                    "received message: OneForOneSpawn({child_id:?})"
                );
                state
                    .perform_one_for_one_spawn(&child_id, myself.clone())
                    .await
            }
            SupervisorMsg::OneForAllSpawn { child_id } => {
                actor_log!(
                    myself,
                    trace,
                    "received message: OneForAllSpawn({child_id:?})"
                );
                state
                    .perform_one_for_all_spawn(&child_id, myself.clone())
                    .await
            }
            SupervisorMsg::RestForOneSpawn { child_id } => {
                actor_log!(
                    myself,
                    trace,
                    "received message: RestForOneSpawn({child_id:?})"
                );
                state
                    .perform_rest_for_one_spawn(&child_id, myself.clone())
                    .await
            }
            SupervisorMsg::InspectState(rpc_reply_port) => {
                actor_log!(myself, trace, "received message: InspectState(...)");
                rpc_reply_port.send(state.clone())?;
                Ok(())
            }
        };

        #[cfg(test)]
        {
            store_final_state(myself, state).await;
        }

        // Return any meltdown or spawn error
        result
    }

    /// Respond to supervision events from child actors.
    /// - `ActorTerminated` => treat as normal exit
    /// - `ActorFailed` => treat as abnormal
    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        evt: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        actor_log!(myself, trace, "{evt:?}");
        match evt {
            SupervisionEvent::ActorStarted(cell) => {
                let child_id = cell
                    .get_name()
                    .ok_or(SupervisorError::ChildNameNotSet { pid: cell.get_id() })?;
                actor_log!(myself, info, "Started child: {}", child_id);
                if state.child_specs.iter().any(|s| s.id == child_id) {
                    // This is a child we know about, so we track it
                    state
                        .child_failure_state
                        .entry(child_id.clone())
                        .or_insert_with(|| ChildFailureState {
                            restart_count: 0,
                            last_fail_instant: ractor::concurrency::Instant::now(),
                        });
                }
            }
            SupervisionEvent::ActorTerminated(cell, _final_state, reason) => {
                // Normal exit => abnormal=false
                let child_id = cell
                    .get_name()
                    .ok_or(SupervisorError::ChildNameNotSet { pid: cell.get_id() })?;
                let child_specs = std::mem::take(&mut state.child_specs);
                if let Some(spec) = child_specs.iter().find(|s| s.id == child_id) {
                    state.handle_child_restart(
                        spec,
                        false,
                        myself.clone(),
                        &ExitReason::Reason(reason),
                    )?;
                }
                state.child_specs = child_specs;
            }
            SupervisionEvent::ActorFailed(cell, err) => {
                // Abnormal exit => abnormal=true
                let child_id = cell
                    .get_name()
                    .ok_or(SupervisorError::ChildNameNotSet { pid: cell.get_id() })?;
                let child_specs = std::mem::take(&mut state.child_specs);
                if let Some(spec) = child_specs.iter().find(|s| s.id == child_id) {
                    state.handle_child_restart(
                        spec,
                        true,
                        myself.clone(),
                        &ExitReason::Error(err),
                    )?;
                }
                state.child_specs = child_specs;
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
        actor_log!(myself, trace, "stopping.");
        #[cfg(test)]
        {
            store_final_state(myself.clone(), _state).await;
        }
        // Stop or kill all children.
        iter(&myself.get_children())
            .for_each_concurrent(None, |cell| {
                let myself = myself.clone();
                async move {
                    actor_log!(myself, debug, "stopping child {cell:?}");

                    // Must unlink to prevent confusion with them receiving further messages.
                    cell.unlink(myself.get_cell());

                    // Allow the children to gracefully exit, murder them if they don't comply.
                    if cell
                        .stop_and_wait(None, Some(GRACEFUL_STOP_TIME))
                        .await
                        .is_err()
                    {
                        actor_log!(myself, warn, "failed to stop child {cell:?}, killing...");
                        cell.kill();
                    }
                }
            })
            .await;
        actor_log!(myself, trace, "stopped.");
        Ok(())
    }
}

#[cfg(test)]
async fn store_final_state(myself: ActorRef<SupervisorMsg>, state: &SupervisorState) {
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
    use crate::core::{ChildBackoffFn, Restart};
    use crate::SpawnFn;
    use ractor::concurrency::Instant;
    use ractor::{call_t, Actor, ActorCell, ActorRef, ActorStatus};
    use serial_test::serial;
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
            .get_or_init(|| tokio::sync::Mutex::new(std::collections::HashMap::new()))
            .lock()
            .await;
        *map.entry(child_id.to_string()).or_default() += 1;
    }

    /// Utility to read the final state of a named supervisor after it stops.
    async fn read_final_supervisor_state(sup_name: &str) -> SupervisorState {
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
                ChildBehavior::ImmediateFail => {
                    panic!("Immediate fail => ActorFailed");
                }
                ChildBehavior::ImmediateNormal => {
                    myself.stop(None);
                }
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
                ChildBehavior::DelayedFail { .. } => {
                    panic!("Delayed fail => ActorFailed");
                }
                ChildBehavior::DelayedNormal { .. } => {
                    myself.stop(None);
                }
                ChildBehavior::ImmediateFail => {
                    panic!("ImmediateFail => ActorFailed");
                }
                ChildBehavior::ImmediateNormal => {
                    myself.stop(None);
                }
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

    fn get_running_children(sup_ref: &ActorRef<SupervisorMsg>) -> HashMap<String, ActorCell> {
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
        let (ch_ref, _join) = Supervisor::spawn_linked(id, TestChild, behavior, sup_cell).await?;
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

    #[ractor::concurrency::test]
    #[serial]
    async fn test_permanent_delayed_fail() -> Result<(), Box<dyn std::error::Error>> {
        before_each().await;

        // meltdown on the 2nd fail => max_restarts=1
        let child_spec = make_child_spec(
            "fail-delay",
            Restart::Permanent,
            ChildBehavior::DelayedFail { ms: 200 },
        );
        let options = SupervisorOptions {
            strategy: SupervisorStrategy::OneForOne,
            max_restarts: 1, // meltdown on second fail
            max_window: Duration::from_secs(2),
            reset_after: None,
        };
        let args = SupervisorArguments {
            child_specs: vec![child_spec],
            options,
        };

        let (sup_ref, sup_handle) =
            Supervisor::spawn("test_permanent_delayed_fail".into(), args).await?;

        sleep(Duration::from_millis(100)).await;
        let st = call_t!(sup_ref, SupervisorMsg::InspectState, 500).unwrap();
        let mut running = get_running_children(&sup_ref);
        assert_eq!(running.len(), 1);
        assert_eq!(st.restart_log.len(), 0);

        // meltdown on second fail => wait for supervisor to stop
        let _ = sup_handle.await;
        assert_eq!(sup_ref.get_status(), ActorStatus::Stopped);

        // final checks
        let final_st = read_final_supervisor_state("test_permanent_delayed_fail").await;
        running = get_running_children(&sup_ref);
        assert_eq!(running.len(), 0);
        assert!(final_st.restart_log.len() >= 2);

        // We had exactly 2 spawns: one initial, one after 1st fail => meltdown on 2nd fail
        assert_eq!(read_actor_call_count("fail-delay").await, 2);

        Ok(())
    }

    #[ractor::concurrency::test]
    #[serial]
    async fn test_transient_delayed_normal() -> Result<(), Box<dyn std::error::Error>> {
        before_each().await;

        // child does a delayed normal exit => no restarts
        let child_spec = make_child_spec(
            "normal-delay",
            Restart::Transient,
            ChildBehavior::DelayedNormal { ms: 300 },
        );
        let options = SupervisorOptions {
            strategy: SupervisorStrategy::OneForOne,
            max_restarts: 5,
            max_window: Duration::from_secs(5),
            reset_after: None,
        };
        let args = SupervisorArguments {
            child_specs: vec![child_spec],
            options,
        };

        let (sup_ref, sup_handle) =
            Supervisor::spawn("test_transient_delayed_normal".into(), args).await?;

        sleep(Duration::from_millis(150)).await;
        let st1 = call_t!(sup_ref, SupervisorMsg::InspectState, 500).unwrap();

        let running = get_running_children(&sup_ref);
        assert_eq!(running.len(), 1);
        assert_eq!(st1.restart_log.len(), 0);

        // child exits normally => no meltdown => we stop
        sleep(Duration::from_millis(300)).await;
        sup_ref.stop(None);
        let _ = sup_handle.await;

        let final_state = read_final_supervisor_state("test_transient_delayed_normal").await;
        let running = get_running_children(&sup_ref);
        assert!(!running.contains_key("normal-delay"));
        assert_eq!(final_state.restart_log.len(), 0);

        // Only ever spawned once
        assert_eq!(read_actor_call_count("normal-delay").await, 1);

        Ok(())
    }

    #[ractor::concurrency::test]
    #[serial]
    async fn test_temporary_delayed_fail() -> Result<(), Box<dyn std::error::Error>> {
        before_each().await;

        // Temporary => never restart even if fails
        let child_spec = make_child_spec(
            "temp-delay",
            Restart::Temporary,
            ChildBehavior::DelayedFail { ms: 200 },
        );
        let options = SupervisorOptions {
            strategy: SupervisorStrategy::OneForOne,
            max_restarts: 10,
            max_window: Duration::from_secs(10),
            reset_after: None,
        };
        let args = SupervisorArguments {
            child_specs: vec![child_spec],
            options,
        };

        let (sup_ref, sup_handle) =
            Supervisor::spawn("test_temporary_delayed_fail".into(), args).await?;

        sleep(Duration::from_millis(100)).await;
        let st1 = call_t!(sup_ref, SupervisorMsg::InspectState, 500).unwrap();
        let running = get_running_children(&sup_ref);
        assert_eq!(running.len(), 1);
        assert_eq!(st1.restart_log.len(), 0);

        // The child fails, but policy=Temporary => no restart
        sleep(Duration::from_millis(300)).await;
        assert_eq!(sup_ref.get_status(), ActorStatus::Running);

        sup_ref.stop(None);
        let _ = sup_handle.await;

        let final_state = read_final_supervisor_state("test_temporary_delayed_fail").await;
        let running = get_running_children(&sup_ref);
        assert_eq!(running.len(), 0);
        assert_eq!(final_state.restart_log.len(), 0);

        // Only ever spawned once
        assert_eq!(read_actor_call_count("temp-delay").await, 1);

        Ok(())
    }

    #[ractor::concurrency::test]
    #[serial]
    async fn test_one_for_all_stop_all_on_failure() -> Result<(), Box<dyn std::error::Error>> {
        before_each().await;

        // meltdown on the 3rd fail => set max_restarts=2 => meltdown at fail #3
        let child1 = make_child_spec(
            "ofa-fail",
            Restart::Permanent,
            ChildBehavior::DelayedFail { ms: 200 },
        );
        let child2 = make_child_spec(
            "ofa-normal",
            Restart::Permanent,
            ChildBehavior::DelayedNormal { ms: 9999 },
        );

        let options = SupervisorOptions {
            strategy: SupervisorStrategy::OneForAll,
            max_restarts: 2,
            max_window: Duration::from_secs(2),
            reset_after: None,
        };
        let args = SupervisorArguments {
            child_specs: vec![child1, child2],
            options,
        };
        let (sup_ref, sup_handle) =
            Supervisor::spawn("test_one_for_all_stop_all_on_failure".into(), args).await?;

        sleep(Duration::from_millis(100)).await;
        let running_children = get_running_children(&sup_ref);
        assert_eq!(running_children.len(), 2);

        let _ = sup_handle.await;
        assert_eq!(sup_ref.get_status(), ActorStatus::Stopped);

        let final_state = read_final_supervisor_state("test_one_for_all_stop_all_on_failure").await;
        assert_eq!(sup_ref.get_children().len(), 0);
        assert_eq!(final_state.restart_log.len(), 3);

        // Because each time "ofa-fail" fails, OneForAll restarts *all* children:
        // meltdown occurs on the 3rd fail => that means "ofa-fail" was spawned 3 times
        assert_eq!(read_actor_call_count("ofa-fail").await, 3);
        // "ofa-normal" also restarts each time, so also spawned 3 times
        assert_eq!(read_actor_call_count("ofa-normal").await, 3);

        Ok(())
    }

    #[ractor::concurrency::test]
    #[serial]
    async fn test_rest_for_one_restart_subset() -> Result<(), Box<dyn std::error::Error>> {
        before_each().await;

        // meltdown on 2nd fail => max_restarts=1 => meltdown at fail #2
        let child_a = make_child_spec(
            "A",
            Restart::Permanent,
            ChildBehavior::DelayedNormal { ms: 9999 },
        );
        let child_b = make_child_spec(
            "B",
            Restart::Permanent,
            ChildBehavior::DelayedFail { ms: 200 },
        );
        let child_c = make_child_spec(
            "C",
            Restart::Permanent,
            ChildBehavior::DelayedNormal { ms: 9999 },
        );

        let options = SupervisorOptions {
            strategy: SupervisorStrategy::RestForOne,
            max_restarts: 1,
            max_window: Duration::from_secs(2),
            reset_after: None,
        };
        let args = SupervisorArguments {
            child_specs: vec![child_a, child_b, child_c],
            options,
        };
        let (sup_ref, sup_handle) =
            Supervisor::spawn("test_rest_for_one_restart_subset".into(), args).await?;

        sleep(Duration::from_millis(100)).await;
        let running_children = get_running_children(&sup_ref);
        assert_eq!(running_children.len(), 3);

        let _ = sup_handle.await;
        assert_eq!(sup_ref.get_status(), ActorStatus::Stopped);

        let final_state = read_final_supervisor_state("test_rest_for_one_restart_subset").await;
        assert_eq!(sup_ref.get_children().len(), 0);
        assert_eq!(final_state.restart_log.len(), 2);
        assert_eq!(final_state.restart_log[0].child_id, "B");
        assert_eq!(final_state.restart_log[1].child_id, "B");

        // "B" fails => triggers a restart for B (and everything after B: child C)
        // meltdown on 2nd fail => total spawns for B = 2, C = 2, A stays up from the start => 1
        assert_eq!(read_actor_call_count("A").await, 1);
        assert_eq!(read_actor_call_count("B").await, 2);
        assert_eq!(read_actor_call_count("C").await, 2);

        Ok(())
    }

    #[ractor::concurrency::test]
    #[serial]
    async fn test_max_restarts_in_time_window() -> Result<(), Box<dyn std::error::Error>> {
        before_each().await;

        // meltdown on 3 fails in <1s => max_restarts=2 => meltdown on fail #3
        let child_spec =
            make_child_spec("fastfail", Restart::Permanent, ChildBehavior::ImmediateFail);

        let options = SupervisorOptions {
            strategy: SupervisorStrategy::OneForOne,
            max_restarts: 2,
            max_window: Duration::from_secs(1),
            reset_after: None,
        };
        let args = SupervisorArguments {
            child_specs: vec![child_spec],
            options,
        };

        let (sup_ref, sup_handle) =
            Supervisor::spawn("test_max_restarts_in_time_window".into(), args).await?;

        let _ = sup_handle.await;
        assert_eq!(sup_ref.get_status(), ActorStatus::Stopped);

        let final_state = read_final_supervisor_state("test_max_restarts_in_time_window").await;
        assert_eq!(
            final_state.restart_log.len(),
            3,
            "3 fails in <1s => meltdown"
        );

        // 3 fails => total spawns is 3
        assert_eq!(read_actor_call_count("fastfail").await, 3);

        Ok(())
    }

    #[ractor::concurrency::test]
    #[serial]
    async fn test_transient_abnormal_exit() -> Result<(), Box<dyn std::error::Error>> {
        before_each().await;

        // meltdown on 1st fail => max_restarts=0
        let child_spec = make_child_spec(
            "transient-bad",
            Restart::Transient,
            ChildBehavior::ImmediateFail,
        );

        let options = SupervisorOptions {
            strategy: SupervisorStrategy::OneForOne,
            max_restarts: 0, // meltdown on the very first fail
            max_window: Duration::from_secs(5),
            reset_after: None,
        };

        let args = SupervisorArguments {
            child_specs: vec![child_spec],
            options,
        };
        let (sup_ref, sup_handle) =
            Supervisor::spawn("test_transient_abnormal_exit".into(), args).await?;

        let _ = sup_handle.await;
        assert_eq!(sup_ref.get_status(), ActorStatus::Stopped);

        let final_state = read_final_supervisor_state("test_transient_abnormal_exit").await;
        assert_eq!(
            final_state.restart_log.len(),
            1,
            "1 fail => meltdown with max_restarts=0"
        );

        // Only ever spawned once; meltdown immediately
        assert_eq!(read_actor_call_count("transient-bad").await, 1);

        Ok(())
    }

    #[ractor::concurrency::test]
    #[serial]
    async fn test_backoff_fn_delays_restart() -> Result<(), Box<dyn std::error::Error>> {
        before_each().await;

        // meltdown on 2nd fail => set max_restarts=1 => meltdown on fail #2
        // but we do a 2s child-level backoff for the second spawn
        let child_backoff: ChildBackoffFn =
            ChildBackoffFn::new(|_id, count, _last, _child_reset| {
                if count <= 1 {
                    None
                } else {
                    Some(Duration::from_secs(2))
                }
            });

        let mut child_spec =
            make_child_spec("backoff", Restart::Permanent, ChildBehavior::ImmediateFail);
        child_spec.backoff_fn = Some(child_backoff);

        let options = SupervisorOptions {
            strategy: SupervisorStrategy::OneForOne,
            max_restarts: 1, // meltdown on 2nd fail
            max_window: Duration::from_secs(10),
            reset_after: None,
        };
        let args = SupervisorArguments {
            child_specs: vec![child_spec],
            options,
        };

        let before = Instant::now();
        let (sup_ref, sup_handle) =
            Supervisor::spawn("test_backoff_fn_delays_restart".into(), args).await?;
        let _ = sup_handle.await;

        let elapsed = before.elapsed();
        assert!(
            elapsed >= Duration::from_secs(2),
            "2s delay on second fail due to child-level backoff"
        );
        assert_eq!(sup_ref.get_status(), ActorStatus::Stopped);

        let final_st = read_final_supervisor_state("test_backoff_fn_delays_restart").await;
        assert_eq!(
            final_st.restart_log.len(),
            2,
            "first fail => immediate restart => second fail => meltdown"
        );

        // Exactly 2 spawns
        assert_eq!(read_actor_call_count("backoff").await, 2);

        Ok(())
    }

    #[ractor::concurrency::test]
    #[serial]
    async fn test_restart_counter_reset_after() -> Result<(), Box<dyn std::error::Error>> {
        before_each().await;

        // Child fails 2 times quickly => meltdown log=2
        // Wait 3s => meltdown log is cleared => final fail => meltdown log=1 => no meltdown
        // => total 3 fails => i.e. the child is started 4 times
        let behavior = ChildBehavior::FailWaitFail {
            initial_fails: 2,
            wait_ms: 3000,
            final_fails: 1,
            current: Arc::new(AtomicU64::new(0)),
        };

        let child_spec = ChildSpec {
            id: "reset-test".to_string(),
            restart: Restart::Permanent,
            spawn_fn: SpawnFn::new(move |sup_cell, id| {
                spawn_test_child(sup_cell, id, behavior.clone())
            }),
            backoff_fn: None,
            reset_after: None, // no child-level reset
        };

        // meltdown if 3 fails happen within max_window=10s => max_restarts=2 => meltdown on #3
        // but if we wait 3s => meltdown log is cleared => final fail => meltdown log=1 => no meltdown
        let options = SupervisorOptions {
            strategy: SupervisorStrategy::OneForOne,
            max_restarts: 2,
            max_window: Duration::from_secs(10),
            reset_after: Some(Duration::from_secs(2)), // if quiet >=2s => meltdown log cleared
        };

        let args = SupervisorArguments {
            child_specs: vec![child_spec],
            options,
        };
        let (sup_ref, sup_handle) =
            Supervisor::spawn("test_restart_counter_reset_after_improved".into(), args).await?;

        // Wait for 2 quick fails => meltdown log=2 => then child is quiet 3s => meltdown log cleared => final fail => meltdown log=1 => no meltdown
        sleep(Duration::from_secs(4)).await;

        // forcibly stop => no meltdown
        sup_ref.stop(None);
        let _ = sup_handle.await;

        let final_st =
            read_final_supervisor_state("test_restart_counter_reset_after_improved").await;
        assert_eq!(sup_ref.get_status(), ActorStatus::Stopped);
        assert_eq!(
            final_st.restart_log.len(),
            1,
            "After clearing, we only see a single fail in meltdown log"
        );

        // The child was actually spawned 4 times:
        //  - Start #1 => fail #1 => restart => #2 => fail #2 => restart => #3
        //  - Then quiet 3s => meltdown log cleared => fail #3 => meltdown log=1 => restart => #4
        assert_eq!(read_actor_call_count("reset-test").await, 4);

        Ok(())
    }

    #[ractor::concurrency::test]
    #[serial]
    async fn test_child_level_restart_counter_reset_after() -> Result<(), Box<dyn std::error::Error>>
    {
        before_each().await;

        // The child fails 2 times => restarts => after 3s quiet, we reset
        // => final fail is treated like "fail #1" from the child's perspective
        // => no meltdown triggered
        // => total 3 fails => so the child is spawned 4 times
        let behavior = ChildBehavior::FailWaitFail {
            initial_fails: 2,
            wait_ms: 3000,
            final_fails: 1,
            current: Arc::new(AtomicU64::new(0)),
        };

        let mut child_spec = make_child_spec("child-reset", Restart::Permanent, behavior);
        // This time we do a child-level reset_after of 2s
        child_spec.reset_after = Some(Duration::from_secs(2));

        // meltdown won't happen quickly because max_restarts=5
        let options = SupervisorOptions {
            strategy: SupervisorStrategy::OneForOne,
            max_restarts: 5,
            max_window: Duration::from_secs(30),
            reset_after: None,
        };
        let args = SupervisorArguments {
            child_specs: vec![child_spec],
            options,
        };

        let (sup_ref, sup_handle) =
            Supervisor::spawn("test_child_level_restart_counter_reset_after".into(), args).await?;

        // first 2 fails happen quickly => child is started 3 times so far
        sleep(Duration::from_millis(100)).await;
        let st1 = call_t!(sup_ref, SupervisorMsg::InspectState, 500).unwrap();
        let cfs1 = st1.child_failure_state.get("child-reset").unwrap();
        assert_eq!(cfs1.restart_count, 2);

        // After 3s quiet => child's restart_count is reset
        sleep(Duration::from_secs(3)).await;

        // final fail => from the child's perspective it's now fail #1 => no meltdown
        sup_ref.stop(None);
        let _ = sup_handle.await;

        let final_st =
            read_final_supervisor_state("test_child_level_restart_counter_reset_after").await;
        let cfs2 = final_st.child_failure_state.get("child-reset").unwrap();
        assert_eq!(
            cfs2.restart_count, 1,
            "child-level reset => next fail sees count=1"
        );

        // total spawns = 4
        assert_eq!(read_actor_call_count("child-reset").await, 4);

        Ok(())
    }

    //
    // Demo: nested supervisors
    //
    #[ractor::concurrency::test]
    #[serial]
    async fn test_nested_supervisors() -> Result<(), Box<dyn std::error::Error>> {
        before_each().await;

        async fn spawn_subsupervisor(
            sup_cell: ActorCell,
            id: String,
            args: SupervisorArguments,
        ) -> Result<ActorCell, SpawnErr> {
            let (sub_sup_ref, _join) =
                Supervisor::spawn_linked(id, Supervisor, args, sup_cell).await?;
            Ok(sub_sup_ref.get_cell())
        }

        // The "sub-sup" is itself a supervisor that spawns "leaf-worker"
        let sub_sup_spec = ChildSpec {
            id: "sub-sup".to_string(),
            restart: Restart::Permanent,
            spawn_fn: SpawnFn::new(move |cell, id| {
                let leaf_child = ChildSpec {
                    id: "leaf-worker".to_string(),
                    restart: Restart::Transient,
                    spawn_fn: SpawnFn::new(|c, i| {
                        // a child that fails once after 300ms
                        let bh = ChildBehavior::DelayedFail { ms: 300 };
                        spawn_test_child(c, i, bh)
                    }),
                    backoff_fn: None,
                    reset_after: None,
                };

                let sub_sup_args = SupervisorArguments {
                    child_specs: vec![leaf_child],
                    options: SupervisorOptions {
                        strategy: SupervisorStrategy::OneForOne,
                        max_restarts: 1, // meltdown on 2nd fail
                        max_window: Duration::from_secs(2),
                        reset_after: None,
                    },
                };
                spawn_subsupervisor(cell, id, sub_sup_args)
            }),
            backoff_fn: None,
            reset_after: None,
        };

        // root supervisor that manages sub-sup
        let root_args = SupervisorArguments {
            child_specs: vec![sub_sup_spec],
            options: SupervisorOptions {
                strategy: SupervisorStrategy::OneForOne,
                max_restarts: 1, // meltdown if "sub-sup" fails 2 times
                max_window: Duration::from_secs(5),
                reset_after: None,
            },
        };

        let (root_sup_ref, root_handle) = Supervisor::spawn("root-sup".into(), root_args).await?;

        // Wait for "leaf-worker" to fail once
        sleep(Duration::from_millis(600)).await;
        assert_eq!(root_sup_ref.get_status(), ActorStatus::Running);

        // Stop the root
        root_sup_ref.stop(None);
        let _ = root_handle.await;

        let root_final = read_final_supervisor_state("root-sup").await;
        let sub_final = read_final_supervisor_state("sub-sup").await;

        assert_eq!(root_final.restart_log.len(), 0);
        assert_eq!(sub_final.restart_log.len(), 1);

        assert_eq!(read_actor_call_count("leaf-worker").await, 2);

        Ok(())
    }
}
