//! # ractor-supervisor
//!
//! An **OTP-style supervisor** for the [`ractor`](https://docs.rs/ractor) framework—helping you build **supervision trees** in a straightforward, Rust-centric way.
//!
//! Inspired by the Elixir/Erlang supervision concept, `ractor-supervisor` provides a robust mechanism for overseeing **one or more child actors** and automatically restarting them under configurable policies. If too many restarts happen in a brief time window—a "meltdown"—the supervisor itself shuts down abnormally, preventing errant restart loops.
//!
//! ## Supervisor Types
//!
//! This crate provides three types of supervisors, each designed for specific use cases:
//!
//! ### 1. Static Supervisor (`Supervisor`)
//! - Manages a fixed set of children defined at startup
//! - Supports all supervision strategies (OneForOne, OneForAll, RestForOne)
//! - Best for static actor hierarchies where child actors are known at startup
//! - Example: A web server with predefined worker pools, cache managers, and connection handlers
//!
//! ### 2. Dynamic Supervisor (`DynamicSupervisor`)
//! - Allows adding/removing children at runtime
//! - Uses OneForOne strategy only (each child managed independently)
//! - Optional `max_children` limit
//! - Best for dynamic workloads where children are spawned/terminated on demand
//! - Example: A job queue processor that spawns worker actors based on load
//!
//! ### 3. Task Supervisor (`TaskSupervisor`)
//! - Specialized version of DynamicSupervisor for managing async tasks
//! - Wraps futures in actor tasks that can be supervised
//! - Simpler API focused on task execution rather than actor management
//! - Best for background jobs, periodic tasks, or any async work needing supervision
//! - Example: Scheduled jobs, background data processing, or cleanup tasks
//!
//! ## Supervision Strategies
//!
//! The strategy defines what happens when a child fails:
//!
//! - **OneForOne**: Only the failing child is restarted.
//! - **OneForAll**: If any child fails, all children are stopped and restarted.
//! - **RestForOne**: The failing child and all subsequently started children (in definition order) are stopped and restarted.
//!
//! Strategies apply to **all failure scenarios**, including:
//! - Spawn errors (failures in `pre_start`/`post_start`)
//! - Runtime panics
//! - Normal and abnormal exits
//!
//! Example: If spawning a child fails during `pre_start`, it will count as a restart and trigger strategy logic.
//!
//! ## Common Features
//!
//! All supervisor types share these core features:
//!
//! ### Restart Policies
//! - **Permanent**: Always restart, no matter how the child exited.
//! - **Transient**: Restart only if the child exited abnormally (panic or error).
//! - **Temporary**: Never restart, regardless of exit reason.
//!
//! ### Meltdown Logic
//! - **`max_restarts`** and **`max_window`**: The "time window" for meltdown counting, expressed as a [`Duration`]. If more than `max_restarts` occur within `max_window`, the supervisor shuts down abnormally (meltdown).
//! - **`reset_after`**: If the supervisor sees no failures for the specified duration, it clears its meltdown log and effectively "resets" the meltdown counters.
//!
//! ### Child-Level Features
//! - **`reset_after`** (per child): If a specific child remains up for the given duration, its own failure count is reset to zero on the next failure.
//! - **`backoff_fn`**: An optional function to delay a child's restart. For instance, you might implement exponential backoff to prevent immediate thrashing restarts.
//!
//! ## Choosing the Right Supervisor
//!
//! 1. Use `Supervisor` when:
//!    - Your actor hierarchy is known at startup
//!    - You need OneForAll or RestForOne strategies
//!    - Children are long-lived and relatively static
//!
//! 2. Use `DynamicSupervisor` when:
//!    - Children need to be added/removed at runtime
//!    - Each child is independent (OneForOne is sufficient)
//!    - You need to limit the total number of children
//!
//! 3. Use `TaskSupervisor` when:
//!    - You're working with futures/async tasks rather than full actors
//!    - Tasks are short-lived or periodic
//!    - You want a simpler API focused on task execution
//!
//! ## Important Requirements
//!
//! 1. **Actor Names**: Both supervisors and their child actors **must** have names set. These names are used for:
//!    - Unique identification in the supervision tree
//!    - Meltdown tracking and logging
//!    - Global actor registry
//!
//! 2. **Proper Spawning**: When spawning supervisors or child actors, always use:
//!    - [`Supervisor::spawn_linked`] or [`Supervisor::spawn`] for static supervisors
//!    - [`DynamicSupervisor::spawn_linked`] or [`DynamicSupervisor::spawn`] for dynamic supervisors
//!    - Do NOT use the generic [`Actor::spawn_linked`] directly
//!
//! ## Multi-Level Supervision Trees
//!
//! Supervisors can manage other **supervisors** as children, forming a **hierarchical** or **tree** structure. This way, different subsystems can each have their own meltdown thresholds or strategies. A meltdown in one subtree doesn't necessarily mean the entire application must go down, unless the top-level supervisor is triggered.
//!
//! For example:
//! ```text
//! Root Supervisor (Static, OneForOne)
//! ├── API Supervisor (Static, OneForAll)
//! │   ├── HTTP Server
//! │   └── WebSocket Server
//! ├── Worker Supervisor (Dynamic)
//! │   └── [Dynamic Worker Pool]
//! └── Task Supervisor
//!     └── [Background Jobs]
//! ```
//!
//! ## Example Usage
//!
//! Here's a complete example using a static supervisor:
//!
//! ```rust
//! use ractor::Actor;
//! use ractor_supervisor::*;
//! use ractor::concurrency::Duration;
//! use tokio::time::Instant;
//! use futures_util::FutureExt;
//!
//! // A minimal child actor that simply does some work in `handle`.
//! struct MyWorker;
//!
//! #[cfg_attr(feature = "async-trait", ractor::async_trait)]
//! impl Actor for MyWorker {
//!     type Msg = ();
//!     type State = ();
//!     type Arguments = ();
//!
//!     // Called before the actor fully starts. We can set up the actor's internal state here.
//!     async fn pre_start(
//!         &self,
//!         _myself: ractor::ActorRef<Self::Msg>,
//!         _args: Self::Arguments,
//!     ) -> Result<Self::State, ractor::ActorProcessingErr> {
//!         Ok(())
//!     }
//!
//!     // The main message handler. This is where you implement your actor's behavior.
//!     async fn handle(
//!         &self,
//!         _myself: ractor::ActorRef<Self::Msg>,
//!         _msg: Self::Msg,
//!         _state: &mut Self::State
//!     ) -> Result<(), ractor::ActorProcessingErr> {
//!         // do some work...
//!         Ok(())
//!     }
//! }
//!
//! // A function to spawn the child actor. This will be used in ChildSpec::spawn_fn.
//! async fn spawn_my_worker(
//!     supervisor_cell: ractor::ActorCell,
//!     child_id: String
//! ) -> Result<ractor::ActorCell, ractor::SpawnErr> {
//!     // We name the child actor using `child_spec.id` (though naming is optional).
//!     let (child_ref, _join) = Supervisor::spawn_linked(
//!         child_id,                    // actor name
//!         MyWorker,                    // actor instance
//!         (),                          // arguments
//!         supervisor_cell             // link to the supervisor
//!     ).await?;
//!     Ok(child_ref.get_cell())
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // A child-level backoff function that implements exponential backoff after the second failure.
//!     // Return Some(delay) to make the supervisor wait before restarting this child.
//!     let my_backoff: ChildBackoffFn = ChildBackoffFn::new(
//!         |_child_id: &str, restart_count: usize, last_fail: Instant, child_reset_after: Option<Duration>| {
//!             // On the first failure, restart immediately (None).
//!             // After the second failure, double the delay each time (exponential).
//!             if restart_count <= 1 {
//!                 None
//!             } else {
//!                 Some(Duration::from_secs(1 << restart_count))
//!             }
//!         }
//!     );
//!
//!     // This specification describes exactly how to manage our single child actor.
//!     let child_spec = ChildSpec {
//!         id: "myworker".into(),  // Unique identifier for meltdown logs and debugging.
//!         restart: Restart::Transient, // Only restart if the child fails abnormally.
//!         spawn_fn: SpawnFn::new(|cell, id| spawn_my_worker(cell, id)),
//!         backoff_fn: Some(my_backoff), // Apply our custom exponential backoff on restarts.
//!         // If the child remains up for 60s, its individual failure counter resets to 0 next time it fails.
//!         reset_after: Some(Duration::from_secs(60)),
//!     };
//!
//!     // Supervisor-level meltdown configuration. If more than 5 restarts occur within a 10s window, meltdown is triggered.
//!     // Also, if we stay quiet for 30s (no restarts), the meltdown log resets.
//!     let options = SupervisorOptions {
//!         strategy: SupervisorStrategy::OneForOne,  // If one child fails, only that child is restarted.
//!         max_restarts: 5,               // Permit up to 5 restarts in the meltdown window.
//!         max_window: Duration::from_secs(10),  // The meltdown window.
//!         reset_after: Some(Duration::from_secs(30)), // If no failures for 30s, meltdown log is cleared.
//!     };
//!
//!     // Group all child specs and meltdown options together:
//!     let args = SupervisorArguments {
//!         child_specs: vec![child_spec], // We only have one child in this example
//!         options,
//!     };
//!
//!     // Spawn the supervisor with our arguments.
//!     let (sup_ref, sup_handle) = Supervisor::spawn(
//!         "root".into(), // name for the supervisor
//!         args
//!     ).await?;
//!
//!     let _ = sup_ref.kill();
//!     let _ = sup_handle.await;
//!
//!     Ok(())
//! }
//! ```
//!
//! For more examples, see:
//! - [`Supervisor`] for static supervision
//! - [`DynamicSupervisor`] for dynamic child management
//! - [`TaskSupervisor`] for supervised async tasks
//!

const GRACEFUL_STOP_TIME: Duration = Duration::from_millis(500);

macro_rules! actor_log {
    (= $target:literal) => {
        const LOG_TARGET: &str = $target;
    };
    ($actor:expr, $which:ident, $($args:expr),*) => {
        log::$which!(
            target: &format!(
                "{} ({LOG_TARGET})",
                $actor
                    .get_name()
                    .unwrap_or_else(|| $actor.get_id().to_string())
            ),
            $($args),*
        );
    };
}

pub mod core;
pub mod dynamic;
pub mod supervisor;
pub mod task;

pub use core::*;
pub use dynamic::*;
use std::time::Duration;
pub use supervisor::*;
pub use task::*;
