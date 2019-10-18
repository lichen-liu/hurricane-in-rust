use super::cloning_strategy::*;
use super::hurricane_frontend;
use super::hurricane_work::{WorkExecutor, WorkExecutorDone};
use super::hurricane_work_io::*;
use super::task_manager::*;
use crate::app::blueprint::*;
use crate::app::hurricane_application::*;
use crate::common::bag::*;
use crate::common::messages::*;
use crate::common::primitive_serializer::*;
use crate::common::props::*;
use crate::common::types::*;
use actix::prelude::*;
use chrono::prelude::*;
use log::*;
use rand::Rng;
use std::marker::PhantomData;
use std::process;
use std::time::{Duration, Instant};
use sysinfo::{NetworkExt, ProcessExt, ProcessorExt, SystemExt};

struct SystemMonitor {
    pid: sysinfo::Pid,
    system: sysinfo::System,
    num_processors: usize,
}

impl SystemMonitor {
    fn new() -> SystemMonitor {
        // Initialize
        let pid = process::id() as sysinfo::Pid;
        let mut system = sysinfo::System::new();

        // Refresh All
        let r = system.refresh_process(pid);
        assert!(r);
        system.refresh_system();
        system.refresh_network();

        let num_processors = system.get_processor_list().len() - 1;

        SystemMonitor {
            pid: pid,
            system: system,
            num_processors,
        }
    }

    /// Refresh and get the system load info.
    ///
    /// (CPU Load, Network Downstream, Network Upstream)
    ///
    /// CPU Load - Average CPU Load across the entire system, in %.
    /// Network Downstream - in Bytes.
    /// Network Upstream - in Bytes.
    fn get_load(&mut self) -> (f32, u64, u64) {
        let system = &mut self.system;
        system.refresh_system();
        system.refresh_network();

        (
            system.get_processor_list()[0].get_cpu_usage() * 100.0,
            system.get_network().get_income(),
            system.get_network().get_outcome(),
        )
    }

    fn num_processors(&self) -> usize {
        self.num_processors
    }

    #[allow(dead_code)]
    fn get_info(&mut self) -> String {
        let pid = self.pid;
        let system = &mut self.system;

        let r = system.refresh_process(pid);
        assert!(r);
        system.refresh_system();
        system.refresh_network();

        let process = system.get_process(pid).unwrap();
        format!(
            "Avg CPU Load ({} CPUs): {:02.4}. {}: {}: {:02.4}, {}kB. Down: {}B. Up: {}B.",
            system.get_processor_list().len() - 1,
            system.get_processor_list()[0].get_cpu_usage() * 100.0,
            process.pid(),
            process.name(),
            process.cpu_usage(),
            process.memory(),
            system.get_network().get_income(),
            system.get_network().get_outcome()
        )
    }
}

/// Core logic Actor for TaskManager.
pub struct TaskManagerCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    id: usize,
    task_manager: Addr<TaskManager<HA>>,
    /// The real `HurricaneApplication`.
    hurricane_app: HA,
    hurricane_work_io: Option<Addr<HurricaneWorkIO>>,
    worker: Option<Addr<WorkExecutor>>,
    /// The `SpawnHandle` for the timed-interval tick.
    tick_handle: Option<SpawnHandle>,
    start_time: Option<Instant>,
    current_task: Task,
    current_task_internal_merge_props: Option<(Blueprint, Props)>,
    system: Option<SystemMonitor>,
    last_cpu_load: f32,
    last_overload: Option<Instant>,
    suppress_progress_request: bool,
}

impl<HA> TaskManagerCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    fn get_name(&self) -> String {
        format!(
            GET_NAME_FORMAT!(),
            format!("[{}, Core]", self.id),
            "TaskManager"
        )
    }

    pub fn new(
        id: usize,
        task_manager: Addr<TaskManager<HA>>,
        hurricane_app: HA,
    ) -> TaskManagerCore<HA> {
        TaskManagerCore {
            id,
            task_manager,
            hurricane_app,
            hurricane_work_io: None,
            worker: None,
            tick_handle: None,
            start_time: None,
            current_task: Task(None),
            current_task_internal_merge_props: None,
            system: None,
            last_cpu_load: 0.0,
            last_overload: None,
            suppress_progress_request: false,
        }
    }

    fn validate_task_blueprint(&self, task_id: &TaskId, blueprint: &mut Blueprint) {
        let num_processors = self.system.as_ref().unwrap().num_processors();

        // Check whether use_conf_parallelism is on. This has to be
        // done first, since the config itself might be problemastic.
        if hurricane_frontend::conf::get().use_conf_parallelism
            && blueprint.merge_type != MergeType::Nonclonable
        {
            // If cannot find the user-specified value, set it to
            // num_processors on the node.
            let parallelism = hurricane_frontend::conf::get()
                .workers_parallelism
                .get(self.id)
                .cloned()
                .unwrap_or(num_processors);

            blueprint.num_threads = parallelism;
            info!(
                "{}Parallelism set to {} for task <{}>.",
                self.get_name(),
                parallelism,
                task_id,
            );
        }

        // Check parallelism from blueprint.
        if blueprint.num_threads < 1 {
            warn!(
                "{}Parallelism set for task ({}) <{}>:",
                self.get_name(),
                blueprint.name(),
                task_id,
            );
            warn!(
                "{}    [{}] is invalid.",
                self.get_name(),
                blueprint.num_threads,
            );
            blueprint.num_threads = 1;
        }

        // Warning if parallelism is set too high.
        if blueprint.num_threads > num_processors {
            warn!(
                "{}Parallelism set for task ({}) <{}>:",
                self.get_name(),
                blueprint.name(),
                task_id,
            );
            warn!(
                "{}    [{}] is too high comparing to number of processors [{}].",
                self.get_name(),
                blueprint.num_threads,
                num_processors
            );
            warn!(
                "{}    Possible negative performance impact!",
                self.get_name()
            );
        }

        // It is also possible that `use_conf_parallelism` causes the violation of Nonclonable constraint.
        if blueprint.merge_type == MergeType::Nonclonable && blueprint.num_threads != 1 {
            warn!(
                "{}Nonclonable Task for blueprint({}) cannot have parallelism({}) other than 1!",
                self.get_name(),
                blueprint.name(),
                blueprint.num_threads
            );
            blueprint.num_threads = 1;
        }
    }

    fn validate_merge_blueprint(&self, merge_blueprint: &mut Blueprint) {
        if merge_blueprint.num_threads > 1 {
            warn!(
                "{}Multithreading is not enabled for Merge in blueprint({})!",
                self.get_name(),
                merge_blueprint.name()
            );
            merge_blueprint.num_threads = 1;
        }

        if merge_blueprint.merge_type != MergeType::Nonclonable {
            warn!(
                "{}Merge Task must be marked as Nonclonable in blueprint({})!",
                self.get_name(),
                merge_blueprint.name()
            );
            merge_blueprint.merge_type = MergeType::Nonclonable;
        }
    }
}

impl<HA> Actor for TaskManagerCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        info!("{}Starting!", self.get_name());

        // Load monitor
        self.system = Some(SystemMonitor::new());

        // Start HurricaneWorkIO
        self.hurricane_work_io = Some(HurricaneWorkIO::spawn(self.id, ctx.address().recipient()));
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("{}Bye~", self.get_name());
    }
}

impl<HA> Handler<LocalMessages> for TaskManagerCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: LocalMessages, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            LocalMessages::FetchTask => {
                if self.worker.is_none() {
                    trace!(
                        "{}Fetching task. [{}]",
                        self.get_name(),
                        Local::now().format("%F %a %H:%M:%S%.3f").to_string()
                    );
                    self.task_manager
                        .try_send(TaskManagerTaskMessage::GetTask)
                        .unwrap();
                } else {
                    warn!("A master worker is already running, not fetching a new task just yet.");
                }
            }
            LocalMessages::MoveOn => {
                self.worker = None;
                self.current_task = Task(None);
                self.last_cpu_load = 0.0;
                self.last_overload = None;
                // Turn progress request back on.
                self.suppress_progress_request = false;
                ctx.notify(LocalMessages::FetchTask);
            }
            LocalMessages::NotifyTaskCompleted => {
                self.task_manager
                    .try_send(TaskManagerTaskMessage::TaskCompleted(
                        self.current_task.task_id().cloned().unwrap(),
                    ))
                    .unwrap();
            }
            LocalMessages::Tick => {
                trace!(
                    "{}Tick. [{}]",
                    self.get_name(),
                    Local::now().format("%H:%M:%S%.3f").to_string()
                );

                // Uncomment to dump out system load info.
                // warn!("{}", self.system.as_mut().unwrap().get_info());

                // Check whether any task is running currently.
                if !self.current_task.is_empty()
                    && self.current_task.blueprint().unwrap().merge_type != MergeType::Nonclonable
                    && !self.suppress_progress_request
                {
                    let (cpu_load, _, _) = self.system.as_mut().unwrap().get_load();
                    self.last_cpu_load = cpu_load;

                    if cpu_load > hurricane_frontend::conf::get().cloning_cpu_load_threshold {
                        match self.last_overload.as_ref() {
                            None => self.last_overload = Some(Instant::now()),
                            Some(&last_overload) => {
                                let time_sinced_last_overload = last_overload.elapsed();
                                if time_sinced_last_overload
                                    .checked_sub(Duration::from_millis(
                                        hurricane_frontend::conf::get().cloning_time_threshold,
                                    ))
                                    .is_some()
                                {
                                    // Ask for progress update if the overload has been lasting
                                    // for at least the threshold amount of time.
                                    debug!(
                                        "{}    CPU Load {} for at least {:?}, request progress.",
                                        self.get_name(),
                                        cpu_load,
                                        time_sinced_last_overload
                                    );

                                    // Send progress update request.
                                    let task_id = self.current_task.task_id().cloned().unwrap();
                                    self.hurricane_work_io
                                        .as_ref()
                                        .unwrap()
                                        .try_send(HurricaneWorkIORequestProgressUpdate(task_id))
                                        .unwrap();
                                }
                            }
                        }
                    } else {
                        self.last_overload = None;
                    }
                }
            }
        }
    }
}

impl<HA> Handler<TaskManagerCoreWorkIOMessage> for TaskManagerCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = ();

    fn handle(
        &mut self,
        msg: TaskManagerCoreWorkIOMessage,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        match msg {
            TaskManagerCoreWorkIOMessage::ProgressReport(task_id, bytes_done, bytes_total) => {
                // Continue only if `TaskManagerCore` is still working on this task.
                if let Some(current_task_id) = self.current_task.task_id() {
                    if task_id == *current_task_id {
                        let progress: f32 = (bytes_done as f32) / (bytes_total as f32) * 100.0;
                        if ProgressBasedCloningStrategy::new(80.0).offer(
                            self.last_cpu_load,
                            0,
                            0,
                            progress,
                        ) {
                            debug!(
                                "{}        Progress: {}%, sending Clone request.",
                                self.get_name(),
                                progress
                            );

                            // Request for clone on this task.
                            self.task_manager
                                .try_send(TaskManagerTaskMessage::PleaseClone(task_id))
                                .unwrap();

                            // Reset all stats.
                            self.last_overload = None;
                            self.last_cpu_load = 0.0;
                        } else {
                            debug!(
                                "{}        Progress: {}%, Clone rejected by CloningStrategy.",
                                self.get_name(),
                                progress
                            );

                            // Reset all stats.
                            self.last_overload = None;
                            self.last_cpu_load = 0.0;
                        }
                    }
                }
            }
            TaskManagerCoreWorkIOMessage::Ready => {
                assert!(self.tick_handle.is_none());

                let task_manager_tick_interval =
                    hurricane_frontend::conf::get().task_manager_tick_interval;

                ctx.run_later(
                    Duration::from_millis(
                        rand::thread_rng().gen_range(0, task_manager_tick_interval / 2),
                    ),
                    move |actor, ctx0| {
                        actor.tick_handle = Some(ctx0.run_interval(
                            Duration::from_millis(task_manager_tick_interval),
                            |_, ctx1| ctx1.notify(LocalMessages::Tick),
                        ));
                    },
                );

                ctx.notify(LocalMessages::FetchTask);
            }
            TaskManagerCoreWorkIOMessage::TaskSessionClosed(task_id) => {
                assert_eq!(task_id, *self.current_task.task_id().unwrap());
                ctx.notify(LocalMessages::NotifyTaskCompleted);
            }
        }
    }
}

impl<HA> Handler<WorkExecutorDone> for TaskManagerCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: WorkExecutorDone, ctx: &mut Self::Context) -> Self::Result {
        match &self.worker {
            Some(w) if w == &msg.0 => {
                let elapsed_time_since_start = self.start_time.take().unwrap().elapsed();

                info!(
                    "{}    Elapsed time: {:?}.",
                    self.get_name(),
                    elapsed_time_since_start
                );

                // If the task has internal merge to do.
                if let Some((_merge_blueprint, merge_props)) =
                    self.current_task_internal_merge_props.take()
                {
                    info!(
                        "{}    Starting Master worker for internal merge.",
                        self.get_name()
                    );

                    // Start the worker for internal merge.
                    let worker =
                        WorkExecutor::new(self.id, ctx.address().recipient(), merge_props).start();
                    let start_time = Instant::now();

                    self.start_time = Some(start_time);
                    self.worker = Some(worker);
                } else {
                    // The task has done entirely.
                    info!(
                        "{}    Master worker done for Task <{}>!",
                        self.get_name(),
                        self.current_task.task_id().unwrap()
                    );

                    // Turn off progress request for now.
                    self.suppress_progress_request = true;

                    // Close previous task session with HurricaneWorkIO.
                    self.hurricane_work_io
                        .as_ref()
                        .unwrap()
                        .try_send(HurricaneWorkIORequestCloseTaskSession(
                            self.current_task.task_id().cloned().unwrap(),
                        ))
                        .unwrap();

                    // Wait for HurricaneWorkIO to close the task session.
                }
            }
            _ => error!("{}Unknown master worker done!", self.get_name()),
        }
    }
}

impl<HA> Handler<Task> for TaskManagerCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: Task, ctx: &mut Self::Context) -> Self::Result {
        match msg.0 {
            None => {
                self.current_task = Task(None);
                ctx.notify_later(
                    LocalMessages::FetchTask,
                    Duration::from_millis(
                        hurricane_frontend::conf::get().task_manager_fetch_task_interval,
                    ),
                );
            }
            Some((task_id, mut blueprint)) => {
                self.validate_task_blueprint(&task_id, &mut blueprint);

                // Store current task.
                self.current_task = Task(Some((task_id.clone(), blueprint.clone())));

                // Create new task session with HurricaneWorkIO.
                let task_session_request = self
                    .hurricane_work_io
                    .as_ref()
                    .unwrap()
                    .send(HurricaneWorkIORequestNewTaskSession(task_id, blueprint));

                // Once the task session has been successfully created, start the worker.
                let task_session_future = task_session_request
                    .into_actor(self)
                    .map_err(|_, _, _| ())
                    .map(|task_session, actor, ctx| {
                        let HurricaneWorkIOTaskSession {
                            task_id,
                            mut input_bags,
                            mut output_bags,
                            internal_merge_reduce_bags,
                        } = task_session.unwrap();
                        assert_eq!(*actor.current_task.task_id().unwrap(), task_id);

                        let props = if actor.current_task.blueprint().unwrap().is_rename_bag_op() {
                            // Straight forword bag rename operation.
                            assert_eq!(input_bags.len(), 1);
                            assert_eq!(output_bags.len(), 1);
                            let inbag = input_bags.pop().unwrap();
                            let outbag = output_bags.pop().unwrap();

                            // Send from in bag to out bag byte by byte.
                            Props::new(
                                inbag
                                    .execute::<U8Format, _, _>(|st| st)
                                    .into_bag(PhantomData::<U8Format>, outbag),
                            )
                        } else {
                            // Get the Task Props.
                            let mounted_blueprint = MountedBlueprint::from(
                                actor.current_task.blueprint().cloned().unwrap(),
                                input_bags,
                                output_bags,
                            );

                            actor.hurricane_app.instantiate(mounted_blueprint)
                        };

                        // Get the Merge Props.
                        if let Some(((inbag_0, inbag_1), outbag)) = internal_merge_reduce_bags {
                            let cur_blueprint = actor.current_task.blueprint().unwrap();
                            assert_eq!(cur_blueprint.merge_type, MergeType::Reduce);

                            let phase_name = cur_blueprint.name().to_owned();
                            let app_conf = cur_blueprint.app_conf.clone();
                            let merge_blueprint = actor.hurricane_app.merge(
                                phase_name,
                                app_conf,
                                vec![inbag_0.bag.clone(), inbag_1.bag.clone()],
                                vec![outbag.bag.clone()],
                            );
                            match merge_blueprint {
                                Some(mut merge_blueprint) => {
                                    actor.validate_merge_blueprint(&mut merge_blueprint);

                                    let mounted_merge_blueprint = MountedBlueprint::from(
                                        merge_blueprint.clone(),
                                        vec![inbag_0, inbag_1],
                                        vec![outbag],
                                    );
                                    let merge_props =
                                        actor.hurricane_app.instantiate(mounted_merge_blueprint);

                                    actor.current_task_internal_merge_props =
                                        Some((merge_blueprint, merge_props));
                                }
                                None => {
                                    panic!(
                                        "Missing merge routine for Blueprint({}) requiring {:?}!",
                                        cur_blueprint.name(),
                                        cur_blueprint.merge_type
                                    );
                                }
                            }
                        } else {
                            assert!(!actor
                                .current_task
                                .blueprint()
                                .unwrap()
                                .merge_type
                                .require_merge());
                        }

                        // Start the worker.
                        let worker =
                            WorkExecutor::new(actor.id, ctx.address().recipient(), props).start();
                        let start_time = Instant::now();

                        // Task Info.
                        let current_task = &actor.current_task;
                        let bp = current_task.blueprint().unwrap();
                        let inbags_name = bp.inputs.iter().fold(String::new(), |names, bag| {
                            format!("{}{}, ", names, bag.name)
                        });
                        let outbags_name = bp.outputs.iter().fold(String::new(), |names, bag| {
                            format!("{}{}, ", names, bag.name)
                        });

                        info!("{}Starting Master worker for:", actor.get_name());
                        info!(
                            "{}    Task ID:      {}",
                            actor.get_name(),
                            current_task.task_id().unwrap()
                        );
                        info!("{}    Task Name:    {}", actor.get_name(), bp.name());
                        info!("{}    Input Bags:   {}", actor.get_name(), inbags_name);
                        info!("{}    Output Bags:  {}", actor.get_name(), outbags_name);
                        info!("{}    Parallelism:  {}", actor.get_name(), bp.num_threads);
                        info!("{}    MergeType:    {:?}", actor.get_name(), bp.merge_type);

                        actor.start_time = Some(start_time);
                        actor.worker = Some(worker);
                    });

                ctx.wait(task_session_future);
            }
        }
    }
}

impl<HA> Handler<TaskCompletedAck> for TaskManagerCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: TaskCompletedAck, ctx: &mut Self::Context) -> Self::Result {
        match msg.0 {
            Ok(_) => {
                trace!("{}Received Task Completed Ack.", self.get_name());
                ctx.notify(LocalMessages::MoveOn);
            }
            Err(_) => ctx.notify(LocalMessages::NotifyTaskCompleted),
        };
    }
}

impl<HA> Handler<PoisonPill> for TaskManagerCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: PoisonPill, ctx: &mut Self::Context) -> Self::Result {
        debug!("{}Shutting down...", self.get_name());

        self.hurricane_work_io
            .as_ref()
            .unwrap()
            .do_send(msg.clone());
        self.task_manager.do_send(msg);

        // TODO: terminate master worker

        ctx.stop();
    }
}

/// `Message` sent by `HurricaneWorkIO`.
#[derive(Message)]
pub enum TaskManagerCoreWorkIOMessage {
    /// The progress report for task, (bytes done, bytes total).
    ProgressReport(TaskId, usize, usize),

    /// Message to indicate `HurricaneWorkIO` is ready.
    Ready,

    /// Message to indicate `HurricaneWorkIO` has closed task session.
    TaskSessionClosed(TaskId),
}

#[derive(Message)]
enum LocalMessages {
    FetchTask,
    MoveOn,
    NotifyTaskCompleted,
    Tick,
}
