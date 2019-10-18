use super::messages::*;
use crate::common::props::{FinalProps, Props};
use actix::prelude::*;
use log::{debug, error, trace, warn};

pub struct WorkExecutor {
    id: usize,
    task_manager: Recipient<WorkExecutorDone>,
    props: Option<Props>,
    workers: Vec<Option<Addr<HurricaneGraphWork>>>,
    num_workers_done: usize,
}

impl WorkExecutor {
    pub fn new(id: usize, task_manager: Recipient<WorkExecutorDone>, props: Props) -> WorkExecutor {
        WorkExecutor {
            id,
            task_manager,
            props: Some(props),
            workers: Vec::new(),
            num_workers_done: 0,
        }
    }

    fn get_name(&self) -> String {
        format!(
            GET_NAME_FORMAT!(),
            format!("[{}, Core]", self.id),
            "WorkExecutor"
        )
    }

    fn terminate(&mut self, ctx: &mut <Self as Actor>::Context) {
        if self.num_workers_done < self.workers.len() {
            error!("{}Failed to execute all slave workers!", self.get_name());
        } else {
            trace!("{}    All slave workers done!", self.get_name());
        }

        self.task_manager
            .try_send(Done(ctx.address(), Ok(())))
            .unwrap();

        ctx.stop();
    }
}

impl Actor for WorkExecutor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // create slave workers
        self.props
            .take()
            .unwrap()
            .into_final_props_iter()
            .enumerate()
            .for_each(|(slave_id, final_props)| {
                let worker = HurricaneGraphWork::new(
                    slave_id,
                    self.id,
                    ctx.address().recipient(),
                    final_props,
                );

                let worker = Arbiter::builder()
                    .name(format!(
                        "Arbiter-HurricaneGraphWork-M{}-S{}",
                        self.id, slave_id
                    ))
                    .stop_system_on_panic(true)
                    .start(|_| worker);
                self.workers.push(Some(worker));
            });

        debug!(
            "{}    Starting {} slave workers.",
            self.get_name(),
            self.workers.len()
        );

        // inform slave workers to start working
        self.workers
            .iter()
            .for_each(|worker| worker.as_ref().unwrap().try_send(Start).unwrap());
    }
}

impl Handler<HurricaneGraphWorkDone> for WorkExecutor {
    type Result = ();

    fn handle(&mut self, msg: HurricaneGraphWorkDone, ctx: &mut Self::Context) -> Self::Result {
        match &msg.1 {
            Ok(_) => {
                trace!("{}    Slave worker {} done!", self.get_name(), msg.0);
                // the worker address must be valid
                assert!(self.workers[msg.0].is_some());

                self.workers[msg.0] = None;
                self.num_workers_done += 1;
                if self.workers.len() == self.num_workers_done {
                    self.terminate(ctx);
                }
            }
            Err(_) => {
                warn!("{}Slave worker {} failed!", self.get_name(), msg.0);
                self.terminate(ctx);
            }
        }
    }
}

/// (Addr<WorkExecutor>,())
pub type WorkExecutorDone = Done<Addr<WorkExecutor>, (), ()>;

#[allow(dead_code)]
pub struct HurricaneGraphWork {
    id: usize,
    work_executor_id: usize,
    work_executor_recipient: Recipient<HurricaneGraphWorkDone>,
    final_props: Option<FinalProps>,
}

impl HurricaneGraphWork {
    fn new(
        id: usize,
        work_executor_id: usize,
        work_executor_recipient: Recipient<HurricaneGraphWorkDone>,
        final_props: FinalProps,
    ) -> HurricaneGraphWork {
        HurricaneGraphWork {
            id,
            work_executor_id,
            work_executor_recipient,
            final_props: Some(final_props),
        }
    }

    #[allow(dead_code)]
    fn get_name(&self) -> String {
        format!(
            GET_NAME_FORMAT!(),
            format!("[{}, {}]", self.work_executor_id, self.id),
            "HGraphWork"
        )
    }
}

impl Actor for HurricaneGraphWork {
    type Context = Context<Self>;
}

impl Handler<Start> for HurricaneGraphWork {
    type Result = ();

    fn handle(&mut self, _: Start, ctx: &mut Self::Context) -> Self::Result {
        let self_id = self.id;
        let task = self
            .final_props
            .take()
            .unwrap()
            .take()
            .into_actor(self)
            .map_err(move |_, _, ctx| {
                ctx.notify(Done(self_id, Err(())));
                ()
            })
            .then(move |_, _, ctx| {
                ctx.notify(Done(self_id, Ok(())));
                actix::fut::ok(())
            });

        ctx.spawn(task);
    }
}

impl Handler<HurricaneGraphWorkDone> for HurricaneGraphWork {
    type Result = ();

    fn handle(&mut self, msg: HurricaneGraphWorkDone, ctx: &mut Self::Context) -> Self::Result {
        // redirect msg to WorkExecutor
        self.work_executor_recipient.try_send(msg).unwrap();

        ctx.stop();
    }
}

/// (slave_id, result)
type HurricaneGraphWorkDone = Done<usize, (), ()>;
