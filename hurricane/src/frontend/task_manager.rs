use super::hurricane_frontend;
use super::task_manager_core::TaskManagerCore;
use crate::app::hurricane_application::*;
use crate::common::{messages::*, types::*};
use crate::communication::app_master_comm::*;
use crate::communication::task_manager_comm::*;
use actix::io::{FramedWrite, WriteHandler};
use actix::prelude::*;
use bincode;
use log::*;
use std::net::SocketAddr;
use tokio::prelude::*;
use tokio_codec::FramedRead;
use tokio_io::io::WriteHalf;
use tokio_retry::strategy::FixedInterval;
use tokio_retry::Retry;
use tokio_tcp::TcpStream;

/// TCP Network Layer Actor of TaskManager for intra-Frontend communication with AppMaster.
///
/// This layer is named `TaskManager` simply because other external actors
/// would deem this actor as the gateway for task management within this compute node.
pub struct TaskManager<HA>
where
    HA: HurricaneApplication + 'static,
{
    /// Identity of this TaskManager.
    id: usize,
    /// Socket addr of TaskManager TCP network layer.
    self_socket_addr: Option<SocketAddr>,

    /// The real `HurricaneApplication`, saved for `TaskManagerCore`.
    hurricane_app: Option<HA>,

    /// TCP channel with AppMaster.
    app_master_channel: Option<(
        SocketAddr,
        FramedWrite<WriteHalf<TcpStream>, AppMasterCommCodec>,
    )>,

    /// Address of `TaskManagerCore` actor, the actor logic of Task Manager.
    task_manager_core: Option<Addr<TaskManagerCore<HA>>>,
}

impl<HA> TaskManager<HA>
where
    HA: HurricaneApplication + 'static,
{
    fn produce_name(id: usize) -> String {
        format!(GET_NAME_FORMAT!(), format!("[{}, None]", id), "TaskManager")
    }

    fn get_name(&self) -> String {
        if let Some(self_socket_addr) = self.self_socket_addr {
            format!(
                GET_NAME_FORMAT!(),
                format!("[{}, {}]", self.id, self_socket_addr),
                "TaskManager"
            )
        } else {
            format!(
                GET_NAME_FORMAT!(),
                format!("[{}, None]", self.id),
                "TaskManager"
            )
        }
    }

    pub fn spawn(id: usize, hurricane_app: HA) -> Addr<TaskManager<HA>> {
        TaskManager::create(move |ctx| {
            let app_master_addr = hurricane_frontend::conf::get().app_master_socket_addr;
            info!(
                "{}Connecting to AppMaster: {}.",
                Self::produce_name(id),
                app_master_addr
            );

            let retry_strategy = FixedInterval::from_millis(
                hurricane_frontend::conf::get().task_manager_connection_retry_interval,
            )
            .take(hurricane_frontend::conf::get().task_manager_connection_retry_attempt);

            let mut attempt_count = 0;

            ctx.add_message_stream(
                Retry::spawn(retry_strategy.clone(), move || {
                    trace!(
                        "{}Connection attempt {} for {}.",
                        Self::produce_name(id),
                        attempt_count,
                        app_master_addr
                    );
                    attempt_count += 1;
                    TcpStream::connect(&app_master_addr)
                })
                .map_err(move |err| {
                    error!(
                        "{}Connection failed for AppMaster: {}!",
                        Self::produce_name(id),
                        app_master_addr
                    );
                    error!("{}{}.", Self::produce_name(id), err);
                    System::current().stop();
                })
                .map(|socket| TcpConnect(socket))
                .into_stream(),
            );

            TaskManager {
                id,
                self_socket_addr: None,
                hurricane_app: Some(hurricane_app),
                app_master_channel: None,
                task_manager_core: None,
            }
        })
    }
}

impl<HA> Actor for TaskManager<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Context = Context<Self>;

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("{}Bye~", self.get_name());
    }
}

impl<HA> StreamHandler<TaskManagerComm, bincode::Error> for TaskManager<HA>
where
    HA: HurricaneApplication + 'static,
{
    fn finished(&mut self, ctx: &mut Self::Context) {
        error!("{}AppMaster dropped connection!", self.get_name());
        // TODO: terminate task manager core
        ctx.stop();
    }

    fn handle(&mut self, msg: TaskManagerComm, ctx: &mut Context<Self>) {
        match msg {
            TaskManagerComm::AppMasterIsReady(sender_socket_addr) => {
                let (app_master_socket, _) = self.app_master_channel.as_ref().unwrap();
                assert_eq!(sender_socket_addr, *app_master_socket);

                debug!(
                    "{}App Master is ready, starting TaskManagerCore.",
                    self.get_name()
                );

                assert!(self.task_manager_core == None, "Cannot start multiple TaskManagerCore actors within the same TaskManager actor!");
                self.task_manager_core = Some(
                    TaskManagerCore::new(
                        self.id,
                        ctx.address(),
                        self.hurricane_app.take().unwrap(),
                    )
                    .start(),
                );
            }
            TaskManagerComm::AppMasterTask(task) => {
                // send task result back to TaskManagerCore.
                self.task_manager_core
                    .as_ref()
                    .unwrap()
                    .try_send(task)
                    .unwrap();
            }
            TaskManagerComm::AppMasterTaskCompletedAck(ack) => {
                // send task completed ack to TaskManagerCore.
                self.task_manager_core
                    .as_ref()
                    .unwrap()
                    .try_send(ack)
                    .unwrap();
            }
            TaskManagerComm::PoisonPill => {
                // send PoisonPill to TaskManagerCore.
                self.task_manager_core
                    .as_ref()
                    .unwrap()
                    .try_send(PoisonPill)
                    .unwrap();

                debug!(
                    "{}Received PoisonPill, redirecting to TaskManagerCore.",
                    self.get_name()
                );
            }
        };
    }
}

impl<HA> Handler<TcpConnect> for TaskManager<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: TcpConnect, ctx: &mut Context<Self>) -> Self::Result {
        let socket = msg.0;

        socket
            .set_nodelay(hurricane_frontend::conf::get().task_manager_tcp_nodelay)
            .unwrap();

        let peer_addr = socket.peer_addr().unwrap();
        let local_addr = socket.local_addr().unwrap();

        if self.self_socket_addr == None {
            self.self_socket_addr = Some(local_addr);
        }

        info!(
            "{}Connection established with App Master: {}.",
            self.get_name(),
            peer_addr
        );

        let (r, w) = socket.split();

        // Register the r part with stream handler
        let message_reader = FramedRead::new(r, TaskManagerCommCodec);
        ctx.add_stream(message_reader);
        // Store the w part
        self.app_master_channel = Some((peer_addr, FramedWrite::new(w, AppMasterCommCodec, ctx)));
    }
}

impl<HA> Handler<TaskManagerTaskMessage> for TaskManager<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: TaskManagerTaskMessage, _ctx: &mut Self::Context) -> Self::Result {
        let (_, framed) = self.app_master_channel.as_mut().unwrap();

        match msg {
            TaskManagerTaskMessage::GetTask => {
                framed.write(AppMasterComm::TaskManagerGetTask(
                    self.self_socket_addr.unwrap(),
                ));
            }
            TaskManagerTaskMessage::TaskCompleted(task_id) => {
                framed.write(AppMasterComm::TaskManagerTaskCompleted(
                    self.self_socket_addr.unwrap(),
                    task_id,
                ));
            }
            TaskManagerTaskMessage::PleaseClone(task_id) => {
                framed.write(AppMasterComm::TaskManagerPleaseClone(
                    self.self_socket_addr.unwrap(),
                    task_id,
                ));
            }
        }
    }
}

impl<HA> Handler<PoisonPill> for TaskManager<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = ();

    fn handle(&mut self, _msg: PoisonPill, ctx: &mut Self::Context) -> Self::Result {
        debug!("{}Shutting down...", self.get_name());

        ctx.stop();
    }
}

impl<HA> WriteHandler<bincode::Error> for TaskManager<HA> where HA: HurricaneApplication + 'static {}

#[derive(Message)]
pub enum TaskManagerTaskMessage {
    GetTask,
    TaskCompleted(TaskId),
    PleaseClone(TaskId),
}
