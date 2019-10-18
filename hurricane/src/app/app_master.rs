use super::app_master_core::AppMasterCore;
use super::hurricane_application::*;
use crate::common::{messages::*, types::TaskId};
use crate::communication::app_master_comm::*;
use crate::communication::task_manager_comm::*;
use crate::frontend::hurricane_frontend;
use actix::io::{FramedWrite, WriteHandler};
use actix::prelude::*;
use bincode;
use futures::Stream;
use log::{debug, error, info};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::prelude::*;
use tokio_codec::FramedRead;
use tokio_io::io::WriteHalf;
use tokio_tcp::{TcpListener, TcpStream};

/// TCP Network Layer Actor of App Master.
pub struct AppMaster<HA>
where
    HA: HurricaneApplication + 'static,
{
    /// Identity of this `AppMaster`.
    id: usize,
    self_socket_addr: Option<SocketAddr>,

    /// The real `HurricaneApplication`, saved for `AppMasterCore`.
    hurricane_app: Option<HA>,

    is_shutting_down: bool,

    /// Incoming connections.
    num_incoming_tcp_connections: usize,
    /// Channels of `TaskManager`s.
    task_managers: HashMap<SocketAddr, FramedWrite<WriteHalf<TcpStream>, TaskManagerCommCodec>>,
    /// Address of `AppMasterCore`, the real actor of App Master.
    app_master_core: Option<Addr<AppMasterCore<HA>>>,
}

impl<HA> AppMaster<HA>
where
    HA: HurricaneApplication + 'static,
{
    fn produce_name(id: usize) -> String {
        format!(GET_NAME_FORMAT!(), format!("[{}, None]", id), "AppMaster")
    }

    fn get_name(&self) -> String {
        if let Some(self_socket_addr) = self.self_socket_addr {
            format!(
                GET_NAME_FORMAT!(),
                format!("[{}, {}]", self.id, self_socket_addr),
                "AppMaster"
            )
        } else {
            format!(
                GET_NAME_FORMAT!(),
                format!("[{}, None]", self.id),
                "AppMaster"
            )
        }
    }

    pub fn spawn(id: usize, hurricane_app: HA) -> Addr<AppMaster<HA>> {
        let current_addr = hurricane_frontend::conf::get().app_master_socket_addr;
        let listener = TcpListener::bind(&current_addr).unwrap();

        Arbiter::builder()
            .name(format!("Arbiter-AppMaster-{}", id))
            .stop_system_on_panic(true)
            .start(move |ctx: &mut Context<AppMaster<HA>>| {
                info!(
                    "{}Waiting for {} incomming connections from TaskManager.",
                    Self::produce_name(id),
                    hurricane_frontend::conf::get().num_task_manager
                );

                ctx.add_message_stream(
                    listener
                        .incoming()
                        .take(hurricane_frontend::conf::get().num_task_manager as u64)
                        .map_err(move |_err| {
                            error!("{}Connection failed.", Self::produce_name(id));
                            System::current().stop();
                        })
                        .map(|socket| TcpConnect(socket)),
                );

                AppMaster {
                    id,
                    self_socket_addr: Some(current_addr.clone()),
                    hurricane_app: Some(hurricane_app),
                    is_shutting_down: false,
                    num_incoming_tcp_connections: 0,
                    task_managers: HashMap::new(),
                    app_master_core: None,
                }
            })
    }

    /// Determines whether the central `AppMaster` should shutdown.
    ///
    /// As `AppMaster` is the curator of `AppMasterCore`, and
    /// it also handles all incoming connection, it can only
    /// be shutdown after all incoming connections are dropped.
    fn should_say_goodbye(&self) -> bool {
        self.num_incoming_tcp_connections == 0
    }
}

impl<HA> Actor for AppMaster<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Context = Context<Self>;

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("{}Bye~", self.get_name());
    }
}

impl<HA> StreamHandler<AppMasterComm, bincode::Error> for AppMaster<HA>
where
    HA: HurricaneApplication + 'static,
{
    fn finished(&mut self, ctx: &mut Self::Context) {
        self.num_incoming_tcp_connections -= 1;
        if self.should_say_goodbye() {
            ctx.stop();
        }
    }

    fn handle(&mut self, msg: AppMasterComm, ctx: &mut Context<Self>) {
        match msg {
            AppMasterComm::TaskManagerGetTask(sender_socket_addr) => {
                if !self.is_shutting_down {
                    // Fetch task from AppMasterCore.
                    let req = self
                        .app_master_core
                        .as_ref()
                        .unwrap()
                        .send(AppMasterGetTask);

                    let task =
                        req.into_actor(self)
                            .map_err(|_, _, _| ())
                            .map(move |task, actor, _ctx| {
                                if !actor.is_shutting_down {
                                    // Send result back to TaskManager.
                                    actor
                                        .task_managers
                                        .get_mut(&sender_socket_addr)
                                        .unwrap()
                                        .write(TaskManagerComm::AppMasterTask(task));
                                }
                            });

                    ctx.spawn(task);
                }
            }
            AppMasterComm::TaskManagerTaskCompleted(sender_socket_addr, task_id) => {
                // Send task completed notification to AppMasterCore.
                let req = self
                    .app_master_core
                    .as_ref()
                    .unwrap()
                    .send(AppMasterTaskCompleted(task_id));

                let task =
                    req.into_actor(self)
                        .map_err(|_, _, _| ())
                        .map(move |ack, actor, _ctx| {
                            // Send result back to TaskManager.
                            actor
                                .task_managers
                                .get_mut(&sender_socket_addr)
                                .unwrap()
                                .write(TaskManagerComm::AppMasterTaskCompletedAck(ack));
                        });

                ctx.spawn(task);
            }
            AppMasterComm::TaskManagerPleaseClone(_sender_socket_addr, task_id) => {
                // Redirect PleaseClone message to AppMasterCore.
                self.app_master_core
                    .as_ref()
                    .unwrap()
                    .try_send(AppMasterPleaseClone(task_id))
                    .unwrap();
            }
        };
    }
}

impl<HA> Handler<TcpConnect> for AppMaster<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: TcpConnect, ctx: &mut Context<Self>) -> Self::Result {
        let socket = msg.0;

        socket
            .set_nodelay(hurricane_frontend::conf::get().app_master_tcp_nodelay)
            .unwrap();

        let peer_addr = socket.peer_addr().unwrap();
        let local_addr = socket.local_addr().unwrap();

        assert_eq!(Some(local_addr), self.self_socket_addr);

        info!(
            "{}Connection established with Task Manager: {}.",
            self.get_name(),
            peer_addr
        );
        self.num_incoming_tcp_connections += 1;

        let (r, w) = socket.split();

        // Register the r part with stream handler.
        let message_reader = FramedRead::new(r, AppMasterCommCodec);
        ctx.add_stream(message_reader);
        // Store the w part.
        let r = self
            .task_managers
            .insert(peer_addr, FramedWrite::new(w, TaskManagerCommCodec, ctx));
        assert!(r.is_none());

        // If all task managers are connected.
        if self.task_managers.len() == hurricane_frontend::conf::get().num_task_manager {
            debug!(
                "{}All Task Managers are connected, starting AppMasterCore.",
                self.get_name()
            );

            assert!(
                self.app_master_core == None,
                "Cannot start multiple AppMasterCore actors within the same AppMaster actor!"
            );
            self.app_master_core = Some(
                AppMasterCore::new(self.id, ctx.address(), self.hurricane_app.take().unwrap())
                    .start(),
            );

            let self_socket_addr = self.self_socket_addr.unwrap();
            // Inform all Task Managers that App Master is ready.
            self.task_managers
                .iter_mut()
                .for_each(|(_, ch)| ch.write(TaskManagerComm::AppMasterIsReady(self_socket_addr)));
        }
    }
}

impl<HA> Handler<PoisonPill> for AppMaster<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = ();

    fn handle(&mut self, _msg: PoisonPill, _ctx: &mut Self::Context) -> Self::Result {
        let self_name = self.get_name();
        debug!("{}Shutting down...", self_name);

        self.is_shutting_down = true;

        // Inform all worker nodes.
        self.task_managers.drain().for_each(|(socket, mut ch)| {
            ch.write(TaskManagerComm::PoisonPill);
            debug!("{}Informing {} for PoisonPill.", self_name, socket);
        });
    }
}

impl<HA> WriteHandler<bincode::Error> for AppMaster<HA> where HA: HurricaneApplication + 'static {}

/// Specialized `GetTask` message for `AppMaster`.
pub struct AppMasterGetTask;

impl Message for AppMasterGetTask {
    type Result = Task;
}

/// Specialized `TaskCompleted` message for `AppMaster`.
pub struct AppMasterTaskCompleted(pub TaskId);

impl Message for AppMasterTaskCompleted {
    type Result = TaskCompletedAck;
}

/// Specialized `PleaseClone` wrapper for `AppMaster`.
pub struct AppMasterPleaseClone(pub TaskId);

impl Message for AppMasterPleaseClone {
    type Result = Result<(), ()>;
}
