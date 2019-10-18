use super::hurricane_backend;
use crate::common::bag::Bag;
use crate::common::chunk::*;
use crate::common::chunk_pool::ChunkPool;
use crate::common::messages::TcpConnect;
use crate::common::primitive_serializer::*;
use crate::communication::hurricane_io_comm::*;
use crate::communication::hurricane_work_io_comm::*;
use crate::frontend::hurricane_frontend;
use actix::io::{FramedWrite, WriteHandler};
use actix::prelude::*;
use bincode;
use futures::Stream;
use log::*;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use tokio::prelude::*;
use tokio_codec::FramedRead;
use tokio_io::io::WriteHalf;
use tokio_tcp::{TcpListener, TcpStream};

fn debug_mode() -> bool {
    true
}

fn make_chunks(num_chunks: usize) -> VecDeque<Chunk> {
    let chunk = ChunkPool::allocate_filled_u8(1, None);
    (0..num_chunks).map(|_| chunk.clone()).collect()
}

fn make_bags<T>(bags: T) -> (HashMap<Bag, VecDeque<Chunk>>, HashMap<Bag, usize>)
where
    T: IntoIterator<Item = (Bag, usize)>,
{
    bags.into_iter()
        .map(|(bag, num_chunks)| ((bag.clone(), make_chunks(num_chunks)), (bag, num_chunks)))
        .unzip()
}

/// Major backend actor
///
/// It feeds the data to the computation nodes based on the requests.
/// The data can be from different sources (disk, memory, network, etc.).
/// But for now, we assume the data is from the local disk.
///
/// Internally, a threadpool should be used for performing
/// concurrent file access operations. The entire operation
/// should be done using Future.
///
/// A future of inserting or pulling out a chunk from a file is created
/// once the request is received. Then the future is fed into the threadpool.
/// Once the future has been executed, the corresponding ack/response is then sent
/// to the requester `HurricaneWorkIO`.
///
/// https://docs.rs/tokio/0.1.15/tokio/fs/index.html
pub struct HurricaneIO {
    /// Identity of this `HurricaneIO`.
    id: usize,
    /// The socket address of this `HurricaneIO`.
    self_socket_addr: Option<SocketAddr>,

    /// Incoming connections.
    num_incoming_tcp_connections: usize,
    /// Channels of `HurricaneWorkIO`s.
    hurricane_work_ios:
        HashMap<SocketAddr, FramedWrite<WriteHalf<TcpStream>, HurricaneWorkIOCommCodec>>,

    /// Mock Files, for TCP layer testing.
    /// Use `push_back` to add, and `pop_front` to take.
    bags: HashMap<Bag, VecDeque<Chunk>>,

    /// Mock Files, for TCP layer testing.
    /// This keeps track of the total number of chunks for `Bag`,
    /// stored on this storage node.
    bags_stat: HashMap<Bag, usize>,
}

impl HurricaneIO {
    fn produce_name(id: usize) -> String {
        format!(GET_NAME_FORMAT!(), format!("[{}, None]", id), "HurricaneIO")
    }

    fn get_name(&self) -> String {
        if let Some(self_socket_addr) = self.self_socket_addr {
            format!(
                GET_NAME_FORMAT!(),
                format!("[{}, {}]", self.id, self_socket_addr),
                "HurricaneIO"
            )
        } else {
            format!(
                GET_NAME_FORMAT!(),
                format!("[{}, None]", self.id),
                "HurricaneIO"
            )
        }
    }

    pub fn spawn(id: usize) -> Addr<HurricaneIO> {
        let current_addr = hurricane_backend::conf::get()
            .hurricane_io_socket_addrs
            .get(id)
            .expect("Cannot find this node.");
        let listener = TcpListener::bind(current_addr).unwrap();

        Arbiter::builder()
            .name(format!("Arbiter-HurricaneIO-{}", id))
            .stop_system_on_panic(true)
            .start(move |ctx: &mut Context<HurricaneIO>| {
                info!(
                    "{}Waiting for {} incomming connections from HurricaneWorkIO.",
                    Self::produce_name(id),
                    hurricane_frontend::conf::get().num_task_manager
                );

                ctx.add_message_stream(
                    listener
                        .incoming()
                        .take(hurricane_frontend::conf::get().num_task_manager as u64)
                        .map_err(move |err| {
                            error!("{}Connection failed.", Self::produce_name(id));
                            error!("{}{}.", Self::produce_name(id), err);
                            System::current().stop();
                        })
                        .map(|socket| TcpConnect(socket)),
                );

                // Use conf to build fake data.
                let (bags, bags_stat) = hurricane_backend::conf::get()
                    .hurricane_io_init_chunks
                    .get(id)
                    .map(|node_init_chunks| {
                        make_bags(node_init_chunks.iter().map(|(name, count)| {
                            info!(
                                "{}Constructed Bag({}) for {} chunks.",
                                Self::produce_name(id),
                                name,
                                *count
                            );
                            (Bag::from(name), *count)
                        }))
                    })
                    .unwrap_or_default();

                HurricaneIO {
                    id,
                    self_socket_addr: Some(current_addr.clone()),
                    num_incoming_tcp_connections: 0,
                    hurricane_work_ios: HashMap::new(),
                    bags,
                    bags_stat,
                }
            })
    }

    fn should_say_goodbye(&self) -> bool {
        self.num_incoming_tcp_connections == 0
    }
}

impl Actor for HurricaneIO {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        if debug_mode() {
            info!("{}", self.get_name());
            info!("{}All Chunks:", self.get_name());
            info!("{}{:?}", self.get_name(), self.bags_stat);
            info!("{}", self.get_name());
        }
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if debug_mode() {
            // Debug
            let out_chunks: HashMap<_, _> = self
                .bags
                .drain()
                .filter(|(_, chunks)| !chunks.is_empty())
                .map(|(bag, chunks)| {
                    let chunks: Vec<Vec<_>> = chunks
                        .into_iter()
                        .map(|chunk| chunk.into_iter::<I64Format>().collect())
                        .collect();
                    (bag, chunks)
                })
                .collect();
            info!("{}", self.get_name());
            info!("{}All Chunks:", self.get_name());
            info!("{}{:?}", self.get_name(), out_chunks);
            info!("{}", self.get_name());
            info!("{}Chunks Processed:", self.get_name());
            info!("{}{:?}", self.get_name(), self.bags_stat);
            info!("{}", self.get_name());
        }

        info!("{}Stopping.", self.get_name());
    }
}

/// HurricaneIO stream handler for the messages in DataIOComm.
impl StreamHandler<HurricaneIOComm, bincode::Error> for HurricaneIO {
    fn finished(&mut self, ctx: &mut Self::Context) {
        self.num_incoming_tcp_connections -= 1;
        if self.should_say_goodbye() {
            ctx.stop();
        }
    }

    fn handle(&mut self, msg: HurricaneIOComm, _ctx: &mut Context<Self>) {
        match msg {
            HurricaneIOComm::Fill(addr, fingerprint, bag) => {
                match self
                    .bags
                    .get_mut(&bag)
                    .and_then(|chunks| chunks.pop_front())
                {
                    Some(chunk) => self.hurricane_work_ios.get_mut(&addr).unwrap().write(
                        if hurricane_backend::conf::get().send_inbags_as_record {
                            // Send record of chunk.
                            HurricaneWorkIOComm::FakeFilled(
                                self.self_socket_addr.unwrap(),
                                fingerprint,
                                bag,
                                chunk.get_chunk_size(),
                            )
                        } else {
                            HurricaneWorkIOComm::Filled(
                                self.self_socket_addr.unwrap(),
                                fingerprint,
                                bag,
                                chunk,
                            )
                        },
                    ),
                    None => {
                        trace!("{}Bag({}) is empty.", self.get_name(), bag.name);

                        self.hurricane_work_ios.get_mut(&addr).unwrap().write(
                            HurricaneWorkIOComm::EOF(
                                self.self_socket_addr.unwrap(),
                                fingerprint,
                                bag,
                            ),
                        )
                    }
                }
            }
            HurricaneIOComm::Drain(addr, fingerprint, bag, chunk) => {
                self.bags
                    .entry(bag.clone())
                    .or_insert(VecDeque::new())
                    .push_back(chunk);

                // Increment the chunks_total for bag.
                *self.bags_stat.entry(bag.clone()).or_insert(0) += 1;

                self.hurricane_work_ios.get_mut(&addr).unwrap().write(
                    HurricaneWorkIOComm::AckDrain(self.self_socket_addr.unwrap(), fingerprint, bag),
                );
            }
            HurricaneIOComm::FakeDrain(addr, fingerprint, bag, size) => {
                self.bags
                    .entry(bag.clone())
                    .or_insert(VecDeque::new())
                    .push_back(ChunkPool::allocate_filled_u8(1, Some(size)));

                // Increment the chunks_total for bag.
                *self.bags_stat.entry(bag.clone()).or_insert(0) += 1;

                self.hurricane_work_ios.get_mut(&addr).unwrap().write(
                    HurricaneWorkIOComm::AckDrain(self.self_socket_addr.unwrap(), fingerprint, bag),
                );
            }
            HurricaneIOComm::Progress(addr, task_id, fingerprint, bag) => {
                let chunks_total = self.bags_stat.get(&bag).cloned().unwrap_or(0);
                let chunks_left = self.bags.get(&bag).map(|chunks| chunks.len()).unwrap_or(0);

                debug!(
                    "{}Received progress request for Bag({}): {}/{}.",
                    self.get_name(),
                    bag.name,
                    chunks_total - chunks_left,
                    chunks_total
                );

                self.hurricane_work_ios.get_mut(&addr).unwrap().write(
                    HurricaneWorkIOComm::ProgressReport(
                        self.self_socket_addr.unwrap(),
                        task_id,
                        fingerprint,
                        bag,
                        chunks_total - chunks_left,
                        chunks_total,
                    ),
                );
            }
        };
    }
}

/// HurricaneIO TCP incomming connection handler.
impl Handler<TcpConnect> for HurricaneIO {
    type Result = ();

    fn handle(&mut self, msg: TcpConnect, ctx: &mut Context<Self>) -> Self::Result {
        let socket = msg.0;

        socket
            .set_nodelay(hurricane_backend::conf::get().hurricane_io_tcp_nodelay)
            .unwrap();
        if let Some(buf_size) = hurricane_backend::conf::get().hurricane_io_so_rcvbuf {
            socket.set_recv_buffer_size(buf_size).unwrap();
        }
        if let Some(buf_size) = hurricane_backend::conf::get().hurricane_io_so_sndbuf {
            socket.set_send_buffer_size(buf_size).unwrap();
        }

        let peer_addr = socket.peer_addr().unwrap();
        let local_addr = socket.local_addr().unwrap();

        assert_eq!(Some(local_addr), self.self_socket_addr);

        info!(
            "{}Connection established with Hurricane Work IO: {}.",
            self.get_name(),
            peer_addr
        );
        self.num_incoming_tcp_connections += 1;

        let (r, w) = socket.split();

        // register the r part with stream handler
        let message_reader = FramedRead::new(r, HurricaneIOCommCodec);
        ctx.add_stream(message_reader);
        // store the w part
        let r = self.hurricane_work_ios.insert(
            peer_addr,
            FramedWrite::new(w, HurricaneWorkIOCommCodec, ctx),
        );
        assert!(r.is_none());

        if self.hurricane_work_ios.len() == hurricane_frontend::conf::get().num_task_manager {
            info!("{}Ready to process incoming requests!", self.get_name());
        }
    }
}

impl WriteHandler<bincode::Error> for HurricaneIO {}
