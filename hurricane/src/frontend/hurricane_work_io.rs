use super::task_manager_core::TaskManagerCoreWorkIOMessage;
use crate::app::blueprint::*;
use crate::backend::hurricane_backend;
use crate::common::bag::*;
use crate::common::chunk::*;
use crate::common::chunk_pool::*;
use crate::common::messages::*;
use crate::common::types::*;
use crate::communication::hurricane_io_comm::*;
use crate::communication::hurricane_work_io_comm::*;
use crate::frontend::hurricane_frontend;
use actix::io::{FramedWrite, WriteHandler};
use actix::prelude::*;
use bincode;
use futures::sync::mpsc::*;
use log::*;
use std::collections::HashMap;
use std::iter::Cycle;
use std::net::SocketAddr;
use std::time::Duration;
use std::vec::IntoIter;
use tokio::prelude::*;
use tokio_codec::FramedRead;
use tokio_io::io::WriteHalf;
use tokio_retry::strategy::FixedInterval;
use tokio_retry::Retry;
use tokio_tcp::TcpStream;
use uuid::Uuid;

fn generate_random_uuid() -> Uuid {
    Uuid::new_v4()
}

struct WorkSession {
    task_id: TaskId,
    blueprint: Blueprint,

    /// Multiple `Filler` channels for a `Bag`, one for each slave worker, i.e. `HurricaneGraphWork`.
    ///
    /// The index for `Vec<Sender<Option<Chunk>>>` is the `Filler` ID.
    bags_filler: Vec<(Bag, Vec<Sender<Option<Chunk>>>)>,

    /// Single `Drainer` channel for a `Bag` for all slave workers, i.e. `HurricaneGraphWork`.
    ///
    /// Stores the registered Receiver SpawnHandle
    bags_drainer: Vec<(Bag, actix::SpawnHandle)>,

    /// Intermediate chunks for internal merging.
    ///
    /// Bag -> (ready terminal count, chunks)
    intermediate_chunks: Option<Vec<(Bag, (u8, Vec<Chunk>))>>,

    /// Whether or not currently doing internal reduction merging.
    in_reduction_merge_stage: bool,

    /// All available `HurricaneIO`s' addresses that hold chunks for each `Bag`.
    bag_hurricane_ios: Vec<(Bag, Vec<SocketAddr>)>,
    /// A cyclic view into `HurricaneIO`s' addresses for each `Bag`.
    bag_hurricane_ios_cycle: Vec<Cycle<IntoIter<SocketAddr>>>,

    /// A list of outstanding bag filler requests.
    ///
    /// `FingerPrint` -> `(Bag, Filler ID, HurricaneIO)`
    bag_filler_requests: HashMap<FingerPrint, (Bag, usize, SocketAddr)>,

    /// A list of outstanding bag drainer requests.
    ///
    /// `FingerPrint` -> `(Bag, HurricaneIO)`
    bag_drainer_requests: HashMap<FingerPrint, (Bag, SocketAddr)>,

    /// A list of outstanding progress report query requests.
    ///
    /// `FingerPrint` -> `(Bag, HurricaneIO)`
    progress_report_requests: HashMap<FingerPrint, (Bag, SocketAddr)>,

    /// A draft of composing the progress report.
    /// A progress report should be initiated only when
    /// there is currently no `progress_report_draft` on air.
    ///
    /// `(Bag, Bytes Done, Bytes Total)`
    progress_report_draft: Option<Vec<(Bag, usize, usize)>>,

    /// The stats of number of chunks for the `Bag` filled and drained.
    ///
    /// `Bag` -> `(filled count, drained count)`
    bags_stats: HashMap<Bag, (usize, usize)>,
}

impl WorkSession {
    fn new(
        task_id: TaskId,
        blueprint: Blueprint,
        bags_filler: Vec<(Bag, Vec<Sender<Option<Chunk>>>)>,
        bags_drainer: Vec<(Bag, actix::SpawnHandle)>,
        intermediate_chunks: Option<Vec<(Bag, (u8, Vec<Chunk>))>>,
        bag_hurricane_ios: Vec<(Bag, Vec<SocketAddr>)>,
    ) -> WorkSession {
        let bag_hurricane_ios_cycle = bag_hurricane_ios
            .iter()
            .map(|(_, ios)| ios.clone().into_iter().cycle())
            .collect();

        WorkSession {
            task_id,
            blueprint,
            bags_filler,
            bags_drainer,
            intermediate_chunks,
            in_reduction_merge_stage: false,
            bag_hurricane_ios,
            bag_hurricane_ios_cycle,
            bag_filler_requests: HashMap::new(),
            bag_drainer_requests: HashMap::new(),
            progress_report_requests: HashMap::new(),
            progress_report_draft: None,
            bags_stats: HashMap::new(),
        }
    }

    fn close(
        mut self,
        task_id: &TaskId,
        work_io_name: &str,
        ctx: &mut Context<HurricaneWorkIO>,
    ) -> Result<(), WorkSession> {
        assert_eq!(self.task_id, *task_id);

        if !self.bag_filler_requests.is_empty()
            || !self.bag_drainer_requests.is_empty()
            || !self.progress_report_requests.is_empty()
            || !self.progress_report_draft.is_none()
        {
            Err(self)
        } else {
            let (filled_bags_stats, drained_bags_stats): (Vec<(Bag, usize)>, Vec<(Bag, usize)>) =
                self.bags_stats
                    .drain()
                    .map(|(bag, (filled_count, drained_count))| {
                        ((bag.clone(), filled_count), (bag, drained_count))
                    })
                    .unzip();

            info!("{}    Filled Bags Stats", work_io_name);
            let total_filled_chunks_count: usize = filled_bags_stats
                .into_iter()
                .filter(|(_, count)| *count != 0)
                .map(|(bag, count)| {
                    info!("{}        Bag({}):{}", work_io_name, bag.name, count);
                    count
                })
                .sum();
            info!(
                "{}        Total:{}",
                work_io_name, total_filled_chunks_count
            );

            info!("{}    Drained Bags Stats", work_io_name);
            let total_drained_bags_count: usize = drained_bags_stats
                .into_iter()
                .filter(|(_, count)| *count != 0)
                .map(|(bag, count)| {
                    info!("{}        Bag({}):{}", work_io_name, bag.name, count);
                    count
                })
                .sum();
            info!("{}        Total:{}", work_io_name, total_drained_bags_count);

            // Unregesiter `Drainer`s `Receiver` listener from `HurricaneWorkIO`.
            self.bags_drainer
                .drain(..)
                .for_each(|(_, handle)| assert!(ctx.cancel_future(handle)));

            Ok(())
        }
    }

    fn get_filler_sender(&mut self, bag: &Bag, filler_id: usize) -> &mut Sender<Option<Chunk>> {
        self.bags_filler
            .iter_mut()
            .find(|&&mut (ref cur_bag, _)| *cur_bag == *bag)
            .map(|&mut (_, ref mut senders)| &mut senders[filler_id])
            .unwrap()
    }

    fn get_next_hurricane_io_addr_for_bag(&mut self, bag: &Bag) -> Option<SocketAddr> {
        self.bag_hurricane_ios
            .iter()
            .enumerate()
            .find(|(_, (b, _))| *b == *bag)
            .map(|(idx, (_, _))| idx)
            .and_then(|idx| self.bag_hurricane_ios_cycle.get_mut(idx).unwrap().next())
    }

    fn remove_hurricane_io_addr_for_bag(&mut self, addr: &SocketAddr, bag: &Bag) -> Option<()> {
        self.bag_hurricane_ios
            .iter_mut()
            .enumerate()
            .find(|(_, (b, _))| *b == *bag)
            .map(|(bag_idx, (_, v))| {
                if let Some(addr_idx) = v
                    .iter()
                    .enumerate()
                    .find(|(_, addr_in_v)| **addr_in_v == *addr)
                    .map(|(addr_idx, _)| addr_idx)
                {
                    // O(1) deletion.
                    let deleted_addr = v.swap_remove(addr_idx);
                    assert_eq!(deleted_addr, *addr);
                }

                (bag_idx, v.clone())
            })
            .and_then(|(idx, v)| {
                if v.is_empty() {
                    self.bag_hurricane_ios_cycle[idx] = v.into_iter().cycle();
                    None
                } else {
                    self.bag_hurricane_ios_cycle[idx] = v.into_iter().cycle();
                    Some(())
                }
            })
    }

    /// `(Bytes Done, Bytes Total)`
    fn try_generate_progress_report(&mut self, task_id: &TaskId) -> Option<(usize, usize)> {
        assert_eq!(
            self.task_id, *task_id,
            "Task {} is no longer available.",
            *task_id
        );

        assert!(
            self.progress_report_draft.is_some(),
            "Currently no progress report under construction!"
        );

        if self.progress_report_requests.is_empty() {
            Some(
                self.progress_report_draft
                    .take()
                    .unwrap()
                    .into_iter()
                    .fold((0, 0), |acc, x| (acc.0 + x.1, acc.1 + x.2)),
            )
        } else {
            // The progress report query request is still in progress,
            // not ready yet.
            None
        }
    }

    /// `(Bytes Done, Bytes Total)`
    fn receive_partial_progress_report(
        &mut self,
        task_id: &TaskId,
        fingerprint: FingerPrint,
        bag: Bag,
        hurricane_io_addr: SocketAddr,
        partial_bytes_done: usize,
        partial_bytes_total: usize,
    ) -> Option<(usize, usize)> {
        assert_eq!(
            self.task_id, *task_id,
            "Task {} is no longer available.",
            *task_id
        );

        assert!(
            self.progress_report_draft.is_some(),
            "Currently no progress report under construction!"
        );

        // Remove the request from book keeping.
        let v = self.progress_report_requests.remove(&fingerprint);
        assert_eq!(v, Some((bag.clone(), hurricane_io_addr)));

        // Write partial progress into draft.
        let (bytes_done, bytes_total) = self
            .progress_report_draft
            .as_mut()
            .and_then(|progress_report_draft| {
                progress_report_draft
                    .iter_mut()
                    .find(|(b, _, _)| *b == bag)
                    .map(|(_, bytes_done, bytes_total)| (bytes_done, bytes_total))
            })
            .unwrap();
        *bytes_done += partial_bytes_done;
        *bytes_total += partial_bytes_total;

        // Try generate the full progress report.
        self.try_generate_progress_report(task_id)
    }

    fn request_progress_report(
        &mut self,
        task_id: TaskId,
        bag_hurricane_ios: &mut HashMap<
            SocketAddr,
            (
                SocketAddr,
                FramedWrite<WriteHalf<TcpStream>, HurricaneIOCommCodec>,
            ),
        >,
    ) -> bool {
        assert_eq!(
            self.task_id, task_id,
            "Task {} is no longer available.",
            task_id
        );

        if self.progress_report_draft.is_some() {
            // The previous progress report is not done yet.
            // Reject this request.
            false
        } else {
            // Start the progress report process.
            self.progress_report_draft = Some(
                self.blueprint
                    .inputs
                    .iter()
                    .map(|bag| (bag.clone(), 0, 0))
                    .collect(),
            );

            // Send progress query for every input bag to every storage node.
            self.progress_report_requests = bag_hurricane_ios
                .iter_mut()
                .map(|(io_addr, (self_addr, ch))| {
                    let task_id_cloned = task_id.clone();
                    self.blueprint.inputs.iter().map(move |bag| {
                        let fingerprint = generate_random_uuid();
                        ch.write(HurricaneIOComm::Progress(
                            self_addr.clone(),
                            task_id_cloned.clone(),
                            fingerprint.clone(),
                            bag.clone(),
                        ));

                        (fingerprint, (bag.clone(), io_addr.clone()))
                    })
                })
                .flatten()
                .collect();

            true
        }
    }

    fn get_intermediate_bag_mut(&mut self, bag: &Bag) -> Option<&mut (u8, Vec<Chunk>)> {
        self.intermediate_chunks
            .as_mut()
            .and_then(|intermediate_chunks| {
                intermediate_chunks
                    .iter_mut()
                    .find(|(b, _)| *b == *bag)
                    .map(|(_, intermediate_chunks)| intermediate_chunks)
            })
    }

    fn has_intermediate_bag(&self, bag: &Bag) -> bool {
        self.intermediate_chunks
            .as_ref()
            .and_then(|intermediate_chunks| intermediate_chunks.iter().find(|(b, _)| *b == *bag))
            .is_some()
    }
}

/// TCP Network Layer Actor of HurricaneWorkIO for Frontend-Backend communication with storage nodes.
///
/// This layer is named `HurricaneWorkIO` simply because other external actors
/// would deem this actor as the gateway for data transactions on this compute node.
///
/// This layer mainly handles the internal Chunk fetching and publishing of Data Bags at the
/// request of `WorkExecutor` within the node in an asynchronous manner. It then redirects
/// the Chunk requests to the Backend storage nodes.
pub struct HurricaneWorkIO {
    id: usize,
    task_manager_core: Recipient<TaskManagerCoreWorkIOMessage>,
    current_work: Option<WorkSession>,

    /// Channels of `HurricaneIO`s.
    ///
    /// Hurricane IO addr -> (self addr, writer)
    bag_hurricane_ios: HashMap<
        SocketAddr,
        (
            SocketAddr,
            FramedWrite<WriteHalf<TcpStream>, HurricaneIOCommCodec>,
        ),
    >,
}

impl HurricaneWorkIO {
    fn produce_name(id: usize) -> String {
        format!(GET_NAME_FORMAT!(), format!("[{}, None]", id), "HrcWorkIO")
    }

    fn get_name(&self) -> String {
        format!(
            GET_NAME_FORMAT!(),
            format!("[{}, Multi]", self.id),
            "HrcWorkIO"
        )
    }

    pub fn spawn(
        id: usize,
        task_manager_core: Recipient<TaskManagerCoreWorkIOMessage>,
    ) -> Addr<HurricaneWorkIO> {
        Arbiter::builder()
            .name(format!("Arbiter-HurricaneWorkIO-{}", id))
            .stop_system_on_panic(true)
            .start(move |ctx: &mut Context<HurricaneWorkIO>| {
                info!(
                    "{}Connecting to {} HurricaneIO.",
                    Self::produce_name(id),
                    hurricane_backend::conf::get()
                        .hurricane_io_socket_addrs
                        .len()
                );

                let retry_strategy = FixedInterval::from_millis(
                    hurricane_frontend::conf::get().hurricane_work_io_connection_retry_interval,
                )
                .take(hurricane_frontend::conf::get().hurricane_work_io_connection_retry_attempt);

                hurricane_backend::conf::get()
                    .hurricane_io_socket_addrs
                    .iter()
                    .for_each(|addr| {
                        let mut attempt_count = 0;
                        ctx.add_message_stream(
                            Retry::spawn(retry_strategy.clone(), move || {
                                trace!(
                                    "{}Connection attempt {} for {}.",
                                    Self::produce_name(id),
                                    attempt_count,
                                    addr
                                );
                                attempt_count += 1;
                                TcpStream::connect(addr)
                            })
                            .map_err(move |err| {
                                error!(
                                    "{}Connection failed for HurricaneIO: {}!",
                                    Self::produce_name(id),
                                    addr
                                );
                                error!("{}{}.", Self::produce_name(id), err);
                                System::current().stop();
                            })
                            .map(|socket| TcpConnect(socket))
                            .into_stream(),
                        );
                    });

                HurricaneWorkIO {
                    id,
                    task_manager_core,
                    current_work: None,
                    bag_hurricane_ios: HashMap::new(),
                }
            })
    }

    fn send_chunk_to_hurricane_io(&mut self, bag: Bag, chunk: Chunk) {
        let current_work = self.current_work.as_mut().unwrap();

        let fingerprint = generate_random_uuid();

        // Keep the receipt of this transaction.
        let io_addr = current_work
            .get_next_hurricane_io_addr_for_bag(&bag)
            .expect("Drainer should always find a Hurricane IO.");
        let r = current_work
            .bag_drainer_requests
            .insert(fingerprint.clone(), (bag.clone(), io_addr));
        assert!(r.is_none());

        // Send the chunk.
        self.bag_hurricane_ios
            .get_mut(&io_addr)
            .map(|(self_socket_addr, writer)| {
                if hurricane_frontend::conf::get().send_outbags_as_record {
                    // Send record of chunk.
                    writer.write(HurricaneIOComm::FakeDrain(
                        self_socket_addr.clone(),
                        fingerprint,
                        bag.clone(),
                        chunk.get_chunk_size(),
                    ))
                } else {
                    writer.write(HurricaneIOComm::Drain(
                        self_socket_addr.clone(),
                        fingerprint,
                        bag.clone(),
                        chunk,
                    ))
                }
            })
            .unwrap();

        current_work.bags_stats.entry(bag).or_insert((0, 0)).1 += 1;
    }

    fn fetch_chunk_from_hurricane_io(&mut self, bag: Bag, filler_id: usize) {
        let self_name = self.get_name();
        let current_work = self.current_work.as_mut().unwrap();

        match current_work.get_next_hurricane_io_addr_for_bag(&bag) {
            Some(io_addr) => {
                let fingerprint = generate_random_uuid();

                // Keep the receipt of this transaction.
                let r = current_work
                    .bag_filler_requests
                    .insert(fingerprint.clone(), (bag.clone(), filler_id, io_addr));
                assert!(r.is_none());

                // Fetch the chunk.
                self.bag_hurricane_ios
                    .get_mut(&io_addr)
                    .map(|(self_socket_addr, writer)| {
                        writer.write(HurricaneIOComm::Fill(
                            self_socket_addr.clone(),
                            fingerprint,
                            bag,
                        ))
                    })
                    .unwrap();
            }
            None => {
                debug!(
                    "{}Shutting down Bag({}) filler {}.",
                    self_name, bag.name, filler_id
                );
                current_work
                    .get_filler_sender(&bag, filler_id)
                    .try_send(None)
                    .unwrap()
            }
        }
    }

    fn create_new_task_session_default(
        &mut self,
        ctx: &mut Context<Self>,
        task_id: TaskId,
        blueprint: Blueprint,
    ) -> Result<HurricaneWorkIOTaskSession, ()> {
        let num_threads = blueprint.num_threads;
        let hwio_address: Recipient<HurricaneWorkIOChunkMessage> = ctx.address().recipient();

        let (bags_filler, input_bags): (Vec<_>, Vec<_>) = blueprint
            .clone()
            .inputs
            .drain(..)
            .map(|bag| {
                // Create channel for each thread.
                let (senders, receivers): (Vec<_>, Vec<_>) = (0..num_threads)
                    .map(|_| channel::<Option<Chunk>>(0))
                    .unzip();

                (
                    (bag.clone(), senders),
                    InBag::from(bag, receivers, hwio_address.clone()),
                )
            })
            .unzip();

        let (output_bags, bags_drainer): (Vec<_>, Vec<_>) = blueprint
            .clone()
            .outputs
            .drain(..)
            .map(|bag| {
                let (sender, receiver) = channel::<(Bag, Chunk)>(0);

                (
                    OutBag::from(bag.clone(), sender, false),
                    (bag.clone(), receiver),
                )
            })
            .unzip();

        // Register the task session with self.
        let bags_drainer = bags_drainer
            .into_iter()
            .map(|(bag, receiver)| (bag, ctx.add_stream(receiver)))
            .collect();

        // Compute the HurricaneIO cyclics.
        let ios_addr: Vec<_> = self
            .bag_hurricane_ios
            .iter()
            .map(|(addr, _)| addr.clone())
            .collect();
        let bag_ios_addr = blueprint
            .inputs
            .iter()
            .chain(blueprint.outputs.iter())
            .map(|bag| (bag.clone(), ios_addr.clone()))
            .collect();

        debug!("{}Created session for Task <{}>.", self.get_name(), task_id);

        // Doesn't need intermediate chunks for merging.
        self.current_work = Some(WorkSession::new(
            task_id.clone(),
            blueprint,
            bags_filler,
            bags_drainer,
            None,
            bag_ios_addr,
        ));

        // Return the task session
        Ok(HurricaneWorkIOTaskSession {
            task_id,
            input_bags,
            output_bags,
            internal_merge_reduce_bags: None,
        })
    }

    fn create_new_task_session_with_reduce(
        &mut self,
        ctx: &mut Context<Self>,
        task_id: TaskId,
        blueprint: Blueprint,
    ) -> Result<HurricaneWorkIOTaskSession, ()> {
        if blueprint.inputs.len() != 1 || blueprint.outputs.len() != 1 {
            error!("{}Does not support Blueprint that requires MergeType::Reduce but has multiple input/output Bags!", self.get_name());
            panic!(
                "Unsupported Blueprint: Requires MergeType::Reduce but has multiple input/output Bags."
            );
        }
        assert_eq!(blueprint.inputs.len(), 1);
        assert_eq!(blueprint.outputs.len(), 1);

        let num_threads = blueprint.num_threads;
        let hwio_address: Recipient<HurricaneWorkIOChunkMessage> = ctx.address().recipient();

        let (inbag_filler_senders, inbag) = blueprint
            .clone()
            .inputs
            .pop()
            .map(|bag| {
                // Create channel for each thread.
                let (senders, receivers): (Vec<_>, Vec<_>) = (0..num_threads)
                    .map(|_| channel::<Option<Chunk>>(0))
                    .unzip();

                (
                    (bag.clone(), senders),
                    InBag::from(bag, receivers, hwio_address.clone()),
                )
            })
            .unwrap();

        // Send all "supposed" output bag to intermediate bag, which needs to be merged.
        let (intermediate_outbag, intermediate_bag_drainer, intermediate_bag) = blueprint
            .clone()
            .outputs
            .pop()
            .map(|mut bag| {
                bag.name.push_str(&format!(".intermediate{}", self.id));

                let (sender, receiver) = channel::<(Bag, Chunk)>(0);

                (
                    OutBag::from(bag.clone(), sender, false),
                    (bag.clone(), receiver),
                    bag,
                )
            })
            .unwrap();

        // Channels for merging via reduction.
        // Create two fillers and one drainer.
        let (reduction_filler_sender_0, reduction_filler_receiver_0) = channel::<Option<Chunk>>(0);
        let (reduction_filler_sender_1, reduction_filler_receiver_1) = channel::<Option<Chunk>>(0);
        let (reduction_drainer_sender, reduction_drainer_receiver) = channel::<(Bag, Chunk)>(0);
        let reduction_inbag_0 = InBag::from(
            intermediate_bag.clone(),
            vec![reduction_filler_receiver_0],
            ctx.address().recipient(),
        );
        let reduction_inbag_1 = InBag::from(
            intermediate_bag.clone(),
            vec![reduction_filler_receiver_1],
            ctx.address().recipient(),
        );
        let reduction_outbag =
            OutBag::from(intermediate_bag.clone(), reduction_drainer_sender, true);

        // Listen to drainer receiver channel.
        let bags_drainer = vec![
            intermediate_bag_drainer,
            (intermediate_bag.clone(), reduction_drainer_receiver),
        ]
        .into_iter()
        .map(|(bag, receiver)| (bag, ctx.add_stream(receiver)))
        .collect();

        // Store filler sender channel.
        let bags_filler = vec![
            inbag_filler_senders,
            (
                intermediate_bag.clone(),
                vec![reduction_filler_sender_0, reduction_filler_sender_1],
            ),
        ];

        // Compute the HurricaneIO cyclics.
        let ios_addr: Vec<_> = self
            .bag_hurricane_ios
            .iter()
            .map(|(addr, _)| addr.clone())
            .collect();
        let bag_ios_addr = blueprint
            .inputs
            .iter()
            .chain(blueprint.outputs.iter())
            .map(|bag| (bag.clone(), ios_addr.clone()))
            .collect();

        debug!(
            "{}Created session for Task <{}>, MergeType::Reduce.",
            self.get_name(),
            task_id
        );

        self.current_work = Some(WorkSession::new(
            task_id.clone(),
            blueprint,
            bags_filler,
            bags_drainer,
            Some(vec![(intermediate_bag, (1, Vec::new()))]),
            bag_ios_addr,
        ));

        // Return the task session
        Ok(HurricaneWorkIOTaskSession {
            task_id,
            input_bags: vec![inbag],
            output_bags: vec![intermediate_outbag],
            internal_merge_reduce_bags: Some((
                (reduction_inbag_0, reduction_inbag_1),
                reduction_outbag,
            )),
        })
    }
}

impl Actor for HurricaneWorkIO {
    type Context = Context<Self>;

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("{}Bye~", self.get_name());
    }
}

/// HurricaneWorkIO TCP outgoing connection handler.
impl Handler<TcpConnect> for HurricaneWorkIO {
    type Result = ();

    fn handle(&mut self, msg: TcpConnect, ctx: &mut Context<Self>) -> Self::Result {
        let socket = msg.0;

        socket
            .set_nodelay(hurricane_frontend::conf::get().hurricane_work_io_tcp_nodelay)
            .unwrap();
        if let Some(buf_size) = hurricane_frontend::conf::get().hurricane_work_io_so_rcvbuf {
            socket.set_recv_buffer_size(buf_size).unwrap();
        }
        if let Some(buf_size) = hurricane_frontend::conf::get().hurricane_work_io_so_sndbuf {
            socket.set_send_buffer_size(buf_size).unwrap();
        }

        let peer_addr = socket.peer_addr().unwrap();
        let local_addr = socket.local_addr().unwrap();

        info!(
            "{}Connection established with HurricaneIO: {} -> {}.",
            self.get_name(),
            local_addr,
            peer_addr
        );

        let (r, w) = socket.split();

        // Register the r part with stream handler.
        let message_reader = FramedRead::new(r, HurricaneWorkIOCommCodec);
        ctx.add_stream(message_reader);
        // Store the w part.
        let r = self.bag_hurricane_ios.insert(
            peer_addr,
            (local_addr, FramedWrite::new(w, HurricaneIOCommCodec, ctx)),
        );
        assert!(r.is_none());

        // If all HurricaneIOs are connected.
        if self.bag_hurricane_ios.len()
            == hurricane_backend::conf::get()
                .hurricane_io_socket_addrs
                .len()
        {
            debug!("{}All Hurricane IOs are connected, ready.", self.get_name());

            // Inform ready.
            self.task_manager_core
                .try_send(TaskManagerCoreWorkIOMessage::Ready)
                .unwrap();
        }
    }
}

impl StreamHandler<HurricaneWorkIOComm, bincode::Error> for HurricaneWorkIO {
    fn finished(&mut self, ctx: &mut Self::Context) {
        error!("{}HurricaneIO dropped connection!", self.get_name());
        ctx.stop();
    }

    fn handle(&mut self, msg: HurricaneWorkIOComm, _ctx: &mut Context<Self>) {
        match msg {
            HurricaneWorkIOComm::AckDrain(sender, fingerprint, bag) => {
                let current_work = self.current_work.as_mut().unwrap();
                let (expected_bag, hurricane_io_addr) = current_work
                    .bag_drainer_requests
                    .remove(&fingerprint)
                    .unwrap();
                assert_eq!(expected_bag, bag);
                assert_eq!(sender, hurricane_io_addr);
            }
            HurricaneWorkIOComm::ProgressReport(
                sender,
                task_id,
                fingerprint,
                bag,
                bytes_done,
                bytes_total,
            ) => {
                let current_work = self.current_work.as_mut().unwrap();
                if let Some((bytes_done, bytes_total)) = current_work
                    .receive_partial_progress_report(
                        &task_id,
                        fingerprint,
                        bag,
                        sender,
                        bytes_done,
                        bytes_total,
                    )
                {
                    // If the full progress report for this task is ready. Send it to TaskManagerCore.
                    self.task_manager_core
                        .try_send(TaskManagerCoreWorkIOMessage::ProgressReport(
                            task_id,
                            bytes_done,
                            bytes_total,
                        ))
                        .unwrap();
                }
            }
            HurricaneWorkIOComm::Filled(sender, fingerprint, bag, chunk) => {
                let current_work = self.current_work.as_mut().unwrap();

                // Remove the record.
                let (expected_bag, filler_id, hurricane_io_addr) = current_work
                    .bag_filler_requests
                    .remove(&fingerprint)
                    .unwrap();
                assert_eq!(expected_bag, bag);
                assert_eq!(hurricane_io_addr, sender);

                current_work
                    .bags_stats
                    .entry(bag.clone())
                    .or_insert((0, 0))
                    .0 += 1;

                current_work
                    .get_filler_sender(&bag, filler_id)
                    .try_send(Some(chunk))
                    .unwrap();
            }
            HurricaneWorkIOComm::FakeFilled(sender, fingerprint, bag, size) => {
                let current_work = self.current_work.as_mut().unwrap();

                // Remove the record.
                let (expected_bag, filler_id, hurricane_io_addr) = current_work
                    .bag_filler_requests
                    .remove(&fingerprint)
                    .unwrap();
                assert_eq!(expected_bag, bag);
                assert_eq!(hurricane_io_addr, sender);

                current_work
                    .bags_stats
                    .entry(bag.clone())
                    .or_insert((0, 0))
                    .0 += 1;

                current_work
                    .get_filler_sender(&bag, filler_id)
                    .try_send(Some(ChunkPool::allocate_filled_u8(1, Some(size))))
                    .unwrap();
            }
            HurricaneWorkIOComm::EOF(sender, fingerprint, bag) => {
                let self_name = self.get_name();
                let current_work = self.current_work.as_mut().unwrap();

                trace!(
                    "{}Removed {} from source of bag({}).",
                    self_name,
                    sender,
                    bag.name
                );

                // Remove the record.
                let (expected_bag, filler_id, hurricane_io_addr) = current_work
                    .bag_filler_requests
                    .remove(&fingerprint)
                    .unwrap();
                assert_eq!(expected_bag, bag);
                assert_eq!(hurricane_io_addr, sender);

                // Try another Hurricane IO.
                match current_work.remove_hurricane_io_addr_for_bag(&sender, &bag) {
                    None => {
                        debug!(
                            "{}Shutting down Bag({}) filler {}.",
                            self_name, bag.name, filler_id
                        );
                        current_work
                            .get_filler_sender(&bag, filler_id)
                            .try_send(None)
                            .or_else(|e| if e.is_disconnected() { Ok(()) } else { Err(e) })
                            .expect("Unexpected filler error!")
                    }
                    Some(_) => self.fetch_chunk_from_hurricane_io(bag, filler_id),
                };
            }
        }
    }
}

impl WriteHandler<bincode::Error> for HurricaneWorkIO {}

impl StreamHandler<(Bag, Chunk), ()> for HurricaneWorkIO {
    fn finished(&mut self, _ctx: &mut Self::Context) {
        // Do not terminate current actor.
    }

    fn handle(&mut self, msg: (Bag, Chunk), ctx: &mut Context<Self>) {
        let current_work = self.current_work.as_mut().unwrap();
        let (bag, chunk) = msg;

        // Check whether this bag belongs to intermediate bag.
        // If so, send this chunk to intermediate bag.
        if let Some((_, intermediate_chunks)) = current_work.get_intermediate_bag_mut(&bag) {
            // Store to intermediate bag anyway.
            intermediate_chunks.push(chunk);

            if current_work.in_reduction_merge_stage {
                ctx.notify(LocalMessages::IntermediateBagTerminalReady(bag));
            }

            return;
        }

        // Send to storage node.
        self.send_chunk_to_hurricane_io(bag, chunk);
    }
}

impl Handler<LocalMessages> for HurricaneWorkIO {
    type Result = ();
    fn handle(&mut self, msg: LocalMessages, _ctx: &mut Self::Context) -> Self::Result {
        match msg {
            LocalMessages::IntermediateBagTerminalReady(bag) => {
                assert!(self.current_work.as_ref().unwrap().in_reduction_merge_stage);
                assert!(
                    self.current_work.as_ref().unwrap().blueprint.merge_type == MergeType::Reduce
                );

                let (terminal_ready_count, intermediate_chunks) = self
                    .current_work
                    .as_mut()
                    .unwrap()
                    .get_intermediate_bag_mut(&bag)
                    .unwrap();

                *terminal_ready_count += 1;
                assert!(*terminal_ready_count <= 3);

                if *terminal_ready_count == 3 {
                    // Reset terminal ready count.
                    *terminal_ready_count = 0;

                    // All terminals are synchronized, ready to send to filler.
                    assert!(!intermediate_chunks.is_empty());
                    if intermediate_chunks.len() == 1 {
                        // Reduction merge completed.
                        let final_chunk = intermediate_chunks.pop().unwrap();
                        let current_work = self.current_work.as_mut().unwrap();

                        let original_output_bag =
                            current_work.blueprint.outputs.first().cloned().unwrap();

                        current_work
                            .get_filler_sender(&bag, 0)
                            .try_send(None)
                            .unwrap();
                        current_work
                            .get_filler_sender(&bag, 1)
                            .try_send(None)
                            .unwrap();

                        // Send to storage node.
                        self.send_chunk_to_hurricane_io(original_output_bag, final_chunk);
                    } else {
                        assert!(intermediate_chunks.len() >= 2);
                        // Send to fillers.
                        let chunk_0 = intermediate_chunks.pop().unwrap();
                        let chunk_1 = intermediate_chunks.pop().unwrap();

                        let current_work = self.current_work.as_mut().unwrap();
                        current_work
                            .get_filler_sender(&bag, 0)
                            .try_send(Some(chunk_0))
                            .unwrap();
                        current_work
                            .get_filler_sender(&bag, 1)
                            .try_send(Some(chunk_1))
                            .unwrap();
                    }
                }
            }
        }
    }
}

impl Handler<HurricaneWorkIORequestNewTaskSession> for HurricaneWorkIO {
    type Result = Result<HurricaneWorkIOTaskSession, ()>;

    fn handle(
        &mut self,
        msg: HurricaneWorkIORequestNewTaskSession,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        assert!(
            self.current_work.is_none(),
            "Old task session is not closed yet!"
        );

        let task_id = msg.0;
        let blueprint = msg.1;

        match blueprint.merge_type {
            MergeType::Append | MergeType::Nonclonable => {
                self.create_new_task_session_default(ctx, task_id, blueprint)
            }
            MergeType::Reduce => self.create_new_task_session_with_reduce(ctx, task_id, blueprint),
        }
    }
}

impl Handler<HurricaneWorkIORequestCloseTaskSession> for HurricaneWorkIO {
    type Result = ();

    fn handle(
        &mut self,
        msg: HurricaneWorkIORequestCloseTaskSession,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        let current_work = self.current_work.take().unwrap();

        // Current work is not guaranteed to be closed successfully,
        // if there are unresponed request. Need mechanism for retry.
        if let Err(current_work) = current_work.close(&msg.0, &self.get_name(), ctx) {
            // Not closed. Put current_work back.
            self.current_work = Some(current_work);

            debug!(
                "{}Failed to close Task session <{}>. Retry.",
                self.get_name(),
                msg.0
            );

            // Retry later.
            ctx.notify_later(msg, Duration::from_millis(20));
        } else {
            // Closed successfully. Notify TaskManagerCore.
            debug!(
                "{}Successfully closed Task session <{}>.",
                self.get_name(),
                msg.0
            );

            self.task_manager_core
                .try_send(TaskManagerCoreWorkIOMessage::TaskSessionClosed(msg.0))
                .unwrap();
        }
    }
}

impl Handler<HurricaneWorkIORequestProgressUpdate> for HurricaneWorkIO {
    type Result = ();

    fn handle(
        &mut self,
        msg: HurricaneWorkIORequestProgressUpdate,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        // Check for progress update only if `HurricaneWorkIO` still has the session
        // on this task.
        if let Some(current_work) = self.current_work.as_mut() {
            if msg.0 == current_work.task_id {
                if !current_work.request_progress_report(msg.0, &mut self.bag_hurricane_ios) {
                    debug!(
                        "{}Ignored progress request. Previous progress is still on air.",
                        self.get_name()
                    )
                } else {
                    trace!("{}Sent progress request To HurricaneIO.", self.get_name())
                }
            }
        }
    }
}

impl Handler<HurricaneWorkIOChunkMessage> for HurricaneWorkIO {
    type Result = ();

    fn handle(
        &mut self,
        msg: HurricaneWorkIOChunkMessage,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        match msg {
            HurricaneWorkIOChunkMessage::FillerGetChunk(filler_id, bag) => {
                let current_work = self.current_work.as_mut().unwrap();

                // Check whether this bag belongs to intermediate bag.
                if current_work.has_intermediate_bag(&bag) {
                    // A request for intermediate chunk has recieved,
                    // this has to mean we are in internal reduction merge stage.
                    current_work.in_reduction_merge_stage = true;
                    ctx.notify(LocalMessages::IntermediateBagTerminalReady(bag));
                    return;
                }

                // Fetch chunk from `HurricaneIO`.
                self.fetch_chunk_from_hurricane_io(bag, filler_id);
            }
        }
    }
}

impl Handler<PoisonPill> for HurricaneWorkIO {
    type Result = ();

    fn handle(&mut self, _msg: PoisonPill, ctx: &mut Self::Context) -> Self::Result {
        debug!("{}Shutting down...", self.get_name());
        ctx.stop();
    }
}

#[derive(Message)]
enum LocalMessages {
    IntermediateBagTerminalReady(Bag),
}

/// `Message` for `HurricaneWorkIO` to start a new task session.
pub struct HurricaneWorkIORequestNewTaskSession(pub TaskId, pub Blueprint);

impl Message for HurricaneWorkIORequestNewTaskSession {
    type Result = Result<HurricaneWorkIOTaskSession, ()>;
}

/// The receipt for successfully-created task session.
pub struct HurricaneWorkIOTaskSession {
    pub task_id: TaskId,
    pub input_bags: Vec<InBag>,
    pub output_bags: Vec<OutBag>,
    pub internal_merge_reduce_bags: Option<((InBag, InBag), OutBag)>,
}

/// `Message` for `HurricaneWorkIO` to close the previous task session.
#[derive(Message)]
pub struct HurricaneWorkIORequestCloseTaskSession(pub TaskId);

/// `Message` for `HurricaneWorkIO` to report current progress.
#[derive(Message)]
pub struct HurricaneWorkIORequestProgressUpdate(pub TaskId);

#[derive(Message)]
/// `Message` for `Filler` and `Drainer` in the data pipe to use.
pub enum HurricaneWorkIOChunkMessage {
    FillerGetChunk(usize, Bag),
}
