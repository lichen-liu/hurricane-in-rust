//! This mod contains the struct `DataProvider` for helping
//! unit-testing anything that involves `Filler` and `Drainer`.
//!
//! This mod provides mock object that handles the chunk transmission
//! to `Filler` and from `Drainer`.
use super::hurricane_work_io::*;
use crate::common::bag::*;
use crate::common::chunk::*;
use actix::prelude::*;
use futures::sync::mpsc::*;
use std::collections::{HashMap, VecDeque};

pub enum DataProviderChecker {
    Answer(HashMap<Bag, Vec<Chunk>>),
    CustomerChecker(Box<dyn FnMut(HashMap<Bag, Vec<Chunk>>) + Send>),
    None,
}
pub struct DataProvider {
    debug_flag: bool,
    /// Flag to set whether to print the report or not.
    /// It seems during `cargo test`, it only captures what's printed
    /// in the main thread, however, this DataProvider actor is running
    /// on another thread.
    print_report_flag: bool,
    self_kill_out_chunks: Option<usize>,

    fillers: Vec<Sender<Option<Chunk>>>,
    in_chunks: VecDeque<Chunk>,
    out_chunks: HashMap<Bag, Vec<Chunk>>,

    num_chunks_from_drainer: usize,
    num_chunks_to_fillers: usize,

    out_chunks_checker: DataProviderChecker,
}

impl DataProvider {
    /// ### Arguments
    /// flags - (debug_flag, print_report_flag)
    pub fn spawn(
        flags: (bool, bool),
        self_kill_out_chunks: Option<usize>,

        fillers: Vec<Sender<Option<Chunk>>>,
        drainers: Vec<Receiver<(Bag, Chunk)>>,

        in_chunks: Vec<Chunk>,
        out_chunks_checker: DataProviderChecker,
    ) -> Addr<DataProvider> {
        Arbiter::builder()
            .name("Arbiter-DataProvider")
            .stop_system_on_panic(true)
            .start(move |ctx: &mut Context<Self>| {
                drainers.into_iter().for_each(|receiver| {
                    ctx.add_stream(receiver);
                });

                let (debug_flag, print_report_flag) = flags;
                DataProvider {
                    debug_flag,
                    print_report_flag,
                    self_kill_out_chunks,
                    fillers,
                    in_chunks: in_chunks.into(),
                    out_chunks: HashMap::new(),
                    num_chunks_from_drainer: 0,
                    num_chunks_to_fillers: 0,
                    out_chunks_checker,
                }
            })
    }
}

impl Actor for DataProvider {
    type Context = Context<Self>;

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if self.print_report_flag {
            println!(
                "DataProvider: {} In Chunks to Fillers.",
                self.num_chunks_to_fillers
            );
        }

        let out_chunks_len: usize = self.out_chunks.values().map(|cv| cv.len()).sum();

        match self.out_chunks_checker {
            DataProviderChecker::Answer(ref ans) => assert_eq!(self.out_chunks, *ans),
            DataProviderChecker::CustomerChecker(ref mut checker) => {
                checker(self.out_chunks.clone())
            }
            DataProviderChecker::None => {}
        }

        if self.print_report_flag {
            println!(
                "DataProvider: {} Out Chunks from Drainers checked!",
                out_chunks_len
            );
        }

        System::current().stop();
    }
}

impl StreamHandler<(Bag, Chunk), ()> for DataProvider {
    fn finished(&mut self, _ctx: &mut Self::Context) {
        // do not terminate current actor
    }

    fn handle(&mut self, msg: (Bag, Chunk), ctx: &mut Context<Self>) {
        if self.debug_flag {
            println!("DataProvider: [Chunk] Received of {:?}", msg.0);
        }
        self.out_chunks
            .entry(msg.0)
            .or_insert(Vec::new())
            .push(msg.1);

        self.num_chunks_from_drainer += 1;

        if let Some(n) = self.self_kill_out_chunks {
            if n == self.num_chunks_from_drainer {
                ctx.notify(Stop);
            }
        }
    }
}

impl Handler<Stop> for DataProvider {
    type Result = ();
    fn handle(&mut self, _msg: Stop, ctx: &mut Self::Context) -> Self::Result {
        ctx.stop();
    }
}

impl Handler<HurricaneWorkIOChunkMessage> for DataProvider {
    type Result = ();

    fn handle(
        &mut self,
        msg: HurricaneWorkIOChunkMessage,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        match msg {
            HurricaneWorkIOChunkMessage::FillerGetChunk(id, bag) => {
                if self.debug_flag {
                    println!("DataProvider: [Request] of {:?} from Drainer {}.", bag, id);
                }
                match self.in_chunks.pop_front() {
                    Some(chunk) => {
                        self.num_chunks_to_fillers += 1;
                        self.fillers[id].try_send(Some(chunk)).unwrap()
                    }
                    None => {
                        if self.debug_flag {
                            println!(
                                "DataProvider: [Done] No more Chunks from {:?} for Drainer {}.",
                                bag, id
                            );
                        }
                        self.fillers[id].try_send(None).unwrap()
                    }
                }
            }
        }
    }
}

#[derive(Message)]
pub struct Stop;
