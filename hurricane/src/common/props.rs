use futures::prelude::*;
use std::convert::Into;
use std::io;

pub struct Props {
    fs: Vec<Box<dyn Future<Item = (), Error = io::Error> + Send>>,
}

impl Props {
    pub fn new<F>(fs: Vec<F>) -> Props
    where
        F: Future + Send + 'static,
        F::Error: Into<io::Error>,
    {
        Props {
            fs: fs
                .into_iter()
                .map(|f| {
                    Box::new(f.map(|_| ()).map_err(|e| e.into()))
                        as Box<dyn Future<Item = (), Error = io::Error> + Send>
                })
                .collect(),
        }
    }

    /// Fake `Props`, for test only.
    pub fn fake() -> Props {
        Props { fs: Vec::new() }
    }

    pub fn into_final_props(self) -> Vec<FinalProps> {
        self.fs.into_iter().map(|dp| FinalProps(dp)).collect()
    }

    pub fn into_final_props_iter(self) -> impl Iterator<Item = FinalProps> {
        self.fs.into_iter().map(|dp| FinalProps(dp))
    }
}

pub struct FinalProps(Box<dyn Future<Item = (), Error = io::Error> + Send>);

impl FinalProps {
    pub fn take(self) -> Box<dyn Future<Item = (), Error = io::Error> + Send> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::bag::*;
    use crate::common::chunk::*;
    use crate::common::chunk_pool::*;
    use crate::common::primitive_serializer::*;
    use crate::frontend::bag_io_mock::*;
    use actix::prelude::*;
    use futures::sync::mpsc::*;
    use std::marker::PhantomData;
    use std::thread;

    #[test]
    fn single_pipe_single_fanin_single_fanout_props() {
        let num_chunks = 100;
        let sys = System::new("Hurricane-UnitTest");

        // create channel for delivering chunks to filler
        let (filler_sender, filler_receiver) = channel::<Option<Chunk>>(0);
        // create channel for delivering chunks to drainer
        let (drainer_sender, drainer_receiver) = channel::<(Bag, Chunk)>(0);

        // generate input chunks for filler
        let chunk = ChunkPool::allocate();
        let num_i64 = chunk.get_chunk_size() / std::mem::size_of::<i64>();
        let mut pu = chunk.into_pusher::<I64Format>();
        (0..num_i64).for_each(|i| assert_eq!(Some(()), pu.put(i as i64)));
        let chunk = Chunk::from_pusher(pu);
        let chunks: Vec<Chunk> = (0..num_chunks).map(|_| chunk.clone()).collect();

        // start the data provider actor
        let data_provider = DataProvider::spawn(
            (false, false),
            Some(num_chunks),
            vec![filler_sender],
            vec![drainer_receiver],
            chunks,
            DataProviderChecker::None,
        );

        let in_bag = InBag::from(Bag::new(), vec![filler_receiver], data_provider.recipient());
        let out_bag = OutBag::from(Bag::new(), drainer_sender, false);

        let props = Props::new(
            in_bag
                .execute::<I64Format, _, _>(|st| st.map(|x| x * 2))
                .into_bag(PhantomData::<I64Format>, out_bag),
        );

        props.into_final_props_iter().for_each(|p| {
            let task = p.take().map_err(|_| ()).map(|_| ());

            Arbiter::spawn(task);
        });

        sys.run();
    }

    #[test]
    fn multi_pipe_single_fanin_single_fanout_props() {
        // DataProvider is running on a separator thread! Will block if spawned in main thread!
        // Plus worker threads.

        // num worker threads
        for i in 1..9 {
            let num_chunks = 100;
            let num_threads = i;
            println!("\nWorkers runing on {} threads.", num_threads);
            let sys = System::new("Hurricane-UnitTest");

            // create channel for delivering chunks to filler
            let (filler_senders, filler_receivers): (Vec<_>, Vec<_>) = (0..num_threads)
                .map(|_| channel::<Option<Chunk>>(0))
                .unzip();

            // create channel for delivering chunks to drainer
            let (drainer_sender, drainer_receiver) = channel::<(Bag, Chunk)>(0);

            // generate input chunks for filler
            let chunk = ChunkPool::allocate();
            let num_i64 = chunk.get_chunk_size() / std::mem::size_of::<i64>();
            let mut pu = chunk.into_pusher::<I64Format>();
            (0..num_i64).for_each(|i| assert_eq!(Some(()), pu.put(i as i64)));
            let chunk = Chunk::from_pusher(pu);
            let chunks: Vec<Chunk> = (0..num_chunks).map(|_| chunk.clone()).collect();

            // start the data provider actor
            let data_provider = DataProvider::spawn(
                (false, false),
                Some(num_chunks),
                filler_senders,
                vec![drainer_receiver],
                chunks,
                DataProviderChecker::None,
            );

            // Do the job
            let in_bag = InBag::from(Bag::new(), filler_receivers, data_provider.recipient());

            let out_bag = OutBag::from(Bag::new(), drainer_sender.clone(), false);

            let props = Props::new(
                in_bag
                    .execute::<I64Format, _, _>(|st| st.map(|x| x * 2))
                    .into_bag(PhantomData::<I64Format>, out_bag),
            );

            let threads: Vec<_> = props
                .into_final_props_iter()
                .map(|p| {
                    let task = p.take().map_err(|_| ());

                    thread::spawn(move || {
                        tokio::run(task);
                    })
                })
                .collect();

            sys.run();

            threads
                .into_iter()
                .for_each(|thread_handle| thread_handle.join().unwrap());
        }
    }
}
