use super::chunk::*;
use crate::common::data_serializer::*;
use crate::frontend::hurricane_graph::*;
use crate::frontend::hurricane_work_io::*;
use crate::util::split_by;
use crate::util::split_by::SplitBy;
use actix::prelude::*;
use futures::prelude::*;
use futures::sink::SendAll;
use futures::sync::mpsc::*;
use serde::{Deserialize, Serialize};
use std::io;
use std::marker::PhantomData;

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, Eq, Hash)]
pub struct Bag {
    pub name: String,
}

impl Default for Bag {
    fn default() -> Bag {
        Bag {
            name: String::from("default-bag"),
        }
    }
}

impl Bag {
    pub fn new() -> Bag {
        Default::default()
    }

    pub fn from<T: ToString>(name: T) -> Bag {
        Bag {
            name: name.to_string(),
        }
    }
}

/// Adapter for `Bag`, connected channel that is able to pull `Chunk`s from the real input `Bag`.
///
/// The layer that maps the abstraction of `Bag` to real data.
pub struct InBag {
    pub bag: Bag,
    pub filler_receivers: Vec<Receiver<Option<Chunk>>>,
    pub hwio_address: Recipient<HurricaneWorkIOChunkMessage>,
}

impl InBag {
    pub fn from(
        bag: Bag,
        filler_receivers: Vec<Receiver<Option<Chunk>>>,
        hwio_address: Recipient<HurricaneWorkIOChunkMessage>,
    ) -> InBag {
        InBag {
            bag,
            filler_receivers,
            hwio_address,
        }
    }

    pub fn with(self, another: InBag) -> InBags {
        InBags::from(vec![self, another])
    }

    pub fn into_fillers<F>(self) -> Vec<Filler<F>>
    where
        F: Format + Send + 'static,
        F::Item: Send,
    {
        let InBag {
            bag,
            filler_receivers,
            hwio_address,
        } = self;

        filler_receivers
            .into_iter()
            .enumerate()
            .map(|(filler_id, filler_receiver)| {
                Filler::<F>::from(
                    filler_id,
                    bag.clone(),
                    filler_receiver,
                    hwio_address.clone(),
                )
            })
            .collect()
    }

    /// Consume the current `Bag` and turn it into an asynchronous `Stream`.
    ///
    /// This function genereates a vector of `Stream` or data pipes, its number is
    /// depending on the number of threads of execution.
    pub fn into_pipes<F>(self) -> Vec<Box<dyn Stream<Item = F::Item, Error = io::Error> + Send>>
    where
        F: Format + Send + 'static,
        F::Item: Send,
    {
        self.into_fillers::<F>()
            .into_iter()
            .map(|filler| {
                Box::new(
                    filler
                        .map(|it| futures::stream::iter_ok::<_, io::Error>(it))
                        .flatten(),
                ) as Box<dyn Stream<Item = F::Item, Error = io::Error> + Send>
            })
            .collect()
    }

    /// Consume the current `Bag`, apply the provided asynchronous `Stream` adapter `A`.
    ///
    /// The adapter `A` is applied on all *identical* `Stream`s or data pipes, that all
    /// drain from the current `InBag`.
    ///
    /// The vector of `Stream` or data pipes that the function generates are for multithreading purpose. The
    /// size is depending on the number of threads of execution.
    pub fn execute<F, A, R>(self, mut f: A) -> Vec<R>
    where
        F: Format + Send + 'static,
        F::Item: Send,
        A: FnMut(Box<dyn Stream<Item = F::Item, Error = io::Error> + Send>) -> R,
        R: Stream<Error = io::Error> + Send,
    {
        self.into_pipes::<F>()
            .into_iter()
            .map(|stream_in| f(stream_in))
            .collect()
    }
}

/// Combinator for `InBag`, combines multiple `InBag`s together.
pub struct InBags {
    inbags: Vec<InBag>,
}

impl InBags {
    pub fn new() -> InBags {
        InBags { inbags: Vec::new() }
    }

    pub fn from(inbags: Vec<InBag>) -> InBags {
        InBags { inbags }
    }

    pub fn with(mut self, inbag: InBag) -> InBags {
        self.inbags.push(inbag);
        self
    }

    /// Consume the current `InBags`, and apply reduction policy `A` to 2 `InBag`s.
    ///
    /// `A` - takes in a `Stream<Item = (F::Item, F::Item), Error = io::Error>`, and produce
    /// a `Stream` with the reduction value.
    pub fn reduce<F, A, R>(mut self, mut f: A) -> Vec<R>
    where
        F: Format + Send + 'static,
        F::Item: Send,
        A: FnMut(Box<dyn Stream<Item = (F::Item, F::Item), Error = io::Error> + Send>) -> R,
        R: Stream<Error = io::Error> + Send,
    {
        // Reduction only supports 2 inbags.
        assert_eq!(self.inbags.len(), 2);

        // Each inbag should only have 1 datapipe.
        let mut data_pipe_0 = self.inbags.pop().unwrap().into_pipes::<F>();
        assert_eq!(data_pipe_0.len(), 1);
        let mut data_pipe_1 = self.inbags.pop().unwrap().into_pipes::<F>();
        assert_eq!(data_pipe_1.len(), 1);
        let stream_0 = data_pipe_0.pop().unwrap();
        let stream_1 = data_pipe_1.pop().unwrap();

        let zipped_stream = Box::new(stream_0.zip(stream_1))
            as Box<dyn Stream<Item = (F::Item, F::Item), Error = io::Error> + Send>;
        vec![f(zipped_stream)]
    }
}

/// Adapter for `Bag`, connected channel that is able to send `Chunk`s to the real output `Bag`.
///
/// The layer that maps the abstraction of `Bag` to real data.
pub struct OutBag {
    pub bag: Bag,
    pub drainer_sender: Sender<(Bag, Chunk)>,
    pub dont_pack: bool,
}

impl OutBag {
    pub fn from(bag: Bag, drainer_sender: Sender<(Bag, Chunk)>, dont_pack: bool) -> OutBag {
        OutBag {
            bag,
            drainer_sender,
            dont_pack,
        }
    }

    pub fn get_drainer<F>(&self, drainer_id: usize) -> Drainer<F>
    where
        F: Format + 'static,
        F::Item: Send + Clone,
    {
        Drainer::<F>::from(
            drainer_id,
            self.bag.clone(),
            self.drainer_sender.clone(),
            self.dont_pack,
        )
    }
}

/// Trait to send `Stream` into a `Drainer` provided by the argument `OutBag`.
pub trait IntoBag<F>
where
    F: Format + 'static,
    F::Item: Send + Clone,
{
    type ResultItem;

    fn into_bag(self, pd: PhantomData<F>, bag: OutBag) -> Self::ResultItem;
}

/// `IntoBag` impl for `Vec` of `Stream`.
impl<S, F> IntoBag<F> for Vec<S>
where
    F: Format + 'static,
    F::Item: Send + Clone,
    S: Stream<Item = F::Item> + Send,
    io::Error: From<<S as Stream>::Error>,
{
    type ResultItem = Vec<SendAll<Drainer<F>, S>>;

    fn into_bag(self, _pd: PhantomData<F>, bag: OutBag) -> Self::ResultItem {
        self.into_iter()
            .enumerate()
            .map(|(drainer_id, st)| bag.get_drainer::<F>(drainer_id).send_all(st))
            .collect()
    }
}

/// Trait to send `Stream` into one `Drainer` of multiple argument `OutBag`,
/// as determined by the predicate function `f`.
pub trait IntoBags<P, F>
where
    F: Format + 'static,
    F::Item: Send + Clone,
{
    type ResultItem;

    fn into_bags(self, pd: PhantomData<F>, bags: Vec<OutBag>, f: P) -> Self::ResultItem;
}

/// `IntoBags` impl for `Vec` of `Stream`.
impl<S, P, F> IntoBags<P, F> for Vec<S>
where
    F: Format + 'static,
    F::Item: Send + Clone,
    S: Stream<Item = F::Item> + Send,
    P: FnMut(S::Item) -> usize + Clone + Send,
    io::Error: From<<S as Stream>::Error>,
{
    type ResultItem = Vec<SendAll<SplitBy<Drainer<F>, P>, S>>;

    fn into_bags(self, _pd: PhantomData<F>, bags: Vec<OutBag>, f: P) -> Self::ResultItem {
        self.into_iter()
            .enumerate()
            .map(|(drainer_id, st)| {
                // For every steam, create a new SplitBy sink
                split_by::new(
                    // Get drainer for every OutBag.
                    bags.iter()
                        .map(|b| b.get_drainer::<F>(drainer_id))
                        .collect(),
                    f.clone(),
                )
                .send_all(st)
            })
            .collect()
    }
}
