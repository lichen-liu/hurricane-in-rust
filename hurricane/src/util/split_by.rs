use core::fmt::{Debug, Formatter, Result as FmtResult};
use core::mem::replace;
use futures::prelude::*;

/// Sink that clones incoming items, filters and forwards them to multiple sinks at the same time.
///
/// Backpressure from any downstream sink propagates up, which means that this sink
/// can only process items as fast as its _slowest_ downstream sink.
pub struct SplitBy<A: Sink, F> {
    sinks: Vec<Downstream<A>>,
    f: F,
}

impl<A: Sink, F> SplitBy<A, F> {
    /// Consumes this combinator, returning the underlying sinks.
    ///
    /// Note that this may discard intermediate state of this combinator,
    /// so care should be taken to avoid losing resources when this is called.
    pub fn into_inner(self) -> Vec<A> {
        self.sinks.into_iter().map(|ds| ds.sink).collect()
    }
}

impl<A: Sink + Debug, F> Debug for SplitBy<A, F>
where
    A::SinkItem: Debug,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_struct("SplitBy")
            .field("sinks", &self.sinks)
            .finish()
    }
}

pub fn new<A, F>(sinks: Vec<A>, f: F) -> SplitBy<A, F>
where
    A: Sink,
    F: FnMut(A::SinkItem) -> usize,
{
    SplitBy {
        sinks: sinks.into_iter().map(|s| Downstream::new(s)).collect(),
        f,
    }
}

impl<A, F> Sink for SplitBy<A, F>
where
    A: Sink,
    F: FnMut(A::SinkItem) -> usize,
    A::SinkItem: Clone,
{
    type SinkItem = A::SinkItem;
    type SinkError = A::SinkError;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let which_sink = (self.f)(item.clone());
        assert!(which_sink < self.sinks.len());

        // Attempt to complete processing any outstanding requests.
        self.sinks[which_sink].keep_flushing()?;

        // Only if the appropriate downstream sinks are ready, start sending the next item.
        if self.sinks[which_sink].is_ready() {
            self.sinks[which_sink].state = self.sinks[which_sink].sink.start_send(item)?;
            Ok(AsyncSink::Ready)
        } else {
            Ok(AsyncSink::NotReady(item))
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        let mut all_ready = true;
        for ds in self.sinks.iter_mut() {
            all_ready = all_ready && ds.poll_complete()?.is_ready();

            if !all_ready {
                break;
            }
        }

        // Only if all downstream sinks are ready, signal readiness.
        if all_ready {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        let mut all_ready = true;
        for ds in self.sinks.iter_mut() {
            all_ready = all_ready && ds.close()?.is_ready();

            if !all_ready {
                break;
            }
        }

        // Only if all downstream sinks are ready, signal readiness.
        if all_ready {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}

/// Helper downstream `Sink`.
///
/// Implementation:
/// https://tokio-rs.github.io/tokio/src/futures/sink/fanout.rs.html
#[derive(Debug)]
struct Downstream<S: Sink> {
    sink: S,
    state: AsyncSink<S::SinkItem>,
}

impl<S: Sink> Downstream<S> {
    fn new(sink: S) -> Self {
        Downstream {
            sink: sink,
            state: AsyncSink::Ready,
        }
    }

    fn is_ready(&self) -> bool {
        self.state.is_ready()
    }

    fn keep_flushing(&mut self) -> Result<(), S::SinkError> {
        if let AsyncSink::NotReady(item) = replace(&mut self.state, AsyncSink::Ready) {
            self.state = self.sink.start_send(item)?;
        }
        Ok(())
    }

    fn poll_complete(&mut self) -> Poll<(), S::SinkError> {
        self.keep_flushing()?;
        let asynch = self.sink.poll_complete()?;
        // Only if all values have been sent _and_ the underlying
        // sink is completely flushed, signal readiness.
        if self.state.is_ready() && asynch.is_ready() {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }

    fn close(&mut self) -> Poll<(), S::SinkError> {
        self.keep_flushing()?;
        // If all items have been flushed, initiate close.
        if self.state.is_ready() {
            self.sink.close()
        } else {
            Ok(Async::NotReady)
        }
    }
}
