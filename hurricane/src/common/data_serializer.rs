use std::io::Cursor;
use std::iter::Iterator;
use std::ops::Range;

/// Owned `[u8]`.
pub type Buffer = Box<[u8]>;
/// Cursor-wrapped view `&[u8]`.
pub type BufferView<'a> = Cursor<&'a [u8]>;
/// Cursor-wrapped view `&mut [u8]`.
pub type BufferViewMut<'a> = Cursor<&'a mut [u8]>;

/// Trait for querying the endianness.
pub trait Endian {
    fn is_big_endian() -> bool {
        true
    }
}

/// Trait for serializing a particular data type.
pub trait DataSerializer: Endian {
    type Item;

    fn to_binary(buf: &mut BufferViewMut, something: Self::Item) -> Option<()>;

    fn from_binary(buf: &mut BufferView) -> Option<Self::Item>;
}

/// Trait for accessing a particular data type from a buffer.
pub trait Format: DataSerializer {
    type Iter: Iterator<Item = <Self as DataSerializer>::Item> + Send;
    type Pusher: Pusher<Item = <Self as DataSerializer>::Item, InnerContainer = Buffer> + Send;

    fn reads(buf: &mut BufferView) -> Option<Self::Item> {
        Self::from_binary(buf)
    }

    fn writes(buf: &mut BufferViewMut, something: Self::Item) -> Option<()> {
        Self::to_binary(buf, something)
    }

    /// Method to convert `Buffer` into an iterator.
    ///
    /// This method does exactly the same thing as its associated version `into_iter()`.
    fn do_into_iter(&self, buf: Buffer, range: Range<usize>) -> Self::Iter {
        Self::into_iter(buf, range)
    }

    /// Associated function to convert `Buffer` into an iterator.
    ///
    /// This assocated function does exactly the same thing as its method version `do_into_iter()`.
    fn into_iter(buf: Buffer, range: Range<usize>) -> Self::Iter;

    /// Method to convert `Buffer` into a pusher.
    ///
    /// This method does exactly the same thing as its associated version `into_pusher()`.
    fn do_into_pusher(&self, buf: Buffer, range: Range<usize>) -> Self::Pusher {
        Self::into_pusher(buf, range)
    }

    /// Associated function to convert `Buffer` into a pusher.
    ///
    /// This assocated function does exactly the same thing as its method version `do_into_pusher()`.
    fn into_pusher(buf: Buffer, range: Range<usize>) -> Self::Pusher;
}

/// An interface for dealing with pushers.
pub trait Pusher {
    type Item;
    type InnerContainer;

    fn put(&mut self, something: Self::Item) -> Option<()>;

    /// Terminate this pusher and return its current position and inner storage.
    ///
    /// ## Return Value
    /// `current pos` - the next location this pusher would write into if
    /// it has not been terminated. This position should be with respect
    /// to only the range of the inner storage that is intended to be written into.
    ///
    /// `inner storage` - the inner container that this pusher is writing.
    fn terminate(self) -> (usize, Self::InnerContainer);

    /// Terminate this pusher and return its current position and inner storage.
    ///
    /// ## Return Value
    /// `current pos` - the next location this pusher would write into if
    /// it has not been terminated. This position should be with respect
    /// to only the range of the inner storage that is intended to be written into.
    ///
    /// `inner storage` - the inner container that this pusher is writing.
    fn terminate_from_box(self: Box<Self>) -> (usize, Self::InnerContainer);

    /// Terminate this pusher and return its inner storage.
    ///
    /// ## Return Value
    /// `inner storage` - the inner container that this pusher is writing.
    fn into_inner(self) -> Self::InnerContainer
    where
        Self: Sized,
    {
        self.terminate().1
    }

    /// Terminate this pusher and return its inner storage.
    ///
    /// ## Return Value
    /// `inner storage` - the inner container that this pusher is writing.
    fn into_inner_from_box(self: Box<Self>) -> Self::InnerContainer {
        self.terminate_from_box().1
    }
}
