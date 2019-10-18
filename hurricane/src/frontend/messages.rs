//! Actor messages used within `Frontend` mod.

use actix::prelude::*;

#[derive(Message)]
/// (Identification, Result)
/// `I` - Identification type.
/// `T` - `Result::Ok` type.
/// `E` - `Result::Err` type.
///
/// # TODO
/// Remove some generic type parameters.
pub struct Done<I, T, E>(pub I, pub Result<T, E>);

#[derive(Message)]
pub struct Start;
