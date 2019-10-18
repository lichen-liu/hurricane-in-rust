//! Actor messages used within all systems.
//! Do not put network-specific messages here!

use super::types::TaskId;
use crate::app::blueprint::Blueprint;
use actix::dev::{MessageResponse, ResponseChannel};
use actix::prelude::*;
use serde::{Deserialize, Serialize};
use tokio_tcp::TcpStream;

/// Message carrying `TaskId` and `Blueprint`.
#[derive(Message, Serialize, Deserialize, Clone)]
pub struct Task(pub Option<(TaskId, Blueprint)>);

impl Task {
    pub fn task_id(&self) -> Option<&TaskId> {
        self.0.as_ref().map(|task| &task.0)
    }

    pub fn blueprint(&self) -> Option<&Blueprint> {
        self.0.as_ref().map(|task| &task.1)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

/// Allow `Task` to be a `Message` Response type.
impl<A, M> MessageResponse<A, M> for Task
where
    A: Actor,
    M: Message<Result = Task>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self);
        }
    }
}

#[derive(Message, Serialize, Deserialize)]
pub struct TaskCompletedAck(pub Result<(), ()>);

/// Allow `TaskCompletedAck` to be a `Message` Response type.
impl<A, M> MessageResponse<A, M> for TaskCompletedAck
where
    A: Actor,
    M: Message<Result = TaskCompletedAck>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self);
        }
    }
}

#[derive(Message, Clone)]
/// Gracefully stop the actor.
pub struct PoisonPill;

#[derive(Message)]
pub struct TcpConnect(pub TcpStream);
