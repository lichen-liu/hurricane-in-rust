use super::types::TaskId;
use std::collections::HashSet;

#[derive(Default)]
pub struct WorkBag {
    pub ready: Vec<TaskId>,
    pub running: HashSet<TaskId>,
    pub done: HashSet<TaskId>,
}
