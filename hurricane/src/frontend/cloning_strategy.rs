pub trait CloningStrategy {
    /// A predicate function to decide whether to clone the current task or not,
    /// according to the argument criteria.
    ///
    /// `cpu_load` - In %.
    ///
    /// `network_downstream_load` - In Bytes.
    ///
    /// `network_upstream_load` - In Bytes.
    ///
    /// `progress` - In % of work done.
    fn offer(
        &mut self,
        cpu_load: f32,
        network_downstream_load: u64,
        network_upstream_load: u64,
        progress: f32,
    ) -> bool;
}

pub struct ProgressBasedCloningStrategy {
    max_progress: f32,
}

impl ProgressBasedCloningStrategy {
    pub fn new(max_progress: f32) -> ProgressBasedCloningStrategy {
        ProgressBasedCloningStrategy { max_progress }
    }
}

impl CloningStrategy for ProgressBasedCloningStrategy {
    fn offer(
        &mut self,
        _cpu_load: f32,
        _network_downstream_load: u64,
        _network_upstream_load: u64,
        progress: f32,
    ) -> bool {
        progress <= self.max_progress
    }
}
