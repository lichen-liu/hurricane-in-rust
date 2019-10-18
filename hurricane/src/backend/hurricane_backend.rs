pub mod conf {
    use crate::util::config::BackendConfig;

    static mut BACKEND_CONFIG: Option<BackendConfig> = None;

    /// This function must be used in caution. Should only be used
    /// during initialization.
    pub fn set(config: BackendConfig) {
        unsafe { BACKEND_CONFIG = Some(config) }
    }

    /// This function must be used in caution.
    pub fn get_mut() -> &'static mut BackendConfig {
        unsafe {
            BACKEND_CONFIG
                .as_mut()
                .expect("BACKEND_CONFIG is not initialized yet!")
        }
    }

    pub fn get() -> &'static BackendConfig {
        unsafe {
            BACKEND_CONFIG
                .as_ref()
                .expect("BACKEND_CONFIG is not initialized yet!")
        }
    }
}

use super::hurricane_io::HurricaneIO;
use actix::prelude::*;
use log::info;
use std::time::Duration;

pub struct HurricaneBackend {
    hurricane_io: Addr<HurricaneIO>,
}

impl HurricaneBackend {
    /// The driver function to start the `HurricaneBackend`.
    ///
    /// ## Arguments
    /// `id` - the current id for this `HurricaneBackend`.
    /// The `id` is in HurricaneBackend space.
    pub fn spawn(id: usize) -> Addr<HurricaneBackend> {
        info!("======================================================================");
        info!("====================== Hurricane Backend Alive =======================");
        info!("======================================================================");

        let hurricane_io = HurricaneIO::spawn(id);

        HurricaneBackend { hurricane_io }.start()
    }
}

impl Actor for HurricaneBackend {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        // Initiate a timed event to check whether the current frontend has stopped.
        // If so, shut down gracefully.
        ctx.run_interval(
            Duration::from_secs(conf::get().backend_heart_beat_interval),
            |actor, ctx| {
                if !actor.hurricane_io.connected() {
                    ctx.stop();
                }
            },
        );
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        System::current().stop();
        info!("======================================================================");
        info!("====================== Hurricane Backend Done! =======================");
        info!("======================================================================");
    }
}
