pub mod conf {
    use crate::util::config::FrontendConfig;

    static mut FRONTEND_CONFIG: Option<FrontendConfig> = None;

    /// This function must be used in caution. Should only be used
    /// during initialization.
    pub fn set(config: FrontendConfig) {
        unsafe { FRONTEND_CONFIG = Some(config) }
    }

    /// This function must be used in caution.
    pub fn get_mut() -> &'static mut FrontendConfig {
        unsafe {
            FRONTEND_CONFIG
                .as_mut()
                .expect("FRONTEND_CONFIG is not initialized yet!")
        }
    }

    pub fn get() -> &'static FrontendConfig {
        unsafe {
            FRONTEND_CONFIG
                .as_ref()
                .expect("FRONTEND_CONFIG is not initialized yet!")
        }
    }
}

use super::task_manager::TaskManager;
use crate::app::app_master::AppMaster;
use crate::app::hurricane_application::*;
use actix::prelude::*;
use log::info;
use std::time::Duration;

pub struct HurricaneFrontend<HA>
where
    HA: HurricaneApplication + 'static,
{
    app_master: Option<Addr<AppMaster<HA>>>,
    task_manager: Addr<TaskManager<HA>>,
}

impl<HA> HurricaneFrontend<HA>
where
    HA: HurricaneApplication + 'static,
{
    /// The driver function to start the `HurricaneFrontend`.
    ///
    /// ## Arguments
    /// `id` - the current id for this `HurricaneFrontend`, for determining whether `AppMaster`
    /// should start or not. The `id` is in HurricaneFrontend space.
    ///
    /// `hurricane_app` - the actual struct that implements `HurricaneApplication` trait.
    pub fn spawn(id: usize, hurricane_app: HA) -> Addr<HurricaneFrontend<HA>> {
        info!("======================================================================");
        info!("====================== Hurricane Frontend Alive ======================");
        info!("======================================================================");

        let app_master = if id == 0 {
            Some(AppMaster::spawn(id, hurricane_app.clone()))
        } else {
            None
        };

        let task_manager = TaskManager::spawn(id, hurricane_app);

        HurricaneFrontend {
            app_master,
            task_manager,
        }
        .start()
    }
}

impl<HA> Actor for HurricaneFrontend<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        // Initiate a timed event to check whether the current frontend has stopped.
        // If so, shut down gracefully.
        ctx.run_interval(
            Duration::from_secs(conf::get().frontend_heart_beat_interval),
            |actor, ctx| {
                if let Some(app_master) = actor.app_master.as_ref() {
                    if !app_master.connected() && !actor.task_manager.connected() {
                        ctx.stop();
                    }
                } else {
                    if !actor.task_manager.connected() {
                        ctx.stop();
                    }
                }
            },
        );
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        System::current().stop();
        info!("======================================================================");
        info!("====================== Hurricane Frontend Done! ======================");
        info!("======================================================================");
    }
}
