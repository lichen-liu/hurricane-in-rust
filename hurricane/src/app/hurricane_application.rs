use super::app_conf::AppConf;
use super::blueprint::{Blueprint, MountedBlueprint};
use crate::common::bag::Bag;
use crate::common::props::Props;

pub trait HurricaneApplication: Clone + Send + Sized {
    fn instantiate(&mut self, blueprint: MountedBlueprint) -> Props;

    fn blueprints(&mut self, app_conf: AppConf) -> Vec<Vec<Blueprint>>;

    #[allow(unused_variables)]
    fn merge(
        &mut self,
        phase: String,
        app_conf: AppConf,
        inputs: Vec<Bag>,
        outputs: Vec<Bag>,
    ) -> Option<Blueprint> {
        None
    }
}
