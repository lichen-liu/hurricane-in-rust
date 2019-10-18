use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Clone)]
pub struct AppConf {
    file: PathBuf,
    size: usize,
}

impl AppConf {
    pub fn new() -> AppConf {
        AppConf {
            file: PathBuf::new(),
            size: 4096,
        }
    }
}

impl Default for AppConf {
    fn default() -> Self {
        AppConf::new()
    }
}
