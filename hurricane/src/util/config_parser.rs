use super::config::*;
use config::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct HurricaneConfig {
    #[serde(default)]
    chunk_config: ChunkConfig,

    frontend_config: FrontendConfig,
    backend_config: BackendConfig,
}

impl HurricaneConfig {
    #[allow(dead_code)]
    pub fn from_file(path: &str) -> HurricaneConfig {
        let mut source = Config::default();
        source.merge(File::with_name(path)).unwrap();
        source.try_into().expect("Invalid Configuration Format!")
    }

    pub fn get_backend_config(&self) -> BackendConfig {
        self.backend_config.clone()
    }

    pub fn get_frontend_config(&self) -> FrontendConfig {
        self.frontend_config.clone()
    }

    pub fn get_chunk_config(&self) -> ChunkConfig {
        self.chunk_config.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_complete_config() {
        // Though not all fields are specified, but all required fields present,
        // thus deemed as "complete".
        // Parse into the Settings Struct.
        let setting: HurricaneConfig = HurricaneConfig::from_file("tests/data/test_config.json");

        assert_eq!(
            setting.frontend_config.app_master_socket_addr,
            "127.0.0.1:12345".parse().unwrap()
        );
        assert_eq!(setting.frontend_config.num_task_manager, 1);
        assert_eq!(setting.frontend_config.num_physical_node, 1);
        assert_eq!(setting.frontend_config.is_cloning_enabled, true);

        assert_eq!(setting.frontend_config.task_manager_tick_interval, 1);
        assert_eq!(setting.frontend_config.cloning_time_threshold, 2);

        assert_eq!(setting.backend_config.hurricane_io_socket_addrs.len(), 1);
        assert_eq!(
            setting
                .backend_config
                .hurricane_io_socket_addrs
                .get(0)
                .cloned(),
            "127.0.0.1:54321".parse().ok()
        );
        assert_eq!(setting.backend_config.backend_heart_beat_interval, 2);

        // Test fields that should use default values.
        assert_eq!(setting.frontend_config.frontend_heart_beat_interval, 2);
        assert_eq!(setting.frontend_config.cloning_cpu_load_threshold, 75.0);
        assert_eq!(setting.frontend_config.use_conf_parallelism, false);
    }

    #[test]
    #[should_panic(expected = "Invalid Configuration Format!: missing field `frontend_config`")]
    fn parse_empty_config() {
        // Should fail, since it does not have frontend_config and backend_config.
        let _setting: HurricaneConfig = HurricaneConfig::from_file("tests/data/empty_config.json");
    }

    #[test]
    #[should_panic(expected = "Cannot find Hurricane IO Socket Addrs in configuration!")]
    fn parse_incomplete_config() {
        // Should fail, since it does not have the must-have field hurricane_io_socket_addrs in backend_config.
        let _setting: HurricaneConfig =
            HurricaneConfig::from_file("tests/data/incomplete_config.json");
    }
}
