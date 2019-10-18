use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct ChunkConfig {
    /// Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize
    pub data_size: usize,
    /// Config.HurricaneConfig.BackendConfig.DataConfig.chunkSizeSize
    pub chunk_size_size: usize,
    /// Config.HurricaneConfig.BackendConfig.DataConfig.cmdSize
    pub cmd_size: usize,
    ///  Config.HurricaneConfig.BackendConfig.DataConfig.bagSize
    pub bag_size: usize,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct FrontendConfig {
    #[serde(default = "err_no_app_master_socket_addr")]
    pub app_master_socket_addr: SocketAddr,
    pub num_task_manager: usize,
    pub num_physical_node: usize,

    pub workers_parallelism: Vec<usize>,
    pub use_conf_parallelism: bool,

    pub is_cloning_enabled: bool,

    /// In millis.
    pub task_manager_fetch_task_interval: u64,
    /// In millis.
    pub task_manager_tick_interval: u64,
    /// In seconds.
    pub frontend_heart_beat_interval: u64,
    /// In %.
    pub cloning_cpu_load_threshold: f32,
    /// In millis.
    pub cloning_time_threshold: u64,

    pub app_master_tcp_nodelay: bool,
    pub task_manager_tcp_nodelay: bool,

    pub hurricane_work_io_tcp_nodelay: bool,
    pub hurricane_work_io_so_rcvbuf: Option<usize>,
    pub hurricane_work_io_so_sndbuf: Option<usize>,

    /// In ms.
    pub task_manager_connection_retry_interval: u64,
    pub task_manager_connection_retry_attempt: usize,

    /// In ms.
    pub hurricane_work_io_connection_retry_interval: u64,
    pub hurricane_work_io_connection_retry_attempt: usize,

    /// Do not actually send chunks to output bag to backend,
    /// instead, send a record to backend. This is mainly
    /// used for benchmarking purpose for low network bandwidth.
    pub send_outbags_as_record: bool,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct BackendConfig {
    #[serde(default = "err_no_hurricane_io_socket_addrs")]
    pub hurricane_io_socket_addrs: Vec<SocketAddr>,
    /// In seconds.
    pub backend_heart_beat_interval: u64,

    pub hurricane_io_tcp_nodelay: bool,
    pub hurricane_io_so_rcvbuf: Option<usize>,
    pub hurricane_io_so_sndbuf: Option<usize>,

    // `Hurricane IO ID` -> `Vec<(Bag, num_chunks)>` per Hurricane IO.
    pub hurricane_io_init_chunks: Vec<Vec<(String, usize)>>,

    /// Do not actually send chunks of input bag to frontend,
    /// instead, send a record to frontend. This is mainly
    /// used for benchmarking purpose for low network bandwidth.
    pub send_inbags_as_record: bool,
}

fn err_no_app_master_socket_addr() -> SocketAddr {
    panic!("Cannot find App Master Socket Addr in configuration!")
}

fn err_no_hurricane_io_socket_addrs() -> Vec<SocketAddr> {
    panic!("Cannot find Hurricane IO Socket Addrs in configuration!")
}

impl Default for ChunkConfig {
    fn default() -> Self {
        ChunkConfig {
            data_size: 1048576,
            chunk_size_size: 4,
            cmd_size: 96,
            bag_size: 64,
        }
    }
}

impl Default for FrontendConfig {
    fn default() -> Self {
        FrontendConfig {
            app_master_socket_addr: "127.0.0.1:8080".parse().unwrap(),
            num_task_manager: 1,
            num_physical_node: 1,

            workers_parallelism: Vec::new(),
            use_conf_parallelism: false,

            is_cloning_enabled: false,

            task_manager_fetch_task_interval: 500,
            task_manager_tick_interval: 500,
            frontend_heart_beat_interval: 2,
            cloning_cpu_load_threshold: 75.0,
            cloning_time_threshold: 1000,

            app_master_tcp_nodelay: false,
            task_manager_tcp_nodelay: false,

            hurricane_work_io_tcp_nodelay: true,
            hurricane_work_io_so_rcvbuf: None,
            hurricane_work_io_so_sndbuf: None,

            task_manager_connection_retry_interval: 500,
            task_manager_connection_retry_attempt: 10,
            hurricane_work_io_connection_retry_interval: 500,
            hurricane_work_io_connection_retry_attempt: 10,

            send_outbags_as_record: false,
        }
    }
}

impl FrontendConfig {
    pub fn new() -> FrontendConfig {
        Default::default()
    }
}

impl Default for BackendConfig {
    fn default() -> Self {
        BackendConfig {
            hurricane_io_socket_addrs: Vec::new(),
            backend_heart_beat_interval: 2,
            hurricane_io_tcp_nodelay: true,
            hurricane_io_so_rcvbuf: None,
            hurricane_io_so_sndbuf: None,
            hurricane_io_init_chunks: Vec::new(),

            send_inbags_as_record: false,
        }
    }
}

impl BackendConfig {
    pub fn new() -> BackendConfig {
        Default::default()
    }
}
