use actix::prelude::*;
use env_logger;
use futures::future;
use log;
use std::env;
use std::io;

extern crate hurricanelib;
use hurricanelib::prelude::*;

#[derive(Clone)]
struct Application;

impl HurricaneApplication for Application {
    fn instantiate(&mut self, blueprint: MountedBlueprint) -> Props {
        let MountedBlueprint {
            name,
            app_conf: _,
            mut inputs,
            mut outputs,
            num_threads: _,
            merge_type: _,
        } = blueprint;

        match name.to_lowercase().trim() {
            "phase-0-map" => phase_0_map(inputs.pop().unwrap(), outputs.pop().unwrap()),
            "phase-1-count" => phase_1_count(inputs.pop().unwrap(), outputs.pop().unwrap()),
            "phase-1-count.merge" => phase_1_count_merge(
                inputs.pop().unwrap(),
                inputs.pop().unwrap(),
                outputs.pop().unwrap(),
            ),
            _ => panic!("Something goes wrong."),
        }
    }

    fn blueprints(&mut self, app_conf: AppConf) -> Vec<Vec<Blueprint>> {
        vec![
            vec![Blueprint::from((
                "Phase-0-Map",
                app_conf.clone(),
                Bag::from("in"),
                Bag::from("mapped"),
                6,
                MergeType::Append,
            ))],
            vec![Blueprint::from((
                "Phase-1-Count",
                app_conf.clone(),
                Bag::from("mapped"),
                Bag::from("out"),
                6,
                MergeType::Reduce,
            ))],
        ]
    }

    fn merge(
        &mut self,
        phase: String,
        app_conf: AppConf,
        inputs: Vec<Bag>,
        outputs: Vec<Bag>,
    ) -> Option<Blueprint> {
        match &phase[..] {
            "Phase-1-Count" => Some(Blueprint::from((
                "Phase-1-Count.merge".to_owned(),
                app_conf,
                inputs,
                outputs,
                1,
                MergeType::Nonclonable,
            ))),
            _ => None,
        }
    }
}

fn phase_0_map(in_bag: InBag, out_bag: OutBag) -> Props {
    Props::new(
        in_bag
            .execute::<I64Format, _, _>(|st| st.map(|_| 1))
            .into_bag(PhantomData::<I64Format>, out_bag),
    )
}

fn phase_1_count(in_bag: InBag, out_bag: OutBag) -> Props {
    Props::new(
        in_bag
            .execute::<I64Format, _, _>(|st| {
                st.map(|x| x * x)
                    .fold(0, |acc, x| future::ok::<_, io::Error>(acc + x))
                    .into_stream()
            })
            .into_bag(PhantomData::<I64Format>, out_bag),
    )
}

fn phase_1_count_merge(in_bag_0: InBag, in_bag_1: InBag, out_bag: OutBag) -> Props {
    Props::new(
        in_bag_0
            .with(in_bag_1)
            .reduce::<I64Format, _, _>(|st| st.map(|(a, b)| a + b))
            .into_bag(PhantomData::<I64Format>, out_bag),
    )
}

fn init_logger() {
    // Setup the logger.
    let mut builder = env_logger::Builder::from_default_env();
    // Print to std out.
    builder.target(env_logger::Target::Stdout);
    // Close all logs.
    builder.filter_level(log::LevelFilter::Off);
    builder.filter_module("hurricanelib", log::LevelFilter::Info);
    builder.default_format_module_path(false);
    builder.default_format_timestamp(false);
    builder.init();
}

fn main() {
    init_logger();

    let setting: HurricaneConfig = HurricaneConfig::from_file("main_config.json");
    hurricane_frontend_conf::set(setting.get_frontend_config());
    hurricane_backend_conf::set(setting.get_backend_config());

    // Get me.
    let args: Vec<String> = env::args().collect();
    let me: usize = args.last().unwrap().parse().unwrap();
    assert!(
        me < hurricane_frontend_conf::get().num_task_manager
            + hurricane_backend_conf::get()
                .hurricane_io_socket_addrs
                .len(),
        "There are {} Frontend node(s) and {} Backend node(s)!",
        hurricane_frontend_conf::get().num_task_manager,
        hurricane_backend_conf::get()
            .hurricane_io_socket_addrs
            .len()
    );

    // Determine whether this program runs on frontend or backend.
    if me < hurricane_frontend_conf::get().num_task_manager {
        // Frontend.
        let sys = System::new("Hurricane-Frontend");
        let _frontend = HurricaneFrontend::spawn(me, Application);
        sys.run();
    } else {
        // Backend.
        let backend_id = me - hurricane_frontend_conf::get().num_task_manager;
        let sys = System::new("Hurricane-Backend");
        let _backend = HurricaneBackend::spawn(backend_id);
        sys.run();
    }
}
