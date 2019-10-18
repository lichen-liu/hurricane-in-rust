use actix::prelude::*;
use bit_vec::*;
use env_logger;
use futures::future;
use log;
use rand::Rng;
use std::env;
use std::io;

extern crate hurricanelib;
use hurricanelib::prelude::*;

#[derive(Clone)]
struct Application {
    num_countries: usize,
    country_code_shift_bits: usize,
    country_ip_capacity: usize,
    country_subip_mask: usize,
}

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
            "0-create_data" => {
                self.phase_0_create_data(inputs.pop().unwrap(), outputs.pop().unwrap())
            }
            "1-split_region" => self.phase_1_split_region(inputs.pop().unwrap(), outputs),
            "2-region_unique" => {
                self.phase_2_region_unique(inputs.pop().unwrap(), outputs.pop().unwrap())
            }
            "2-region_unique.merge" => self.phase_2_region_unique_merge(
                inputs.pop().unwrap(),
                inputs.pop().unwrap(),
                outputs.pop().unwrap(),
            ),
            "3-region_count" => {
                self.phase_3_region_count(inputs.pop().unwrap(), outputs.pop().unwrap())
            }
            "3-region_count.merge" => self.phase_3_region_count_merge(
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
                "0-create_data",
                app_conf.clone(),
                Bag::from("garbage"),
                Bag::from("input"),
                6,
                MergeType::Append,
            ))],
            vec![Blueprint::from((
                "1-split_region",
                app_conf.clone(),
                Bag::from("input"),
                || -> Vec<_> {
                    (0..self.num_countries)
                        .map(|region_id| Bag::from(format!("region_{}", region_id)))
                        .collect()
                }(),
                6,
                MergeType::Append,
            ))],
            (0..self.num_countries)
                .map(|region_id| {
                    Blueprint::from((
                        "2-region_unique",
                        app_conf.clone(),
                        Bag::from(format!("region_{}", region_id)),
                        Bag::from(format!("unique_region_{}", region_id)),
                        6,
                        MergeType::Reduce,
                    ))
                })
                .collect(),
            (0..self.num_countries)
                .map(|region_id| {
                    Blueprint::from((
                        "3-region_count",
                        app_conf.clone(),
                        Bag::from(format!("unique_region_{}", region_id)),
                        Bag::from(format!("region_count_{}", region_id)),
                        6,
                        MergeType::Reduce,
                    ))
                })
                .collect(),
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
            "2-region_unique" => Some(Blueprint::from((
                "2-region_unique.merge".to_owned(),
                app_conf,
                inputs,
                outputs,
                1,
                MergeType::Nonclonable,
            ))),
            "3-region_count" => Some(Blueprint::from((
                "3-region_count.merge".to_owned(),
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

impl Application {
    fn new() -> Application {
        let num_countries: usize = 64;
        assert!(num_countries.is_power_of_two());
        let country_code_len: usize = num_countries.trailing_zeros() as usize;
        let country_code_shift_bits: usize = 32 - country_code_len as usize;
        let country_ip_capacity: usize = 1 << country_code_shift_bits;
        let country_subip_mask: usize =
            (!((num_countries - 1) << country_code_shift_bits) as usize) & 0x00000000ffffffff;

        println!("num_countries={}", num_countries);
        println!("country_code_len={}", country_code_len);
        println!("country_code_shift_bits={}", country_code_shift_bits);
        println!("country_ip_capacity={}", country_ip_capacity);
        println!("country_subip_mask={:X}", country_subip_mask);

        Application {
            num_countries,
            country_code_shift_bits,
            country_ip_capacity,
            country_subip_mask,
        }
    }

    fn phase_0_create_data(&mut self, in_bag: InBag, out_bag: OutBag) -> Props {
        Props::new(
            in_bag
                .execute::<I32Format, _, _>(|st| {
                    st.map(move |_| {
                        // Uniform distribution
                        let num: u32 = rand::thread_rng().gen_range(0, 0xffffffff);
                        num as i32
                    })
                })
                .into_bag(PhantomData::<I32Format>, out_bag),
        )
    }

    fn phase_1_split_region(&self, in_bag: InBag, out_bags: Vec<OutBag>) -> Props {
        let country_code_shift_bits = self.country_code_shift_bits;
        Props::new(in_bag.execute::<I32Format, _, _>(|st| st).into_bags(
            PhantomData::<I32Format>,
            out_bags,
            move |ip| ((ip as u32) >> country_code_shift_bits) as usize,
        ))
    }

    fn phase_2_region_unique(&self, in_bag: InBag, out_bag: OutBag) -> Props {
        let country_ip_capacity = self.country_ip_capacity;
        let country_subip_mask = self.country_subip_mask;
        Props::new(
            in_bag
                .execute::<I32Format, _, _>(|st| {
                    let new_bv = BitVec::from_elem(country_ip_capacity, false);
                    st.fold(new_bv, move |mut bv, x| {
                        bv.set((x as usize) & country_subip_mask, true);
                        future::ok::<_, io::Error>(bv)
                    })
                    .into_stream()
                })
                .into_bag(PhantomData::<BitVecFormat>, out_bag),
        )
    }

    fn phase_2_region_unique_merge(
        &self,
        in_bag_0: InBag,
        in_bag_1: InBag,
        out_bag: OutBag,
    ) -> Props {
        Props::new(
            in_bag_0
                .with(in_bag_1)
                .reduce::<BitVecFormat, _, _>(|st| {
                    st.map(|(mut a, b)| {
                        a.union(&b);
                        a
                    })
                })
                .into_bag(PhantomData::<BitVecFormat>, out_bag),
        )
    }

    fn phase_3_region_count(&self, in_bag: InBag, out_bag: OutBag) -> Props {
        Props::new(
            in_bag
                .execute::<BitVecFormat, _, _>(|st| {
                    st.fold(0, |acc, bv| {
                        future::ok::<_, io::Error>(acc + bv.iter().filter(|x| *x).count() as i64)
                    })
                    .into_stream()
                })
                .into_bag(PhantomData::<I64Format>, out_bag),
        )
    }

    fn phase_3_region_count_merge(
        &self,
        in_bag_0: InBag,
        in_bag_1: InBag,
        out_bag: OutBag,
    ) -> Props {
        Props::new(
            in_bag_0
                .with(in_bag_1)
                .reduce::<I64Format, _, _>(|st| st.map(|(a, b)| a + b))
                .into_bag(PhantomData::<I64Format>, out_bag),
        )
    }
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

    let setting: HurricaneConfig = HurricaneConfig::from_file("config.json");
    hurricane_frontend_conf::set(setting.get_frontend_config());
    hurricane_backend_conf::set(setting.get_backend_config());
    hurricane_chunk_metadata::set(setting.get_chunk_config());

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
        let _frontend = HurricaneFrontend::spawn(me, Application::new());
        sys.run();
    } else {
        // Backend.
        let backend_id = me - hurricane_frontend_conf::get().num_task_manager;
        let sys = System::new("Hurricane-Backend");
        let _backend = HurricaneBackend::spawn(backend_id);
        sys.run();
    }
}
