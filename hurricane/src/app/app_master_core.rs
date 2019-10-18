use super::app_conf::*;
use super::app_master::*;
use super::blueprint::*;
use super::hurricane_application::*;
use crate::common::{bag::*, messages::*, types::TaskId, work_bag::*};
use crate::frontend::hurricane_frontend;
use actix::prelude::*;
use chrono::prelude::*;
use itertools::EitherOrBoth;
use itertools::Itertools;
use log::*;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use uuid::Uuid;

fn generate_random_uuid() -> Uuid {
    Uuid::new_v4()
}

/// Map Vec<Blueprint> into HashMap<TaskId, Blueprint>.
fn convert_into_description(blueprints: Vec<Blueprint>) -> HashMap<TaskId, Blueprint> {
    blueprints
        .into_iter()
        .map(|bp| (generate_random_uuid(), bp))
        .collect()
}

/// Map Vec<Vec<Blueprint>> into Vec<HashMap<TaskId, Blueprint>.
fn convert_into_app_description(
    blueprints: Vec<Vec<Blueprint>>,
) -> Vec<HashMap<TaskId, Blueprint>> {
    blueprints
        .into_iter()
        .map(|vec_bp| convert_into_description(vec_bp))
        .collect()
}

/// Build the merge tree for `destination_bag` from `source_bags`.
///
/// `source_bags` - must have all bags intended to be merged together.
fn build_merge_tree_for_bag(
    destination_bag: Bag,
    source_bags: Vec<Bag>,
) -> Option<Vec<Vec<(EitherOrBoth<Bag, Bag>, Bag)>>> {
    // Input.
    let intermediate_final_bag = Bag::from(format!("{}.final", destination_bag.name));
    let merging_base_layer: Vec<(EitherOrBoth<Bag, Bag>, Bag)> = source_bags
        .into_iter()
        .map(|src_bag| (EitherOrBoth::Left(src_bag.clone()), src_bag))
        .collect();

    if merging_base_layer.len() <= 1 {
        return None;
    }

    assert!(merging_base_layer.len() > 1);
    // Build merge tree.
    // for merging, then ((src.left, src.right), dest)
    // if no merging, then ((src, ), dest)
    // if renaming, then((, src), dest)
    let mut merge_tree: Vec<Vec<(EitherOrBoth<Bag, Bag>, Bag)>> = vec![merging_base_layer];
    let mut merge_level = 0;

    while merge_tree.last().unwrap().len() > 1 {
        let mut merge_level_local_id = 0;
        let merge_layer = merge_tree.last().unwrap();

        let next_merge_layer: Vec<_> = merge_layer
            .iter()
            .batching(|it| match it.next() {
                None => None,
                Some((_, bag_0)) => match it.next() {
                    None => Some((EitherOrBoth::Left(bag_0.clone()), bag_0.clone())),
                    Some((_, bag_1)) => {
                        let r = Some((
                            EitherOrBoth::Both(bag_0.clone(), bag_1.clone()),
                            Bag::from(format!(
                                "{}.merge{}.{}",
                                destination_bag.name, merge_level, merge_level_local_id
                            )),
                        ));
                        merge_level_local_id += 1;
                        r
                    }
                },
            })
            .collect();

        merge_tree.push(next_merge_layer);
        merge_level += 1;
    }

    // Set the root of merge tree to have the intermediate_final_bag name.
    assert_eq!(merge_tree.last().unwrap().len(), 1);
    merge_tree.last_mut().unwrap().first_mut().unwrap().1 = intermediate_final_bag.clone();

    // Add one more layer to do the renaming.
    merge_tree.push(vec![(
        EitherOrBoth::Right(intermediate_final_bag),
        destination_bag,
    )]);

    Some(merge_tree)
}

fn combine_merge_trees<T: Clone>(all_merge_trees: Vec<Vec<Vec<T>>>) -> VecDeque<Vec<T>> {
    let mut merged_trees = VecDeque::new();

    // Figure out the maximum number of stages.
    merged_trees.resize(
        all_merge_trees
            .iter()
            .map(|tree| tree.len())
            .max()
            .unwrap_or(0),
        Vec::new(),
    );

    all_merge_trees.into_iter().for_each(|tree| {
        tree.into_iter()
            .enumerate()
            .for_each(|(layer_id, layer)| merged_trees[layer_id].extend(layer))
    });

    merged_trees
}

/// Core logic Actor for AppMaster.
pub struct AppMasterCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    id: usize,
    app_master: Option<Addr<AppMaster<HA>>>,
    /// The real `HurricaneApplication`.
    hurricane_app: HA,

    app_description: Vec<HashMap<TaskId, Blueprint>>,
    clone_description: HashMap<TaskId, Blueprint>,
    merge_description: HashMap<TaskId, Blueprint>,
    /// clone `TaskId` -> original `TaskId`.
    clone_parents: HashMap<TaskId, TaskId>,
    /// original `TaskId` -> cloned `TaskId`s.
    clone_tasks: HashMap<TaskId, Vec<TaskId>>,
    work_bag: WorkBag,
    merge_tasks: Option<VecDeque<Vec<TaskId>>>,
    current_phase: Option<usize>,
    start_time: Option<Instant>,
}

impl<HA> AppMasterCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    fn get_name(&self) -> String {
        format!(
            GET_NAME_FORMAT!(),
            format!("[{}, Core]", self.id),
            "AppMaster"
        )
    }

    pub fn new(
        id: usize,
        app_master: Addr<AppMaster<HA>>,
        mut hurricane_app: HA,
    ) -> AppMasterCore<HA> {
        let blueprints = hurricane_app.blueprints(AppConf::new());

        let app_description = convert_into_app_description(blueprints);

        AppMasterCore {
            id,
            app_master: Some(app_master),
            hurricane_app,
            app_description,
            clone_description: HashMap::new(),
            merge_description: HashMap::new(),
            clone_parents: HashMap::new(),
            clone_tasks: HashMap::new(),
            work_bag: Default::default(),
            merge_tasks: None,
            current_phase: None,
            start_time: None,
        }
    }

    fn get_blueprint(&self, task_id: &TaskId) -> Option<Blueprint> {
        match self.app_description[self.current_phase.unwrap()]
            .get(task_id)
            .cloned()
        {
            Some(blueprint) => Some(blueprint),
            None => match self.clone_description.get(task_id).cloned() {
                Some(blueprint) => Some(blueprint),
                None => self.merge_description.get(task_id).cloned(),
            },
        }
    }

    fn build_blueprint_for_merge_tree(
        &mut self,
        merge_tree: Vec<Vec<(EitherOrBoth<Bag, Bag>, Bag)>>,
        parent_blueprint: Blueprint,
    ) -> Vec<Vec<Blueprint>> {
        // for merging, then ((src.left, src.right), dest)
        // if no merging, then ((src, ), dest)
        // if renaming, then((, src), dest)
        merge_tree.into_iter().map(|layer| {
            layer.into_iter().filter_map(|(ins, out)| match ins {
                EitherOrBoth::Left(_) => None,
                EitherOrBoth::Right(in0) => {
                    let mut bp = Blueprint::from((
                        &format!("{}.merge.rename", parent_blueprint.name)[..],
                        parent_blueprint.app_conf.clone(),
                        in0,
                        out,
                        1,
                        MergeType::Nonclonable,
                    ));
                    bp.set_rename_bag_op(true);
                    Some(bp)
                }
                EitherOrBoth::Both(in0, in1) => {
                    let mut bp = self.hurricane_app.merge(
                        parent_blueprint.name.to_string(),
                        parent_blueprint.app_conf.clone(),
                        vec![in0, in1],
                        vec![out],
                    );
                    match bp.as_mut() {
                        Some(merge_blueprint) => {
                            if merge_blueprint.num_threads > 1 {
                                warn!("{}Multithreading is not enabled for Merge in blueprint '{}'!", self.get_name(), merge_blueprint.name());
                                merge_blueprint.num_threads = 1;
                            }

                            if merge_blueprint.merge_type != MergeType::Nonclonable {
                                warn!("{}Merge Task must be marked as Nonclonable in blueprint '{}'!", self.get_name(), merge_blueprint.name());
                                merge_blueprint.merge_type = MergeType::Nonclonable;
                            }
                        }
                        None => {
                            error!("{}Missing merge routine for Blueprint '{}' requiring 'MergeType::Reduce'!", self.get_name(), parent_blueprint.name());
                            panic!("Missing merge routine for Blueprint '{}' requiring 'MergeType::Reduce'!", self.get_name());
                        }
                    }
                    bp
                }
            }).collect()
        }).filter(|layer:&Vec<_>|!layer.is_empty()).collect()
    }

    fn goto_next_phase(&mut self) -> bool {
        match self.merge_tasks.take() {
            None => {
                if !hurricane_frontend::conf::get().is_cloning_enabled {
                    assert!(self.clone_tasks.is_empty());
                    self.merge_tasks = Some(VecDeque::new());
                    self.goto_next_phase()
                } else {
                    // Take clone_tasks out from self.
                    let clone_tasks: Vec<_> = self.clone_tasks.drain().collect();

                    // Build merge trees for cloned tasks that require merging.
                    let mut uncombined_merge_trees: Vec<Vec<Vec<Blueprint>>> = Vec::new();
                    clone_tasks
                        .into_iter()
                        .for_each(|(parent_task, cloned_tasks)| {
                            let parent_blueprint = self.get_blueprint(&parent_task).unwrap();

                            if parent_blueprint.merge_type.require_merge() {
                                parent_blueprint.outputs.iter().enumerate().for_each(
                                    |(outbag_id, parent_outbag)| {
                                        let mut bags_to_merge: Vec<_> = cloned_tasks
                                            .iter()
                                            .map(|cloned_task_id| {
                                                let cloned_blueprint =
                                                    self.get_blueprint(cloned_task_id).unwrap();
                                                assert!(cloned_blueprint
                                                    .merge_type
                                                    .require_merge());

                                                cloned_blueprint.outputs[outbag_id].clone()
                                            })
                                            .collect();
                                        bags_to_merge.push(parent_outbag.clone());

                                        info!(
                                            "{}Going to merge {} Cloned Bag(s) of Bag({}) for task '{}'.",
                                            self.get_name(),
                                            bags_to_merge.len(),
                                            parent_outbag.name,
                                            parent_blueprint.name
                                        );

                                        // Build merge tree.
                                        // for merging, then ((src.left, src.right), dest)
                                        // if no merging, then ((src, ), dest)
                                        // if renaming, then((, src), dest)
                                        // If only one parent_outbag, then no need to merge.
                                        if let Some(merge_tree) = build_merge_tree_for_bag(
                                            parent_outbag.clone(),
                                            bags_to_merge,
                                        ) {
                                            uncombined_merge_trees.push(
                                                self.build_blueprint_for_merge_tree(
                                                    merge_tree,
                                                    parent_blueprint.clone(),
                                                ),
                                            );
                                        }
                                    },
                                );
                            }
                        });

                    // Combine multiple merge trees into a single big merge tree.
                    let combined_merge_trees: VecDeque<Vec<Blueprint>> =
                        combine_merge_trees(uncombined_merge_trees);

                    // Generate TaskId.
                    let rich_combined_merge_trees: Vec<HashMap<_, _>> = combined_merge_trees
                        .into_iter()
                        .map(|layer| convert_into_description(layer.clone()))
                        .collect();

                    // Store combined merge tree in terms of TaskId.
                    self.merge_tasks = Some(
                        rich_combined_merge_trees
                            .iter()
                            .map(|layer| {
                                layer.iter().map(|(task_id, _)| task_id).cloned().collect()
                            })
                            .collect(),
                    );

                    let merge_tasks = self.merge_tasks.as_ref().unwrap();
                    let num_merge_tasks: usize = merge_tasks.iter().map(|layer| layer.len()).sum();
                    info!(
                        "{}Phase {} has {} stage(s) merging ({} tasks).",
                        self.get_name(),
                        self.current_phase.unwrap(),
                        merge_tasks.len(),
                        num_merge_tasks
                    );

                    // Store TaskId -> Blueprint mapping.
                    self.merge_description = rich_combined_merge_trees
                        .into_iter()
                        .map(|layer| layer.into_iter())
                        .flatten()
                        .collect();

                    self.clone_description.clear();
                    self.clone_parents.clear();
                    self.clone_tasks.clear();

                    self.goto_next_phase()
                }
            }
            Some(mut merge_tasks) => {
                if merge_tasks.is_empty() {
                    self.merge_tasks = None;
                    self.merge_description.clear();

                    // advance current_phase to a valid one
                    let mut current_phase: usize = match self.current_phase {
                        None => 0,
                        Some(n) => n + 1,
                    };

                    if current_phase >= self.app_description.len() {
                        return false;
                    }

                    while self.app_description[current_phase].is_empty() {
                        if current_phase == self.app_description.len() - 1 {
                            return false; // no more phases
                        }

                        current_phase += 1;
                    }

                    assert!(self.work_bag.ready.is_empty());
                    self.work_bag
                        .ready
                        .extend(self.app_description[current_phase].keys().cloned());
                    self.current_phase = Some(current_phase);

                    info!(
                        "{}Moving on to phase {}.",
                        self.get_name(),
                        self.current_phase.unwrap()
                    );

                    true
                } else {
                    self.work_bag.ready.extend(merge_tasks.pop_front().unwrap());
                    self.merge_tasks = Some(merge_tasks);
                    true
                }
            }
        }
    }
}

impl<HA> Actor for AppMasterCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.start_time = Some(Instant::now());

        info!(
            "{}Hurricane Application Begin! [{}]",
            self.get_name(),
            Local::now().format("%F %a %H:%M:%S%.3f").to_string()
        );

        self.merge_tasks = Some(VecDeque::new());
        if !self.goto_next_phase() {
            // shutdown all
            info!("{}All Hurricane phases and tasks done!", self.get_name());
            ctx.notify_later(PoisonPill, Duration::from_secs(1));
        }
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        let elapsed_time_since_start = self.start_time.take().unwrap().elapsed();

        info!(
            "{}Hurricane Application End! [{}]",
            self.get_name(),
            Local::now().format("%F %a %H:%M:%S%.3f").to_string()
        );
        info!(
            "{}    Elapsed time: {:?}.",
            self.get_name(),
            elapsed_time_since_start
        );
    }
}

impl<HA> Handler<AppMasterGetTask> for AppMasterCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = Task;

    fn handle(&mut self, _msg: AppMasterGetTask, _ctx: &mut Self::Context) -> Self::Result {
        match self.work_bag.ready.pop() {
            Some(task_id) => {
                let blueprint = self.get_blueprint(&task_id).unwrap();
                let r = self.work_bag.running.insert(task_id.clone());
                assert!(r);
                Task(Some((task_id, blueprint)))
            }
            None => Task(None),
        }
    }
}

impl<HA> Handler<AppMasterTaskCompleted> for AppMasterCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = TaskCompletedAck;

    fn handle(&mut self, msg: AppMasterTaskCompleted, ctx: &mut Self::Context) -> Self::Result {
        if self.work_bag.running.contains(&msg.0) {
            let task_id = self.work_bag.running.take(&msg.0).unwrap();
            let r = self.work_bag.done.insert(task_id);
            assert!(r);
            if self.work_bag.ready.is_empty() && self.work_bag.running.is_empty() {
                if !self.goto_next_phase() {
                    // shutdown all
                    info!("{}All Hurricane phases and tasks done!", self.get_name());
                    ctx.notify_later(PoisonPill, Duration::from_secs(1));
                }
            }

            TaskCompletedAck(Ok(()))
        } else {
            TaskCompletedAck(Err(()))
        }
    }
}

impl<HA> Handler<PoisonPill> for AppMasterCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: PoisonPill, ctx: &mut Self::Context) -> Self::Result {
        debug!("{}Shutting down...", self.get_name());

        self.app_master.as_ref().unwrap().do_send(msg);

        ctx.stop();
    }
}

impl<HA> Handler<AppMasterPleaseClone> for AppMasterCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = Result<(), ()>;

    fn handle(&mut self, msg: AppMasterPleaseClone, _ctx: &mut Self::Context) -> Self::Result {
        if hurricane_frontend::conf::get().is_cloning_enabled {
            let task_id = msg.0;
            if let Some(mut blueprint) = self.get_blueprint(&task_id) {
                if blueprint.merge_type == MergeType::Nonclonable {
                    info!("{}<{}> --C--> X.", self.get_name(), task_id);
                    warn!(
                        "{}Attempt to clone Nonclonable Task '{}'!",
                        self.get_name(),
                        blueprint.name()
                    );
                    return Err(());
                }

                let parent_task_id = self
                    .clone_parents
                    .get(&task_id)
                    .unwrap_or(&task_id)
                    .to_owned();

                let num_clones = self
                    .clone_tasks
                    .get(&parent_task_id)
                    .map(|cloned_task| cloned_task.len())
                    .unwrap_or(0)
                    + 1;

                if num_clones < hurricane_frontend::conf::get().num_physical_node {
                    // Generate new cloned_task_id and cloned_blueprint.
                    let cloned_task_id = generate_random_uuid();
                    info!(
                        "{}<{}> --C--> <{}>.",
                        self.get_name(),
                        task_id,
                        cloned_task_id
                    );

                    let cloned_blueprint = if blueprint.merge_type.require_merge() {
                        blueprint.outputs.iter_mut().for_each(|bag| {
                            bag.name.push_str(&format!(".clone{}", num_clones));
                        });
                        blueprint
                    } else {
                        blueprint
                    };

                    // Store the cloned_task_id -> cloned_blueprint mapping.
                    let r = self
                        .clone_description
                        .insert(cloned_task_id.clone(), cloned_blueprint);
                    assert!(
                        r.is_none(),
                        "Oops! The new randomly generated cloned_task_id by Uuid has a conflict!"
                    );

                    // Store the cloned_task_id -> parent_task_id mapping.
                    let r = self
                        .clone_parents
                        .insert(cloned_task_id.clone(), parent_task_id.clone());
                    assert!(
                        r.is_none(),
                        "Oops! The new randomly generated cloned_task_id by Uuid has a conflict!"
                    );

                    // Store the cloned_task.
                    self.clone_tasks
                        .entry(parent_task_id)
                        .or_insert(Vec::new())
                        .push(cloned_task_id.clone());

                    // Add cloned_task_id into ready bag.
                    self.work_bag.ready.push(cloned_task_id);

                    return Ok(());
                } else {
                    info!("{}<{}> --C--> X.", self.get_name(), task_id);
                }
            }
        }
        return Err(());
    }
}

impl<HA> Handler<LocalDebugMessages> for AppMasterCore<HA>
where
    HA: HurricaneApplication + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: LocalDebugMessages, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            LocalDebugMessages::CloneWorker(assert_result) => {
                if let Some(task_id) = self
                    .work_bag
                    .running
                    .iter()
                    .nth(0)
                    .or(self.work_bag.ready.first())
                    .cloned()
                {
                    info!(
                        "{}<DEBUG> CloneWorker for task {}.",
                        self.get_name(),
                        task_id
                    );

                    let req = ctx.address().send(AppMasterPleaseClone(task_id));
                    ctx.spawn(
                        req.into_actor(self)
                            .map_err(|_, _, _| ())
                            .map(move |res, _, _| {
                                if let Some(expect_should_clone) = assert_result {
                                    if expect_should_clone {
                                        assert_eq!(res, Ok(()));
                                    } else {
                                        assert_eq!(res, Err(()));
                                    }
                                }
                                ()
                            }),
                    );
                }
            }
            LocalDebugMessages::KillSystem(delay) => {
                ctx.run_later(delay, |_, _| System::current().stop());
            }
        }
    }
}

/// Messages used for debugging purpose.
#[allow(dead_code)]
#[derive(Message)]
enum LocalDebugMessages {
    /// Self-asserting message, assert for whether `PleaseClone` is successful.
    CloneWorker(Option<bool>),
    /// Suicide message, kill the entire System with `Duration` for delay.
    KillSystem(Duration),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::props::*;
    use crate::frontend::hurricane_frontend;
    use crate::util::config::FrontendConfig;

    #[derive(Clone)]
    struct MockApp;
    impl HurricaneApplication for MockApp {
        fn instantiate(&mut self, _blueprint: MountedBlueprint) -> Props {
            Props::fake()
        }

        fn blueprints(&mut self, _app_conf: AppConf) -> Vec<Vec<Blueprint>> {
            vec![vec![Blueprint::new(), Blueprint::new()]]
        }
    }

    #[test]
    fn please_clone() {
        // Set up one of key Decision condition for cloning.
        let mut conf = FrontendConfig::new();
        conf.num_physical_node = 3;
        conf.is_cloning_enabled = true;
        hurricane_frontend::conf::set(conf);

        let sys = System::new("Hurricane-UnitTest");

        // Mock input from `HurricaneApplication::blueprints()`.
        let fake_input: Vec<Vec<Blueprint>> = vec![vec![Default::default(), Default::default()]];

        let app_description = convert_into_app_description(fake_input);

        let app_master = AppMasterCore {
            id: 0,
            app_master: None,
            hurricane_app: MockApp,
            app_description,
            clone_description: HashMap::new(),
            merge_description: HashMap::new(),
            clone_parents: HashMap::new(),
            clone_tasks: HashMap::new(),
            work_bag: Default::default(),
            merge_tasks: None,
            current_phase: None,
            start_time: None,
        }
        .start();

        app_master.do_send(LocalDebugMessages::CloneWorker(Some(true)));
        app_master.do_send(LocalDebugMessages::CloneWorker(Some(true)));
        app_master.do_send(LocalDebugMessages::CloneWorker(Some(false)));
        app_master.do_send(LocalDebugMessages::KillSystem(Duration::from_secs(2)));

        sys.run();
    }

    #[test]
    fn build_merge_tree_for_single_bag() {
        let dest_bag = Bag::from("phase1.out");

        let src_bags: Vec<Bag> = (0..1000000)
            .map(|i| {
                if i == 0 {
                    Bag::from(format!("{}", dest_bag.name))
                } else {
                    Bag::from(format!("{}.clone{}", dest_bag.name, i))
                }
            })
            .collect();

        let merge_tree = build_merge_tree_for_bag(dest_bag, src_bags).unwrap();

        let mut prev_layer_num_element = 0;
        for (i, layer) in merge_tree.iter().enumerate() {
            // println!("[{}] {:?}", i, layer);
            if i == 0 {
                prev_layer_num_element = layer.len() * 2;
            }

            assert_eq!(layer.len(), (prev_layer_num_element + 1) / 2);

            prev_layer_num_element = layer.len();
        }
    }

    #[test]
    fn combine_merge_stages() {
        // A vector of merge trees, internal structured replaced by `usize`.
        let num_tree = 100;
        let all_merge_trees: Vec<Vec<Vec<usize>>> = (0..num_tree)
            .map(|tree_id| {
                (0..tree_id)
                    .map(|layer_id| (0..(tree_id - layer_id)).collect())
                    .collect()
            })
            .collect();

        // Print unmerged trees.
        // for (tree_id, tree) in all_merge_trees.iter().enumerate() {
        //     println!("Tree {}", tree_id);
        //     for layer in tree.iter() {
        //         println!("{:?}", layer);
        //     }
        //     println!("");
        // }

        // Merge.

        let merged_trees: VecDeque<Vec<usize>> = combine_merge_trees(all_merge_trees);

        // Print merged trees
        // println!("Merged");
        for (layer_id, layer) in merged_trees.iter().enumerate() {
            // println!("{:?}", layer);

            let expected_layer_len: usize =
                (layer_id..num_tree).map(|tree_id| tree_id - layer_id).sum();
            assert_eq!(layer.len(), expected_layer_len);
        }
    }
}
