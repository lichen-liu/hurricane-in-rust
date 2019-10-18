use super::app_conf::AppConf;
use crate::common::bag::{Bag, InBag, OutBag};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum MergeType {
    Append,
    Reduce,
    Nonclonable,
}

impl MergeType {
    pub fn new() -> MergeType {
        MergeType::Append
    }

    pub fn require_merge(&self) -> bool {
        *self == MergeType::Reduce
    }
}

impl Default for MergeType {
    fn default() -> Self {
        MergeType::new()
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Blueprint {
    pub name: String,
    pub app_conf: AppConf,
    pub inputs: Vec<Bag>,
    pub outputs: Vec<Bag>,
    pub num_threads: usize,
    pub merge_type: MergeType,
    rename_bag_op: bool,
}

impl Blueprint {
    pub fn new() -> Blueprint {
        Blueprint {
            name: Default::default(),
            app_conf: AppConf::new(),
            inputs: Default::default(),
            outputs: Default::default(),
            num_threads: 1,
            merge_type: MergeType::new(),
            rename_bag_op: false,
        }
    }

    pub fn from(args: impl Into<Blueprint>) -> Blueprint {
        args.into()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn set_rename_bag_op(&mut self, rename_bag_op: bool) {
        self.rename_bag_op = rename_bag_op;
        if rename_bag_op {
            assert_eq!(self.inputs.len(), 1);
            assert_eq!(self.outputs.len(), 1);
        }
    }

    pub fn is_rename_bag_op(&self) -> bool {
        self.rename_bag_op
    }
}

impl Default for Blueprint {
    fn default() -> Self {
        Blueprint::new()
    }
}

impl Into<Blueprint> for (String, AppConf, Vec<Bag>, Vec<Bag>, usize, MergeType) {
    fn into(self) -> Blueprint {
        Blueprint {
            name: self.0,
            app_conf: self.1,
            inputs: self.2,
            outputs: self.3,
            num_threads: self.4,
            merge_type: self.5,
            rename_bag_op: false,
        }
    }
}

impl Into<Blueprint> for (&str, AppConf, usize, MergeType) {
    fn into(self) -> Blueprint {
        Blueprint {
            name: String::from(self.0),
            app_conf: self.1,
            inputs: Vec::new(),
            outputs: Vec::new(),
            num_threads: self.2,
            merge_type: self.3,
            rename_bag_op: false,
        }
    }
}

impl Into<Blueprint> for (&str, AppConf, Bag, usize, MergeType) {
    fn into(self) -> Blueprint {
        Blueprint {
            name: String::from(self.0),
            app_conf: self.1,
            inputs: vec![self.2],
            outputs: Vec::new(),
            num_threads: self.3,
            merge_type: self.4,
            rename_bag_op: false,
        }
    }
}

impl Into<Blueprint> for (&str, AppConf, Bag, Bag, usize, MergeType) {
    fn into(self) -> Blueprint {
        Blueprint {
            name: String::from(self.0),
            app_conf: self.1,
            inputs: vec![self.2],
            outputs: vec![self.3],
            num_threads: self.4,
            merge_type: self.5,
            rename_bag_op: false,
        }
    }
}

impl Into<Blueprint> for (&str, AppConf, Vec<Bag>, Bag, usize, MergeType) {
    fn into(self) -> Blueprint {
        Blueprint {
            name: String::from(self.0),
            app_conf: self.1,
            inputs: self.2,
            outputs: vec![self.3],
            num_threads: self.4,
            merge_type: self.5,
            rename_bag_op: false,
        }
    }
}

impl Into<Blueprint> for (&str, AppConf, Bag, Vec<Bag>, usize, MergeType) {
    fn into(self) -> Blueprint {
        Blueprint {
            name: String::from(self.0),
            app_conf: self.1,
            inputs: vec![self.2],
            outputs: self.3,
            num_threads: self.4,
            merge_type: self.5,
            rename_bag_op: false,
        }
    }
}

pub struct MountedBlueprint {
    pub name: String,
    pub app_conf: AppConf,
    pub inputs: Vec<InBag>,
    pub outputs: Vec<OutBag>,
    pub num_threads: usize,
    pub merge_type: MergeType,
}

impl MountedBlueprint {
    pub fn from(
        blueprint: Blueprint,
        inputs: Vec<InBag>,
        outputs: Vec<OutBag>,
    ) -> MountedBlueprint {
        let Blueprint {
            name,
            app_conf,
            inputs: _,
            outputs: _,
            num_threads,
            merge_type,
            rename_bag_op: _,
        } = blueprint;

        MountedBlueprint {
            name,
            app_conf,
            inputs,
            outputs,
            num_threads,
            merge_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blueprint_constructor() {
        let blueprint = Blueprint::from(("hi", AppConf::new(), 2, MergeType::Reduce));

        assert_eq!(blueprint.name, "hi");
        assert_eq!(blueprint.inputs, vec![]);
        assert_eq!(blueprint.outputs, vec![]);
        assert_eq!(blueprint.num_threads, 2);
        assert_eq!(blueprint.merge_type, MergeType::Reduce);
    }
}
