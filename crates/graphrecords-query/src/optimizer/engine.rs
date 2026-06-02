use super::{
    cost::Stats,
    phase::{
        FixpointPolicy, OptimizationReport, PhaseId, PhaseLabel, PhaseOutcome, RunCondition,
        StopReason,
    },
    plan::OptimizeInputs,
    rule::{ErasedRule, Rule, Transformed},
};
use crate::Operand;
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
use std::{
    any::{Any, TypeId, type_name},
    collections::hash_map::DefaultHasher,
    error::Error,
    fmt::{self, Display, Formatter},
    hash::Hasher,
};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Direction {
    TopDown,
    BottomUp,
    Manual,
}

const DIRECTION_ORDER: [Direction; 3] =
    [Direction::TopDown, Direction::BottomUp, Direction::Manual];

#[derive(Debug)]
pub enum OptimizerError {
    PhaseCycle(Vec<PhaseId>),
    RuleCycle(Vec<&'static str>),
    NonExcludable(&'static str),
}

impl Display for OptimizerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::PhaseCycle(labels) => write!(
                formatter,
                "optimizer phase ordering has a cycle involving {labels:?}"
            ),
            Self::RuleCycle(names) => write!(
                formatter,
                "optimizer rule ordering has a cycle involving {names:?}"
            ),
            Self::NonExcludable(name) => write!(
                formatter,
                "optimizer rule `{name}` is marked non-excludable and cannot be excluded"
            ),
        }
    }
}

impl Error for OptimizerError {}

struct RuleEntry {
    identity: TypeId,
    name: &'static str,
    operand_type: TypeId,
    direction: Option<Direction>,
    before: Vec<TypeId>,
    after: Vec<TypeId>,
    excludable: bool,
    run_if: Option<RunCondition>,
    rule: Box<dyn Any>,
}

struct Phase {
    id: PhaseId,
    direction: Direction,
    policy: FixpointPolicy,
    run_if: Option<RunCondition>,
    before: Vec<PhaseId>,
    after: Vec<PhaseId>,
    rules: Vec<RuleEntry>,
}

pub struct Optimizer {
    phases: Vec<Phase>,
    excluded: GrHashSet<TypeId>,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    #[must_use]
    pub fn new() -> Self {
        Self {
            phases: Vec::new(),
            excluded: GrHashSet::default(),
        }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.phases.is_empty()
    }

    pub fn add_phase(&mut self, label: impl PhaseLabel) -> PhaseHandle<'_> {
        let index = self.phase_entry(PhaseId::new(label));
        PhaseHandle {
            optimizer: self,
            index,
        }
    }

    pub fn add_rule<O, R>(&mut self, phase: impl PhaseLabel, rule: R) -> RuleHandle<'_>
    where
        O: Operand + 'static,
        R: Rule<O>,
    {
        let phase_index = self.phase_entry(PhaseId::new(phase));

        let erased: ErasedRule<O> =
            Box::new(move |operand, session| rule.apply(operand, session.stats()));

        let phase = &mut self.phases[phase_index];
        phase.rules.push(RuleEntry {
            identity: TypeId::of::<R>(),
            name: type_name::<R>(),
            operand_type: TypeId::of::<O>(),
            direction: None,
            before: Vec::new(),
            after: Vec::new(),
            excludable: true,
            run_if: None,
            rule: Box::new(erased),
        });

        let rule_index = phase.rules.len() - 1;

        RuleHandle {
            optimizer: self,
            phase_index,
            rule_index,
        }
    }

    pub fn exclude<R: 'static>(&mut self) -> &mut Self {
        self.excluded.insert(TypeId::of::<R>());
        self
    }

    fn phase_entry(&mut self, id: PhaseId) -> usize {
        if let Some(index) = self.phases.iter().position(|phase| phase.id == id) {
            return index;
        }

        self.phases.push(Phase {
            id,
            direction: Direction::BottomUp,
            policy: FixpointPolicy::fixpoint(),
            run_if: None,
            before: Vec::new(),
            after: Vec::new(),
            rules: Vec::new(),
        });
        self.phases.len() - 1
    }

    pub fn validate(&self) -> Result<(), OptimizerError> {
        self.ordered_phase_indices().map_err(|cycle| {
            let labels = cycle
                .iter()
                .map(|&index| self.phases[index].id.clone())
                .collect();

            OptimizerError::PhaseCycle(labels)
        })?;

        for phase in &self.phases {
            for entry in &phase.rules {
                if !entry.excludable && self.excluded.contains(&entry.identity) {
                    return Err(OptimizerError::NonExcludable(entry.name));
                }
            }

            for groups in self.group_rules(phase).values() {
                for entries in groups.values() {
                    rule_order(entries).map_err(|cycle| {
                        let names = cycle.iter().map(|&index| entries[index].name).collect();

                        OptimizerError::RuleCycle(names)
                    })?;
                }
            }
        }

        Ok(())
    }

    pub fn run<'a, O: Operand + Clone + 'static>(&'a self, stats: &'a Stats<'a>, root: &O) -> O {
        self.run_reported(stats, root).0
    }

    pub fn run_reported<'a, O: Operand + Clone + 'static>(
        &'a self,
        stats: &'a Stats<'a>,
        root: &O,
    ) -> (O, OptimizationReport) {
        let order = self.resolve_phase_order();
        let mut current = root.clone();
        let mut phases = Vec::with_capacity(order.len());

        for &phase_index in &order {
            let phase = &self.phases[phase_index];
            let (next, stop) = self.run_phase(phase, stats, current);

            current = next;

            phases.push(PhaseOutcome {
                label: phase.id.clone(),
                stop,
            });
        }

        (current, OptimizationReport { phases })
    }

    fn run_phase<O: Operand + Clone + 'static>(
        &self,
        phase: &Phase,
        stats: &Stats,
        current: O,
    ) -> (O, StopReason) {
        if phase
            .run_if
            .as_ref()
            .is_some_and(|condition| !condition(stats))
        {
            return (current, StopReason::Skipped);
        }

        let passes = self.compile_phase(phase, stats);
        if passes.is_empty() {
            return (current, StopReason::Empty);
        }

        match phase.policy {
            FixpointPolicy::Once => {
                let (current, _changed) = apply_passes(&passes, current, stats);
                (current, StopReason::CompletedOnce)
            }
            FixpointPolicy::Fixpoint { max_iterations } => {
                let mut current = current;
                let mut seen: GrHashMap<u64, Vec<O>> = GrHashMap::default();

                seen.entry(signature(&current))
                    .or_default()
                    .push(current.clone());

                for iteration in 1..=max_iterations {
                    let (next, changed) = apply_passes(&passes, current, stats);
                    current = next;

                    if !changed {
                        return (
                            current,
                            StopReason::Converged {
                                iterations: iteration,
                            },
                        );
                    }

                    let bucket = seen.entry(signature(&current)).or_default();
                    if bucket
                        .iter()
                        .any(|plan| current.as_plan_node().dyn_eq(plan.as_plan_node()))
                    {
                        return (
                            current,
                            StopReason::Oscillation {
                                iterations: iteration,
                            },
                        );
                    }

                    bucket.push(current.clone());
                }

                (
                    current,
                    StopReason::IterationLimit {
                        iterations: max_iterations,
                    },
                )
            }
        }
    }

    fn compile_phase<'p>(&self, phase: &'p Phase, stats: &Stats) -> Vec<Pass<'p>> {
        let mut by_direction = self.group_rules(phase);

        DIRECTION_ORDER
            .into_iter()
            .filter_map(|direction| {
                let mut rules = by_direction.remove(&direction)?;

                rules.retain(|_, entries| {
                    entries.retain(|entry| {
                        entry
                            .run_if
                            .as_ref()
                            .is_none_or(|condition| condition(stats))
                    });

                    !entries.is_empty()
                });

                if rules.is_empty() {
                    return None;
                }

                for entries in rules.values_mut() {
                    let order =
                        rule_order(entries).unwrap_or_else(|_| (0..entries.len()).collect());
                    *entries = order.into_iter().map(|index| entries[index]).collect();
                }

                Some(Pass { direction, rules })
            })
            .collect()
    }

    fn group_rules<'p>(
        &self,
        phase: &'p Phase,
    ) -> GrHashMap<Direction, GrHashMap<TypeId, Vec<&'p RuleEntry>>> {
        let mut groups: GrHashMap<Direction, GrHashMap<TypeId, Vec<&RuleEntry>>> =
            GrHashMap::default();

        for entry in &phase.rules {
            if entry.excludable && self.excluded.contains(&entry.identity) {
                continue;
            }

            let direction = entry.direction.unwrap_or(phase.direction);
            groups
                .entry(direction)
                .or_default()
                .entry(entry.operand_type)
                .or_default()
                .push(entry);
        }

        groups
    }

    fn resolve_phase_order(&self) -> Vec<usize> {
        self.ordered_phase_indices()
            .unwrap_or_else(|_| (0..self.phases.len()).collect())
    }

    fn ordered_phase_indices(&self) -> Result<Vec<usize>, Vec<usize>> {
        let mut edges = Vec::new();

        for (index, phase) in self.phases.iter().enumerate() {
            for id in &phase.before {
                if let Some(other) = self.phases.iter().position(|phase| &phase.id == id) {
                    edges.push((index, other));
                }
            }

            for id in &phase.after {
                if let Some(other) = self.phases.iter().position(|phase| &phase.id == id) {
                    edges.push((other, index));
                }
            }
        }

        toposort(self.phases.len(), &edges)
    }
}

pub struct PhaseHandle<'a> {
    optimizer: &'a mut Optimizer,
    index: usize,
}

impl PhaseHandle<'_> {
    fn phase(&mut self) -> &mut Phase {
        &mut self.optimizer.phases[self.index]
    }

    pub fn direction(&mut self, direction: Direction) -> &mut Self {
        self.phase().direction = direction;
        self
    }

    pub fn policy(&mut self, policy: FixpointPolicy) -> &mut Self {
        self.phase().policy = policy;
        self
    }

    pub fn once(&mut self) -> &mut Self {
        self.policy(FixpointPolicy::Once)
    }

    pub fn fixpoint(&mut self) -> &mut Self {
        self.policy(FixpointPolicy::fixpoint())
    }

    pub fn before(&mut self, label: impl PhaseLabel) -> &mut Self {
        self.phase().before.push(PhaseId::new(label));
        self
    }

    pub fn after(&mut self, label: impl PhaseLabel) -> &mut Self {
        self.phase().after.push(PhaseId::new(label));
        self
    }

    pub fn run_if(&mut self, condition: impl Fn(&Stats<'_>) -> bool + 'static) -> &mut Self {
        self.phase().run_if = Some(Box::new(condition));
        self
    }
}

pub struct RuleHandle<'a> {
    optimizer: &'a mut Optimizer,
    phase_index: usize,
    rule_index: usize,
}

impl RuleHandle<'_> {
    fn entry(&mut self) -> &mut RuleEntry {
        &mut self.optimizer.phases[self.phase_index].rules[self.rule_index]
    }

    pub fn direction(&mut self, direction: Direction) -> &mut Self {
        self.entry().direction = Some(direction);
        self
    }

    pub fn label<L: 'static>(&mut self) -> &mut Self {
        let entry = self.entry();

        entry.identity = TypeId::of::<L>();
        entry.name = type_name::<L>();

        self
    }

    pub fn before<R: 'static>(&mut self) -> &mut Self {
        self.entry().before.push(TypeId::of::<R>());
        self
    }

    pub fn after<R: 'static>(&mut self) -> &mut Self {
        self.entry().after.push(TypeId::of::<R>());
        self
    }

    pub fn non_excludable(&mut self) -> &mut Self {
        self.entry().excludable = false;
        self
    }

    pub fn run_if(&mut self, condition: impl Fn(&Stats<'_>) -> bool + 'static) -> &mut Self {
        self.entry().run_if = Some(Box::new(condition));
        self
    }
}

struct Pass<'a> {
    direction: Direction,
    rules: GrHashMap<TypeId, Vec<&'a RuleEntry>>,
}

fn apply_passes<O: Operand + Clone + 'static>(
    passes: &[Pass],
    mut current: O,
    stats: &Stats,
) -> (O, bool) {
    let mut changed = false;

    for pass in passes {
        let session = Session {
            rules: &pass.rules,
            direction: pass.direction,
            stats,
        };
        let transformed = session.optimize(&current);

        current = transformed.value;
        changed |= transformed.changed;
    }

    (current, changed)
}

pub struct Session<'a> {
    rules: &'a GrHashMap<TypeId, Vec<&'a RuleEntry>>,
    direction: Direction,
    stats: &'a Stats<'a>,
}

impl Session<'_> {
    #[must_use]
    pub const fn stats(&self) -> &Stats<'_> {
        self.stats
    }

    pub fn optimize<O: Operand + Clone + 'static>(&self, operand: &O) -> Transformed<O> {
        match self.direction {
            Direction::BottomUp => {
                let rebuilt = operand.context().optimize_inputs(operand, self);
                let applied = self.apply_rules(rebuilt.value);

                Transformed {
                    changed: rebuilt.changed || applied.changed,
                    value: applied.value,
                }
            }
            Direction::TopDown => {
                let rewritten = self.apply_rules(operand.clone());
                let rebuilt = rewritten
                    .value
                    .context()
                    .optimize_inputs(&rewritten.value, self);

                Transformed {
                    changed: rewritten.changed || rebuilt.changed,
                    value: rebuilt.value,
                }
            }
            Direction::Manual => self.apply_rules(operand.clone()),
        }
    }

    fn apply_rules<O: Operand + 'static>(&self, mut operand: O) -> Transformed<O> {
        let Some(entries) = self.rules.get(&TypeId::of::<O>()) else {
            return Transformed::unchanged(operand);
        };

        let mut changed = false;

        for entry in entries {
            let rule = entry
                .rule
                .downcast_ref::<ErasedRule<O>>()
                .expect("Rule entry must hold an erased rule matching its operand bucket");
            let transformed = rule(operand, self);

            operand = transformed.value;
            changed |= transformed.changed;
        }

        Transformed {
            value: operand,
            changed,
        }
    }
}

fn signature<O: Operand>(operand: &O) -> u64 {
    let mut hasher = DefaultHasher::new();
    operand.as_plan_node().dyn_hash(&mut hasher);
    hasher.finish()
}

fn rule_order(entries: &[&RuleEntry]) -> Result<Vec<usize>, Vec<usize>> {
    let mut edges = Vec::new();

    for (index, entry) in entries.iter().enumerate() {
        for target in &entry.before {
            if let Some(other) = entries.iter().position(|entry| entry.identity == *target) {
                edges.push((index, other));
            }
        }

        for target in &entry.after {
            if let Some(other) = entries.iter().position(|entry| entry.identity == *target) {
                edges.push((other, index));
            }
        }
    }

    toposort(entries.len(), &edges)
}

fn toposort(count: usize, edges: &[(usize, usize)]) -> Result<Vec<usize>, Vec<usize>> {
    let mut indegree = vec![0usize; count];
    let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); count];

    for &(before, after) in edges {
        adjacency[before].push(after);
        indegree[after] += 1;
    }

    let mut ready: Vec<usize> = (0..count).filter(|&index| indegree[index] == 0).collect();
    let mut order = Vec::with_capacity(count);

    while !ready.is_empty() {
        let mut position = 0;
        for candidate in 1..ready.len() {
            if ready[candidate] < ready[position] {
                position = candidate;
            }
        }

        let index = ready.swap_remove(position);
        order.push(index);

        for &next in &adjacency[index] {
            indegree[next] -= 1;
            if indegree[next] == 0 {
                ready.push(next);
            }
        }
    }

    if order.len() == count {
        Ok(order)
    } else {
        Err((0..count).filter(|index| !order.contains(index)).collect())
    }
}
