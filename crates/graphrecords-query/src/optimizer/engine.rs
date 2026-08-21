use super::{
    phase::{
        FixpointPolicy, OptimizationReport, PhaseId, PhaseLabel, PhaseOutcome, RunCondition,
        StopReason,
    },
    rule::{ErasedRule, Rule, Transformed},
    stats::Stats,
};
use crate::Expression;
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
#[non_exhaustive]
pub struct OptimizerError {
    misconfigurations: Vec<Misconfiguration>,
}

impl OptimizerError {
    #[must_use]
    pub const fn new(misconfigurations: Vec<Misconfiguration>) -> Self {
        Self { misconfigurations }
    }

    #[must_use]
    pub fn misconfigurations(&self) -> &[Misconfiguration] {
        &self.misconfigurations
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum Misconfiguration {
    DuplicatePhase(PhaseId),
    UnknownPhase {
        phase: PhaseId,
        rule: &'static str,
    },
    UnknownPhaseReference {
        phase: PhaseId,
        reference: PhaseId,
    },
    UnknownRuleReference {
        phase: PhaseId,
        rule: &'static str,
        reference: &'static str,
        registered_elsewhere: Option<PhaseId>,
    },
    UnknownExclusion(&'static str),
    NonExcludable(&'static str),
    PhaseCycle(Vec<PhaseId>),
    RuleCycle {
        phase: PhaseId,
        rules: Vec<&'static str>,
    },
}

impl Display for Misconfiguration {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicatePhase(phase) => {
                write!(formatter, "phase {phase:?} is declared more than once")
            }
            Self::UnknownPhase { phase, rule } => write!(
                formatter,
                "rule `{rule}` targets phase {phase:?}, which is never declared"
            ),
            Self::UnknownPhaseReference { phase, reference } => write!(
                formatter,
                "phase {phase:?} orders against phase {reference:?}, which is never declared"
            ),
            Self::UnknownRuleReference {
                phase,
                rule,
                reference,
                registered_elsewhere,
            } => {
                write!(
                    formatter,
                    "rule `{rule}` in phase {phase:?} orders against `{reference}`, which is "
                )?;

                match registered_elsewhere {
                    Some(other) => write!(
                        formatter,
                        "not registered in this phase (it is registered in phase {other:?} instead)"
                    ),
                    None => formatter.write_str("never registered"),
                }
            }
            Self::UnknownExclusion(name) => write!(
                formatter,
                "exclusion targets rule `{name}`, which is never registered"
            ),
            Self::NonExcludable(name) => write!(
                formatter,
                "rule `{name}` is marked non-excludable and cannot be excluded"
            ),
            Self::PhaseCycle(labels) => {
                write!(formatter, "phase ordering has a cycle involving {labels:?}")
            }
            Self::RuleCycle { phase, rules } => write!(
                formatter,
                "rule ordering in phase {phase:?} has a cycle involving {rules:?}"
            ),
        }
    }
}

impl Display for OptimizerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("optimizer configuration is invalid:")?;

        for misconfiguration in &self.misconfigurations {
            write!(formatter, "\n- {misconfiguration}")?;
        }

        Ok(())
    }
}

impl Error for OptimizerError {}

struct RuleEntry {
    identity: TypeId,
    name: &'static str,
    expression_type: TypeId,
    direction: Option<Direction>,
    before: Vec<RuleIdentity>,
    after: Vec<RuleIdentity>,
    excludable: bool,
    run_if: Option<RunCondition>,
    rule: Box<dyn Any + Send + Sync>,
}

struct PendingRule {
    phase: PhaseId,
    entry: RuleEntry,
}

struct RuleIdentity {
    identity: TypeId,
    name: &'static str,
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

struct CompiledPhase {
    id: PhaseId,
    policy: FixpointPolicy,
    run_if: Option<RunCondition>,
    passes: Vec<CompiledPass>,
}

struct CompiledPass {
    direction: Direction,
    buckets: GrHashMap<TypeId, Vec<CompiledRule>>,
    has_run_conditions: bool,
}

struct CompiledRule {
    run_if: Option<RunCondition>,
    rule: Box<dyn Any + Send + Sync>,
}

pub struct OptimizerBuilder {
    phases: Vec<Phase>,
    rules: Vec<PendingRule>,
    exclusions: Vec<RuleIdentity>,
}

impl Default for OptimizerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerBuilder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            phases: Vec::new(),
            rules: Vec::new(),
            exclusions: Vec::new(),
        }
    }

    pub fn add_phase(&mut self, label: impl PhaseLabel) -> PhaseHandle<'_> {
        self.phases.push(Phase {
            id: PhaseId::new(label),
            direction: Direction::BottomUp,
            policy: FixpointPolicy::fixpoint(),
            run_if: None,
            before: Vec::new(),
            after: Vec::new(),
            rules: Vec::new(),
        });

        let index = self.phases.len() - 1;

        PhaseHandle {
            builder: self,
            index,
        }
    }

    pub fn add_rule<E: Expression + 'static, R: Rule<E>>(
        &mut self,
        phase: impl PhaseLabel,
        rule: R,
    ) -> RuleHandle<'_> {
        let erased: ErasedRule<_> =
            Box::new(move |expression, session| rule.apply(expression, session.stats()));

        self.rules.push(PendingRule {
            phase: PhaseId::new(phase),
            entry: RuleEntry {
                identity: TypeId::of::<R>(),
                name: type_name::<R>(),
                expression_type: TypeId::of::<E>(),
                direction: None,
                before: Vec::new(),
                after: Vec::new(),
                excludable: true,
                run_if: None,
                rule: Box::new(erased),
            },
        });

        let index = self.rules.len() - 1;

        RuleHandle {
            builder: self,
            index,
        }
    }

    pub fn exclude<R: 'static>(&mut self) -> &mut Self {
        self.exclusions.push(RuleIdentity {
            identity: TypeId::of::<R>(),
            name: type_name::<R>(),
        });
        self
    }

    pub fn build(self) -> Result<Optimizer, OptimizerError> {
        let (phase_index, duplicates) = index_phases(&self.phases);
        let (phases, unknown_phases) = join_rules(self.phases, self.rules, &phase_index);
        let (order, ordering_findings) = phase_order(&phases, &phase_index);
        let reference_findings = validate_rule_references(&phases);
        let (excluded, exclusion_findings) = resolve_exclusions(self.exclusions, &phases);
        let (compiled_phases, cycle_findings) = compile_phases(phases, order, &excluded);

        let misconfigurations: Vec<_> = duplicates
            .into_iter()
            .chain(unknown_phases)
            .chain(ordering_findings)
            .chain(reference_findings)
            .chain(exclusion_findings)
            .chain(cycle_findings)
            .collect();

        if !misconfigurations.is_empty() {
            return Err(OptimizerError::new(misconfigurations));
        }

        Ok(Optimizer {
            phases: compiled_phases,
        })
    }
}

pub struct PhaseHandle<'a> {
    builder: &'a mut OptimizerBuilder,
    index: usize,
}

impl PhaseHandle<'_> {
    fn phase(&mut self) -> &mut Phase {
        &mut self.builder.phases[self.index]
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

    pub fn run_if(
        &mut self,
        condition: impl Fn(&Stats<'_>) -> bool + Send + Sync + 'static,
    ) -> &mut Self {
        self.phase().run_if = Some(Box::new(condition));
        self
    }
}

pub struct RuleHandle<'a> {
    builder: &'a mut OptimizerBuilder,
    index: usize,
}

impl RuleHandle<'_> {
    fn entry(&mut self) -> &mut RuleEntry {
        &mut self.builder.rules[self.index].entry
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
        self.entry().before.push(RuleIdentity {
            identity: TypeId::of::<R>(),
            name: type_name::<R>(),
        });
        self
    }

    pub fn after<R: 'static>(&mut self) -> &mut Self {
        self.entry().after.push(RuleIdentity {
            identity: TypeId::of::<R>(),
            name: type_name::<R>(),
        });
        self
    }

    pub fn non_excludable(&mut self) -> &mut Self {
        self.entry().excludable = false;
        self
    }

    pub fn run_if(
        &mut self,
        condition: impl Fn(&Stats<'_>) -> bool + Send + Sync + 'static,
    ) -> &mut Self {
        self.entry().run_if = Some(Box::new(condition));
        self
    }
}

fn index_phases(phases: &[Phase]) -> (GrHashMap<PhaseId, usize>, Vec<Misconfiguration>) {
    let mut index = GrHashMap::default();
    let mut duplicates = Vec::new();

    for (position, phase) in phases.iter().enumerate() {
        if index.contains_key(&phase.id) {
            duplicates.push(Misconfiguration::DuplicatePhase(phase.id.clone()));
            continue;
        }

        index.insert(phase.id.clone(), position);
    }

    (index, duplicates)
}

fn join_rules(
    mut phases: Vec<Phase>,
    rules: Vec<PendingRule>,
    phase_index: &GrHashMap<PhaseId, usize>,
) -> (Vec<Phase>, Vec<Misconfiguration>) {
    let mut unknown_phases = Vec::new();

    for pending in rules {
        match phase_index.get(&pending.phase) {
            Some(&position) => phases[position].rules.push(pending.entry),
            None => unknown_phases.push(Misconfiguration::UnknownPhase {
                phase: pending.phase,
                rule: pending.entry.name,
            }),
        }
    }

    (phases, unknown_phases)
}

fn phase_order(
    phases: &[Phase],
    phase_index: &GrHashMap<PhaseId, usize>,
) -> (Option<Vec<usize>>, Vec<Misconfiguration>) {
    let mut edges = Vec::new();
    let mut findings = Vec::new();

    for (position, phase) in phases.iter().enumerate() {
        for reference in &phase.before {
            match phase_index.get(reference) {
                Some(&target) => edges.push((position, target)),
                None => findings.push(Misconfiguration::UnknownPhaseReference {
                    phase: phase.id.clone(),
                    reference: reference.clone(),
                }),
            }
        }

        for reference in &phase.after {
            match phase_index.get(reference) {
                Some(&target) => edges.push((target, position)),
                None => findings.push(Misconfiguration::UnknownPhaseReference {
                    phase: phase.id.clone(),
                    reference: reference.clone(),
                }),
            }
        }
    }

    match toposort(phases.len(), &edges) {
        Ok(order) => (Some(order), findings),
        Err(cycle) => {
            findings.push(Misconfiguration::PhaseCycle(
                cycle
                    .into_iter()
                    .map(|position| phases[position].id.clone())
                    .collect(),
            ));

            (None, findings)
        }
    }
}

fn validate_rule_references(phases: &[Phase]) -> Vec<Misconfiguration> {
    let mut findings = Vec::new();

    for phase in phases {
        for (entry_index, entry) in phase.rules.iter().enumerate() {
            for reference in entry.before.iter().chain(&entry.after) {
                let registered_here = phase.rules.iter().enumerate().any(|(other_index, other)| {
                    other_index != entry_index && other.identity == reference.identity
                });

                if registered_here {
                    continue;
                }

                let registered_elsewhere = phases
                    .iter()
                    .find(|other| {
                        other.id != phase.id
                            && other
                                .rules
                                .iter()
                                .any(|candidate| candidate.identity == reference.identity)
                    })
                    .map(|other| other.id.clone());

                findings.push(Misconfiguration::UnknownRuleReference {
                    phase: phase.id.clone(),
                    rule: entry.name,
                    reference: reference.name,
                    registered_elsewhere,
                });
            }
        }
    }

    findings
}

fn resolve_exclusions(
    exclusions: Vec<RuleIdentity>,
    phases: &[Phase],
) -> (GrHashSet<TypeId>, Vec<Misconfiguration>) {
    let mut excluded = GrHashSet::default();
    let mut findings = Vec::new();

    for exclusion in exclusions {
        if !excluded.insert(exclusion.identity) {
            continue;
        }

        let matches: Vec<_> = phases
            .iter()
            .flat_map(|phase| &phase.rules)
            .filter(|entry| entry.identity == exclusion.identity)
            .collect();

        if matches.is_empty() {
            findings.push(Misconfiguration::UnknownExclusion(exclusion.name));
        }

        if matches.iter().any(|entry| !entry.excludable) {
            findings.push(Misconfiguration::NonExcludable(exclusion.name));
        }
    }

    (excluded, findings)
}

fn compile_phases(
    phases: Vec<Phase>,
    order: Option<Vec<usize>>,
    excluded: &GrHashSet<TypeId>,
) -> (Vec<CompiledPhase>, Vec<Misconfiguration>) {
    let execution_order = order.unwrap_or_else(|| (0..phases.len()).collect());
    let mut slots: Vec<_> = phases.into_iter().map(Some).collect();

    let (compiled_phases, findings): (Vec<_>, Vec<_>) = execution_order
        .into_iter()
        .map(|index| {
            let phase = slots[index]
                .take()
                .expect("Each phase must appear exactly once in the execution order");

            compile_phase(phase, excluded)
        })
        .unzip();

    (compiled_phases, findings.into_iter().flatten().collect())
}

fn compile_phase(
    phase: Phase,
    excluded: &GrHashSet<TypeId>,
) -> (CompiledPhase, Vec<Misconfiguration>) {
    let Phase {
        id,
        direction: phase_direction,
        policy,
        run_if,
        rules,
        ..
    } = phase;

    let mut by_direction: GrHashMap<_, GrHashMap<TypeId, Vec<_>>> = GrHashMap::default();

    for entry in rules {
        if entry.excludable && excluded.contains(&entry.identity) {
            continue;
        }

        let direction = entry.direction.unwrap_or(phase_direction);
        by_direction
            .entry(direction)
            .or_default()
            .entry(entry.expression_type)
            .or_default()
            .push(entry);
    }

    let mut passes = Vec::new();
    let mut findings = Vec::new();

    for direction in DIRECTION_ORDER {
        let Some(direction_buckets) = by_direction.remove(&direction) else {
            continue;
        };

        let mut buckets = GrHashMap::default();

        for (expression_type, entries) in direction_buckets {
            match order_bucket(&id, entries) {
                Ok(ordered) => {
                    buckets.insert(expression_type, ordered);
                }
                Err(cycle_finding) => findings.push(cycle_finding),
            }
        }

        let has_run_conditions = buckets.values().flatten().any(|rule| rule.run_if.is_some());

        passes.push(CompiledPass {
            direction,
            buckets,
            has_run_conditions,
        });
    }

    (
        CompiledPhase {
            id,
            policy,
            run_if,
            passes,
        },
        findings,
    )
}

fn order_bucket(
    phase_id: &PhaseId,
    entries: Vec<RuleEntry>,
) -> Result<Vec<CompiledRule>, Misconfiguration> {
    let references: Vec<_> = entries.iter().collect();

    let order = match rule_order(&references) {
        Ok(order) => order,
        Err(cycle) => {
            return Err(Misconfiguration::RuleCycle {
                phase: phase_id.clone(),
                rules: cycle.iter().map(|&index| references[index].name).collect(),
            });
        }
    };

    let mut slots: Vec<_> = entries.into_iter().map(Some).collect();

    Ok(order
        .into_iter()
        .map(|index| {
            let entry = slots[index]
                .take()
                .expect("Each rule must appear exactly once in the bucket order");

            CompiledRule {
                run_if: entry.run_if,
                rule: entry.rule,
            }
        })
        .collect())
}

pub struct Optimizer {
    phases: Vec<CompiledPhase>,
}

impl Optimizer {
    #[must_use]
    pub const fn builder() -> OptimizerBuilder {
        OptimizerBuilder::new()
    }

    #[must_use]
    pub const fn none() -> Self {
        Self { phases: Vec::new() }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.phases.is_empty()
    }

    pub fn run<'a, E: Expression + Clone + 'static>(&'a self, stats: &'a Stats<'a>, root: &E) -> E {
        self.run_reported(stats, root).0
    }

    pub fn run_reported<'a, E: Expression + Clone + 'static>(
        &'a self,
        stats: &'a Stats<'a>,
        root: &E,
    ) -> (E, OptimizationReport) {
        let mut current = root.clone();
        let mut phases = Vec::with_capacity(self.phases.len());

        for phase in &self.phases {
            let (next, stop) = Self::run_phase(phase, stats, current);

            current = next;

            phases.push(PhaseOutcome {
                label: phase.id.clone(),
                stop,
            });
        }

        (current, OptimizationReport { phases })
    }

    fn run_phase<E: Expression + Clone + 'static>(
        phase: &CompiledPhase,
        stats: &Stats,
        current: E,
    ) -> (E, StopReason) {
        if phase
            .run_if
            .as_ref()
            .is_some_and(|condition| !condition(stats))
        {
            return (current, StopReason::Skipped);
        }

        if phase.passes.is_empty() {
            return (current, StopReason::Empty);
        }

        let enabled = enabled_rules(&phase.passes, stats);

        if let Some(passes_enabled) = &enabled {
            let any_enabled = passes_enabled.iter().any(|pass_enabled| {
                pass_enabled.as_ref().is_none_or(|buckets_enabled| {
                    buckets_enabled.values().flatten().any(|&enabled| enabled)
                })
            });

            if !any_enabled {
                return (current, StopReason::Empty);
            }
        }

        match phase.policy {
            FixpointPolicy::Once => {
                let current = apply_passes(&phase.passes, enabled.as_deref(), current, stats).0;
                (current, StopReason::CompletedOnce)
            }
            FixpointPolicy::Fixpoint { max_iterations } => {
                let mut current = current;
                let mut seen: GrHashMap<_, Vec<E>> = GrHashMap::default();

                seen.entry(signature(&current))
                    .or_default()
                    .push(current.clone());

                for iteration in 1..=max_iterations {
                    let (next, changed) =
                        apply_passes(&phase.passes, enabled.as_deref(), current, stats);
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
}

type EnabledBuckets = GrHashMap<TypeId, Vec<bool>>;
type EnabledPasses = Vec<Option<EnabledBuckets>>;

fn enabled_rules(passes: &[CompiledPass], stats: &Stats) -> Option<EnabledPasses> {
    if passes.iter().all(|pass| !pass.has_run_conditions) {
        return None;
    }

    Some(
        passes
            .iter()
            .map(|pass| {
                if !pass.has_run_conditions {
                    return None;
                }

                Some(
                    pass.buckets
                        .iter()
                        .map(|(&expression_type, rules)| {
                            (
                                expression_type,
                                rules
                                    .iter()
                                    .map(|rule| {
                                        rule.run_if
                                            .as_ref()
                                            .is_none_or(|condition| condition(stats))
                                    })
                                    .collect(),
                            )
                        })
                        .collect(),
                )
            })
            .collect(),
    )
}

fn apply_passes<E: Expression + Clone + 'static>(
    passes: &[CompiledPass],
    enabled: Option<&[Option<EnabledBuckets>]>,
    mut current: E,
    stats: &Stats,
) -> (E, bool) {
    let mut changed = false;

    for (pass_index, pass) in passes.iter().enumerate() {
        let session = Session {
            buckets: &pass.buckets,
            enabled: enabled.and_then(|passes_enabled| passes_enabled[pass_index].as_ref()),
            direction: pass.direction,
            stats,
        };
        let transformed = session.optimize(&current);
        let (value, was_changed) = transformed.into_parts();

        current = value;
        changed |= was_changed;
    }

    (current, changed)
}

pub struct Session<'a> {
    buckets: &'a GrHashMap<TypeId, Vec<CompiledRule>>,
    enabled: Option<&'a EnabledBuckets>,
    direction: Direction,
    stats: &'a Stats<'a>,
}

impl Session<'_> {
    #[must_use]
    pub const fn stats(&self) -> &Stats<'_> {
        self.stats
    }

    pub fn optimize<E: Expression + Clone + 'static>(&self, expression: &E) -> Transformed<E> {
        match self.direction {
            Direction::TopDown => {
                let (rewritten, rewritten_changed) =
                    self.apply_rules(expression.clone()).into_parts();
                let (rebuilt, rebuilt_changed) =
                    rewritten.context().optimize(&rewritten, self).into_parts();

                if rewritten_changed || rebuilt_changed {
                    Transformed::changed(rebuilt)
                } else {
                    Transformed::unchanged(rebuilt)
                }
            }
            Direction::BottomUp => {
                let (rebuilt, rebuilt_changed) =
                    expression.context().optimize(expression, self).into_parts();
                let (applied, applied_changed) = self.apply_rules(rebuilt).into_parts();

                if rebuilt_changed || applied_changed {
                    Transformed::changed(applied)
                } else {
                    Transformed::unchanged(applied)
                }
            }
            Direction::Manual => self.apply_rules(expression.clone()),
        }
    }

    fn apply_rules<E: Expression + 'static>(&self, mut expression: E) -> Transformed<E> {
        let Some(rules) = self.buckets.get(&TypeId::of::<E>()) else {
            return Transformed::unchanged(expression);
        };

        let bucket_enabled = self
            .enabled
            .and_then(|buckets_enabled| buckets_enabled.get(&TypeId::of::<E>()));

        let mut changed = false;

        for (index, compiled) in rules.iter().enumerate() {
            if bucket_enabled.is_some_and(|enabled| !enabled[index]) {
                continue;
            }

            let rule = compiled
                .rule
                .downcast_ref::<ErasedRule<E>>()
                .expect("Compiled rule must hold an erased rule matching its expression bucket");
            let (value, was_changed) = rule(expression, self).into_parts();

            expression = value;
            changed |= was_changed;
        }

        if changed {
            Transformed::changed(expression)
        } else {
            Transformed::unchanged(expression)
        }
    }
}

fn signature<E: Expression>(expression: &E) -> u64 {
    let mut hasher = DefaultHasher::new();
    expression.as_plan_node().dyn_hash(&mut hasher);
    hasher.finish()
}

fn rule_order(entries: &[&RuleEntry]) -> Result<Vec<usize>, Vec<usize>> {
    let mut edges = Vec::new();

    for (index, entry) in entries.iter().enumerate() {
        for reference in &entry.before {
            for (other, candidate) in entries.iter().enumerate() {
                if other != index && candidate.identity == reference.identity {
                    edges.push((index, other));
                }
            }
        }

        for reference in &entry.after {
            for (other, candidate) in entries.iter().enumerate() {
                if other != index && candidate.identity == reference.identity {
                    edges.push((other, index));
                }
            }
        }
    }

    toposort(entries.len(), &edges)
}

fn toposort(count: usize, edges: &[(usize, usize)]) -> Result<Vec<usize>, Vec<usize>> {
    let mut indegree = vec![0usize; count];
    let mut adjacency = vec![Vec::new(); count];

    for &(before, after) in edges {
        adjacency[before].push(after);
        indegree[after] += 1;
    }

    let mut ready: Vec<_> = (0..count).filter(|&index| indegree[index] == 0).collect();
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
        Err((0..count)
            .filter(|&index| !order.contains(&index) && cycles_back(index, &adjacency))
            .collect())
    }
}

fn cycles_back(start: usize, adjacency: &[Vec<usize>]) -> bool {
    let mut visited = vec![false; adjacency.len()];
    let mut stack = adjacency[start].clone();

    while let Some(node) = stack.pop() {
        if node == start {
            return true;
        }

        if !visited[node] {
            visited[node] = true;
            stack.extend(adjacency[node].iter().copied());
        }
    }

    false
}
