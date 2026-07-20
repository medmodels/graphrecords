use super::stats::Stats;
pub use graphrecords_macros::PhaseLabel;
use std::{
    any::Any,
    fmt::{self, Debug, Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

pub trait PhaseLabel: Any + Send + Sync {
    fn dyn_eq(&self, other: &dyn PhaseLabel) -> bool;

    fn dyn_hash(&self, state: &mut dyn Hasher);

    fn dyn_debug(&self, formatter: &mut Formatter<'_>) -> fmt::Result;

    fn as_any(&self) -> &dyn Any;
}

pub struct PhaseId(Arc<dyn PhaseLabel>);

impl PhaseId {
    #[must_use]
    pub fn new(label: impl PhaseLabel) -> Self {
        Self(Arc::new(label))
    }
}

impl Clone for PhaseId {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl PartialEq for PhaseId {
    fn eq(&self, other: &Self) -> bool {
        self.0.dyn_eq(other.0.as_ref())
    }
}

impl Eq for PhaseId {}

impl Hash for PhaseId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.dyn_hash(state);
    }
}

impl Debug for PhaseId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.0.dyn_debug(formatter)
    }
}

pub const DEFAULT_MAX_ITERATIONS: usize = 100;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FixpointPolicy {
    Once,
    Fixpoint { max_iterations: usize },
}

impl FixpointPolicy {
    #[must_use]
    pub const fn once() -> Self {
        Self::Once
    }

    #[must_use]
    pub const fn fixpoint() -> Self {
        Self::Fixpoint {
            max_iterations: DEFAULT_MAX_ITERATIONS,
        }
    }

    #[must_use]
    pub const fn fixpoint_with(max_iterations: usize) -> Self {
        Self::Fixpoint { max_iterations }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum StopReason {
    CompletedOnce,
    Converged { iterations: usize },
    Oscillation { iterations: usize },
    IterationLimit { iterations: usize },
    Skipped,
    Empty,
}

#[derive(Clone, Debug)]
pub struct PhaseOutcome {
    pub label: PhaseId,
    pub stop: StopReason,
}

#[derive(Clone, Debug, Default)]
pub struct OptimizationReport {
    pub phases: Vec<PhaseOutcome>,
}

impl OptimizationReport {
    #[must_use]
    pub const fn display(&self) -> ReportDisplay<'_> {
        ReportDisplay(self)
    }
}

pub struct ReportDisplay<'a>(&'a OptimizationReport);

impl Display for ReportDisplay<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        for (index, outcome) in self.0.phases.iter().enumerate() {
            if index > 0 {
                writeln!(formatter)?;
            }

            write!(formatter, "{:?}: ", outcome.label)?;

            match &outcome.stop {
                StopReason::Converged { iterations } => {
                    write!(formatter, "converged ({iterations} iterations)")?;
                }
                StopReason::CompletedOnce => formatter.write_str("completed once")?,
                StopReason::Oscillation { iterations } => {
                    write!(formatter, "oscillation after {iterations} iterations")?;
                }
                StopReason::IterationLimit { iterations } => {
                    write!(formatter, "did not converge within {iterations} iterations")?;
                }
                StopReason::Skipped => formatter.write_str("skipped (run condition false)")?,
                StopReason::Empty => formatter.write_str("skipped (no rules)")?,
            }
        }

        Ok(())
    }
}

pub(super) type RunCondition = Box<dyn Fn(&Stats<'_>) -> bool + Send + Sync>;
