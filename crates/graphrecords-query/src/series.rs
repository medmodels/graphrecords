use crate::{
    EdgesExpression, EvaluateExpression, Explain, ExplainFormatter, Expression, GroupsExpression,
    NodesExpression, QueryResult, ReturnExpression, Unordered,
    execution::EvaluationCache,
    explain::write_truncated,
    expressions::{AllEdges, AllGroups, AllNodes},
    optimizer::{
        Estimate, Estimated, OptimizationReport, Optimizer, PlanIdentity, PlanInputs, Stats,
    },
    returns::QueryExplanation,
};
use graphrecords_core::{GraphRecord, StateView};
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::{Arc, OnceLock},
};

pub type NodesSeries = Series<NodesExpression<Unordered>>;
pub type EdgesSeries = Series<EdgesExpression<Unordered>>;
pub type GroupsSeries = Series<GroupsExpression<Unordered>>;

pub struct Series<E> {
    graphrecord: GraphRecord,
    expression: E,
    optimizer: Arc<Optimizer>,
    optimized: OnceLock<Optimized<E>>,
}

struct Optimized<E> {
    expression: E,
    report: OptimizationReport,
    cache: EvaluationCache,
}

impl<E1> Series<E1> {
    const fn new(graphrecord: GraphRecord, expression: E1, optimizer: Arc<Optimizer>) -> Self {
        Self {
            graphrecord,
            expression,
            optimizer,
            optimized: OnceLock::new(),
        }
    }

    pub const fn expression(&self) -> &E1 {
        &self.expression
    }

    pub(crate) const fn graphrecord(&self) -> &GraphRecord {
        &self.graphrecord
    }

    pub fn bind<E2>(&self, expression: E2) -> Series<E2> {
        Series::new(
            self.graphrecord.clone(),
            expression,
            Arc::clone(&self.optimizer),
        )
    }
}

impl<E: Clone> Clone for Series<E> {
    fn clone(&self) -> Self {
        self.bind(self.expression.clone())
    }
}

impl<E: ReturnExpression> Series<E> {
    fn optimized(&self) -> &Optimized<E> {
        self.optimized.get_or_init(|| {
            let (expression, report) = if self.optimizer.is_empty() {
                (self.expression.clone(), OptimizationReport::default())
            } else {
                let stats = Stats::new(&self.graphrecord);

                self.expression.clone().optimize(&self.optimizer, &stats)
            };

            Optimized {
                expression,
                report,
                cache: EvaluationCache::new(&self.graphrecord),
            }
        })
    }

    pub fn evaluate(&self) -> QueryResult<E::ReturnValue<'_>> {
        let optimized = self.optimized();

        optimized
            .expression
            .evaluate(&self.graphrecord, &optimized.cache)
    }

    pub fn explain(&self) -> QueryExplanation<'_, E> {
        let optimized = self.optimized();

        QueryExplanation {
            expression: &optimized.expression,
            report: Some(&optimized.report),
        }
    }

    pub const fn explain_unoptimized(&self) -> QueryExplanation<'_, E> {
        QueryExplanation {
            expression: &self.expression,
            report: None,
        }
    }
}

impl<E: ReturnExpression + EvaluateExpression> Series<E> {
    pub(crate) fn elements(&self) -> QueryResult<<E as EvaluateExpression>::ReturnValue<'_>> {
        let optimized = self.optimized();

        EvaluateExpression::evaluate(&optimized.expression, &self.graphrecord, &optimized.cache)
    }
}

impl<E: Expression> PlanIdentity for Series<E> {
    fn identity_eq(&self, other: &Self) -> bool {
        StateView::of(&self.graphrecord).state_identity()
            == StateView::of(&other.graphrecord).state_identity()
            && self.expression.identity_eq(&other.expression)
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        StateView::of(&self.graphrecord)
            .state_identity()
            .hash(state);
        self.expression.identity_hash(state);
    }
}

impl<E: Clone> PlanInputs for Series<E> {}

impl<E: ReturnExpression + Expression> Explain for Series<E> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        self.optimized().expression.describe(formatter)
    }
}

impl<E: ReturnExpression + Estimated> Estimated for Series<E> {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        let stats = Stats::new(&self.graphrecord);

        self.optimized().expression.estimate(&stats)
    }
}

impl<E: ReturnExpression> fmt::Debug for Series<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Series [")?;
        write_truncated(formatter, &self.expression.compact_plan())?;
        formatter.write_str("]")?;

        if self.optimized.get().is_some() {
            formatter.write_str(" optimized")?;
        }

        Ok(())
    }
}

pub trait Queryable {
    fn nodes(&self) -> NodesSeries;

    fn edges(&self) -> EdgesSeries;

    fn groups(&self) -> GroupsSeries;

    fn query<E: ReturnExpression>(&self, expression: E) -> Series<E>;
}

impl Queryable for GraphRecord {
    fn nodes(&self) -> NodesSeries {
        self.query(NodesExpression::new(AllNodes))
    }

    fn edges(&self) -> EdgesSeries {
        self.query(EdgesExpression::new(AllEdges))
    }

    fn groups(&self) -> GroupsSeries {
        self.query(GroupsExpression::new(AllGroups))
    }

    fn query<E: ReturnExpression>(&self, expression: E) -> Series<E> {
        Series::new(self.clone(), expression, Arc::clone(Optimizer::builtin()))
    }
}
