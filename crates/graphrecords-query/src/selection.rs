use crate::{
    Arity, EvaluateOperand, Explanation, IndexDomain, NodesOperand, Operand, QueryResult,
    ReturnShape, Unordered,
    execution::EvaluationCache,
    index::GroupKey,
    operands::{
        AllEdges, AllNodes, EdgesOperand, GroupOperand, OperandHandle, Partition, ReturnPartition,
    },
    optimizer::{OptimizationReport, Optimizer, Stats},
};
use graphrecords_core::GraphRecord;
use std::fmt::{self, Display, Formatter};

pub trait ReturnOperand<'a>: Clone {
    type ReturnValue;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue>;

    #[allow(unused_variables)]
    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport)
    where
        Self: Sized,
    {
        (self, OptimizationReport::default())
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result;
}

pub trait IntoReturn: Operand {
    type Return<'a>: 'a
    where
        Self: 'a;

    fn into_return<'a>(values: Self::ReturnValue<'a>) -> Self::Return<'a>
    where
        Self: 'a;
}

impl<S: ReturnShape, C: Arity> IntoReturn for OperandHandle<S, C> {
    type Return<'a>
        = C::Container<'a, S::ReturnElement<'a>>
    where
        Self: 'a;

    fn into_return<'a>(values: Self::ReturnValue<'a>) -> Self::Return<'a>
    where
        Self: 'a,
    {
        C::map_elements(values, S::into_return_element)
    }
}

impl<M: IndexDomain, K: GroupKey, O: IntoReturn> IntoReturn for GroupOperand<M, K, O> {
    type Return<'a>
        = ReturnPartition<'a, M, K, O::Return<'a>>
    where
        Self: 'a;

    fn into_return<'a>(values: Partition<'a, M, K, O>) -> Self::Return<'a>
    where
        Self: 'a,
    {
        values.into_return_partition(|payload| payload.map(O::into_return))
    }
}

impl<'a, S: ReturnShape, C: Arity> ReturnOperand<'a> for OperandHandle<S, C> {
    type ReturnValue = <Self as IntoReturn>::Return<'a>;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue> {
        let values = EvaluateOperand::evaluate(self, graphrecord, cache)?;

        Ok(Self::into_return(values))
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }
}

impl<'a, M: IndexDomain, K: GroupKey, O: IntoReturn> ReturnOperand<'a> for GroupOperand<M, K, O> {
    type ReturnValue = <Self as IntoReturn>::Return<'a>;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue> {
        let values = EvaluateOperand::evaluate(self, graphrecord, cache)?;

        Ok(Self::into_return(values))
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }
}

macro_rules! impl_return_operand_for_tuples {
    ($($T:ident),+) => {
        impl<'a, $($T: ReturnOperand<'a>),+> ReturnOperand<'a> for ($($T,)+) {
            type ReturnValue = ($($T::ReturnValue,)+);

            #[allow(non_snake_case)]
            fn evaluate(&'a self, graphrecord: &'a GraphRecord, cache: &'a EvaluationCache<'a>) -> QueryResult<Self::ReturnValue> {
                let ($($T,)+) = self;

                $(let $T = $T.evaluate(graphrecord, cache)?;)+

                Ok(($($T,)+))
            }

            #[allow(non_snake_case)]
            fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
                let ($($T,)+) = self;

                let mut report = OptimizationReport::default();
                $(
                    let ($T, sub_report) = $T.optimize(optimizer, stats);
                    report.phases.extend(sub_report.phases);
                )+

                (($($T,)+), report)
            }

            #[allow(non_snake_case)]
            fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                let ($($T,)+) = self;

                let mut index = 0;
                $(
                    if index > 0 {
                        writeln!(formatter)?;
                    }
                    writeln!(formatter, "[{index}]")?;
                    $T.fmt_plan(formatter)?;
                    index += 1;
                )+
                let _ = index;

                Ok(())
            }
        }
    };
}

impl_return_operand_for_tuples!(R1);
impl_return_operand_for_tuples!(R1, R2);
impl_return_operand_for_tuples!(R1, R2, R3);
impl_return_operand_for_tuples!(R1, R2, R3, R4);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7, R8);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7, R8, R9);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7, R8, R9, R10);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14);
impl_return_operand_for_tuples!(
    R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14, R15
);

pub struct Selection<'a, R: ReturnOperand<'a>> {
    graphrecord: &'a GraphRecord,
    cache: EvaluationCache<'a>,
    unoptimized_return_operand: R,
    optimized_return_operand: R,
    report: OptimizationReport,
}

impl<'a, R: ReturnOperand<'a>> Selection<'a, R> {
    pub fn new_node<Q>(graphrecord: &'a GraphRecord, query: Q) -> Self
    where
        Q: FnOnce(&NodesOperand<Unordered>) -> R,
    {
        Self::new_node_with(graphrecord, Optimizer::shared_builtin(), query)
    }

    pub fn new_node_with<Q>(graphrecord: &'a GraphRecord, optimizer: &Optimizer, query: Q) -> Self
    where
        Q: FnOnce(&NodesOperand<Unordered>) -> R,
    {
        let operand = NodesOperand::new(AllNodes);
        let unoptimized_return_operand = query(&operand);
        let (optimized_return_operand, report) =
            Self::optimize(unoptimized_return_operand.clone(), optimizer, graphrecord);

        Self {
            graphrecord,
            cache: EvaluationCache::new(graphrecord),
            unoptimized_return_operand,
            optimized_return_operand,
            report,
        }
    }

    pub fn new_edge<Q>(graphrecord: &'a GraphRecord, query: Q) -> Self
    where
        Q: FnOnce(&EdgesOperand<Unordered>) -> R,
    {
        Self::new_edge_with(graphrecord, Optimizer::shared_builtin(), query)
    }

    pub fn new_edge_with<Q>(graphrecord: &'a GraphRecord, optimizer: &Optimizer, query: Q) -> Self
    where
        Q: FnOnce(&EdgesOperand<Unordered>) -> R,
    {
        let operand = EdgesOperand::new(AllEdges);
        let unoptimized_return_operand = query(&operand);
        let (optimized_return_operand, report) =
            Self::optimize(unoptimized_return_operand.clone(), optimizer, graphrecord);

        Self {
            graphrecord,
            cache: EvaluationCache::new(graphrecord),
            unoptimized_return_operand,
            optimized_return_operand,
            report,
        }
    }

    fn optimize(
        return_operand: R,
        optimizer: &Optimizer,
        graphrecord: &GraphRecord,
    ) -> (R, OptimizationReport) {
        if optimizer.is_empty() {
            return (return_operand, OptimizationReport::default());
        }

        let stats = Stats::new(graphrecord);

        return_operand.optimize(optimizer, &stats)
    }

    pub fn evaluate(&'a self) -> QueryResult<R::ReturnValue> {
        self.optimized_return_operand
            .evaluate(self.graphrecord, &self.cache)
    }

    pub const fn explain(&'a self) -> QueryExplanation<'a, R> {
        QueryExplanation {
            operand: &self.optimized_return_operand,
            report: Some(&self.report),
        }
    }

    pub const fn explain_unoptimized(&'a self) -> QueryExplanation<'a, R> {
        QueryExplanation {
            operand: &self.unoptimized_return_operand,
            report: None,
        }
    }
}

pub struct QueryExplanation<'a, R> {
    operand: &'a R,
    report: Option<&'a OptimizationReport>,
}

impl<'a, R: ReturnOperand<'a>> Display for QueryExplanation<'a, R> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.operand.fmt_plan(formatter)?;

        if let Some(report) = self.report.filter(|report| !report.phases.is_empty()) {
            write!(formatter, "\n\noptimization:\n{}", report.display())?;
        }

        Ok(())
    }
}

pub trait QueryNodes {
    fn query_nodes<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>;

    fn query_nodes_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>;
}

impl QueryNodes for GraphRecord {
    fn query_nodes<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_node(self, query)
    }

    fn query_nodes_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_node_with(self, optimizer, query)
    }
}

pub trait QueryEdges {
    fn query_edges<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>;

    fn query_edges_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>;
}

impl QueryEdges for GraphRecord {
    fn query_edges<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_edge(self, query)
    }

    fn query_edges_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_edge_with(self, optimizer, query)
    }
}
