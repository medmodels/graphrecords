use crate::{
    BoxedIterator, EvaluateOperand, Explanation, NodeOperand, Operand, OperandContext, QueryResult,
    execution::EvaluationCache,
    operands::{
        AllEdges, AllNodes, BoolMaskOperand, EdgeOperand, GroupOperand, GroupedIterator,
        IndicesOperand, ValueOperand, ValuesOperand,
    },
    operations::GroupKey,
    optimizer::{OptimizationReport, Optimizer, Stats},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GraphRecordValue, NodeIndex},
};
use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

macro_rules! impl_return_operand {
    ($( $Operand:ty => $ReturnValue:ty ),* $(,)?) => {
        $(
            impl<'a> ReturnOperand<'a> for $Operand {
                type ReturnValue = $ReturnValue;

                fn evaluate(&'a self, graphrecord: &'a GraphRecord, cache: &'a EvaluationCache<'a>) -> QueryResult<Self::ReturnValue> {
                    <Self as EvaluateOperand>::evaluate(self, graphrecord, cache)
                }

                fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
                    optimizer.run_reported(stats, &self)
                }

                fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                    write!(formatter, "{}", Explanation::new(self))
                }
            }
        )*
    };
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
        Q: FnOnce(&NodeOperand) -> R,
    {
        Self::new_node_with(graphrecord, &Optimizer::builtin(), query)
    }

    pub fn new_node_with<Q>(graphrecord: &'a GraphRecord, optimizer: &Optimizer, query: Q) -> Self
    where
        Q: FnOnce(&NodeOperand) -> R,
    {
        let operand = NodeOperand::new(AllNodes);
        let unoptimized_return_operand = query(&operand);
        let (optimized_return_operand, report) =
            optimize(unoptimized_return_operand.clone(), optimizer, graphrecord);

        Self {
            graphrecord,
            cache: EvaluationCache::new(),
            unoptimized_return_operand,
            optimized_return_operand,
            report,
        }
    }

    pub fn new_edge<Q>(graphrecord: &'a GraphRecord, query: Q) -> Self
    where
        Q: FnOnce(&EdgeOperand) -> R,
    {
        Self::new_edge_with(graphrecord, &Optimizer::builtin(), query)
    }

    pub fn new_edge_with<Q>(graphrecord: &'a GraphRecord, optimizer: &Optimizer, query: Q) -> Self
    where
        Q: FnOnce(&EdgeOperand) -> R,
    {
        let operand = EdgeOperand::new(AllEdges);
        let unoptimized_return_operand = query(&operand);
        let (optimized_return_operand, report) =
            optimize(unoptimized_return_operand.clone(), optimizer, graphrecord);

        Self {
            graphrecord,
            cache: EvaluationCache::new(),
            unoptimized_return_operand,
            optimized_return_operand,
            report,
        }
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

fn optimize<'a, R: ReturnOperand<'a>>(
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

impl_return_operand!(
    ValuesOperand<NodeIndex> => BoxedIterator<'a, (&'a NodeIndex, QueryResult<GraphRecordValue>)>,
    ValuesOperand<EdgeIndex> => BoxedIterator<'a, (&'a EdgeIndex, QueryResult<GraphRecordValue>)>,
    BoolMaskOperand<NodeIndex> => BoxedIterator<'a, (&'a NodeIndex, QueryResult<bool>)>,
    BoolMaskOperand<EdgeIndex> => BoxedIterator<'a, (&'a EdgeIndex, QueryResult<bool>)>,
    ValueOperand<NodeIndex> => Option<(&'a NodeIndex, QueryResult<GraphRecordValue>)>,
    ValueOperand<EdgeIndex> => Option<(&'a EdgeIndex, QueryResult<GraphRecordValue>)>,
);

impl<'a> ReturnOperand<'a> for IndicesOperand<NodeIndex> {
    type ReturnValue = BoxedIterator<'a, QueryResult<NodeIndex>>;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue> {
        Ok(Box::new(
            EvaluateOperand::evaluate(self, graphrecord, cache)?.map(|(_index, value)| value),
        ))
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }
}

impl<'a> ReturnOperand<'a> for IndicesOperand<EdgeIndex> {
    type ReturnValue = BoxedIterator<'a, QueryResult<EdgeIndex>>;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue> {
        Ok(Box::new(
            EvaluateOperand::evaluate(self, graphrecord, cache)?.map(|(_index, value)| value),
        ))
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }
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

impl<'a, R: ReturnOperand<'a>> ReturnOperand<'a> for &R {
    type ReturnValue = R::ReturnValue;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue> {
        R::evaluate(self, graphrecord, cache)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        R::fmt_plan(self, formatter)
    }
}

impl<'a, O, K> ReturnOperand<'a> for GroupOperand<O, K>
where
    K: GroupKey,
    O: Operand + ReturnOperand<'a>,
    Arc<dyn OperandContext<Self>>: 'a,
{
    type ReturnValue =
        GroupedIterator<'a, K::Key, QueryResult<<O as EvaluateOperand>::ReturnValue<'a>>>;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue> {
        EvaluateOperand::evaluate(self, graphrecord, cache)
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }
}
