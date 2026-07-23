use crate::{
    BoxedIterator, EvaluateOperand, Explanation, Failure, FailureKind, NodeOperand, Operand,
    OrderState, Position, Positional, QueryResult, Unordered,
    execution::EvaluationCache,
    operands::{
        AllEdges, AllNodes, AttributeOperand, AttributesOperand, BareAttributeOperand,
        BareAttributesOperand, BareBoolMaskOperand, BareBoolOperand, BareFailureKindOperand,
        BareFailureKindsOperand, BareFailureOperand, BareFailuresOperand, BareValueOperand,
        BareValuesOperand, BoolMaskOperand, BoolOperand, DefiniteBoolOperand, DefiniteValueOperand,
        EdgeOperand, FailureKindOperand, FailureKindsOperand, FailureOperand, FailuresOperand,
        GroupOperand, GroupedIterator, IndexOperand, IndicesOperand, NestedAttributesOperand,
        NestedBoolMaskOperand, ValueOperand, ValuesOperand,
    },
    operations::GroupKey,
    optimizer::{OptimizationReport, Optimizer, Stats},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex},
};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
use std::fmt::{self, Display, Formatter};

macro_rules! impl_return_operand {
    ($( $({$O:ident})? $Operand:ty => $ReturnValue:ty ),* $(,)?) => {
        $(
            impl<'a $(, $O: OrderState)?> ReturnOperand<'a> for $Operand {
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
        Q: FnOnce(&NodeOperand<Unordered>) -> R,
    {
        Self::new_node_with(graphrecord, Optimizer::shared_builtin(), query)
    }

    pub fn new_node_with<Q>(graphrecord: &'a GraphRecord, optimizer: &Optimizer, query: Q) -> Self
    where
        Q: FnOnce(&NodeOperand<Unordered>) -> R,
    {
        let operand = NodeOperand::<Unordered>::new(AllNodes);
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
        Q: FnOnce(&EdgeOperand<Unordered>) -> R,
    {
        Self::new_edge_with(graphrecord, Optimizer::shared_builtin(), query)
    }

    pub fn new_edge_with<Q>(graphrecord: &'a GraphRecord, optimizer: &Optimizer, query: Q) -> Self
    where
        Q: FnOnce(&EdgeOperand<Unordered>) -> R,
    {
        let operand = EdgeOperand::<Unordered>::new(AllEdges);
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
    {O} ValuesOperand<NodeIndex, O> => BoxedIterator<'a, (&'a NodeIndex, QueryResult<GraphRecordValue>)>,
    {O} ValuesOperand<EdgeIndex, O> => BoxedIterator<'a, (&'a EdgeIndex, QueryResult<GraphRecordValue>)>,
    {O} ValuesOperand<Positional, O> => BoxedIterator<'a, (Position, QueryResult<GraphRecordValue>)>,
    {O} ValuesOperand<FailureKind, O> => BoxedIterator<'a, (FailureKind, QueryResult<GraphRecordValue>)>,
    {O} BareValuesOperand<O> => BoxedIterator<'a, QueryResult<GraphRecordValue>>,
    ValueOperand<NodeIndex> => Option<(&'a NodeIndex, QueryResult<GraphRecordValue>)>,
    ValueOperand<EdgeIndex> => Option<(&'a EdgeIndex, QueryResult<GraphRecordValue>)>,
    ValueOperand<Positional> => Option<(Position, QueryResult<GraphRecordValue>)>,
    ValueOperand<FailureKind> => Option<(FailureKind, QueryResult<GraphRecordValue>)>,
    BareValueOperand => Option<QueryResult<GraphRecordValue>>,
    DefiniteValueOperand => QueryResult<GraphRecordValue>,

    {O} BoolMaskOperand<NodeIndex, O> => BoxedIterator<'a, (&'a NodeIndex, QueryResult<bool>)>,
    {O} BoolMaskOperand<EdgeIndex, O> => BoxedIterator<'a, (&'a EdgeIndex, QueryResult<bool>)>,
    {O} BoolMaskOperand<Positional, O> => BoxedIterator<'a, (Position, QueryResult<bool>)>,
    {O} BoolMaskOperand<FailureKind, O> => BoxedIterator<'a, (FailureKind, QueryResult<bool>)>,
    {O} BareBoolMaskOperand<O> => BoxedIterator<'a, QueryResult<bool>>,
    BoolOperand<NodeIndex> => Option<(&'a NodeIndex, QueryResult<bool>)>,
    BoolOperand<EdgeIndex> => Option<(&'a EdgeIndex, QueryResult<bool>)>,
    BoolOperand<Positional> => Option<(Position, QueryResult<bool>)>,
    BoolOperand<FailureKind> => Option<(FailureKind, QueryResult<bool>)>,
    BareBoolOperand => Option<QueryResult<bool>>,
    DefiniteBoolOperand => QueryResult<bool>,

    {O} NestedAttributesOperand<NodeIndex, O> => BoxedIterator<'a, (&'a NodeIndex, QueryResult<GrHashSet<GraphRecordAttribute>>)>,
    {O} NestedAttributesOperand<EdgeIndex, O> => BoxedIterator<'a, (&'a EdgeIndex, QueryResult<GrHashSet<GraphRecordAttribute>>)>,
    {O} NestedAttributesOperand<FailureKind, O> => BoxedIterator<'a, (FailureKind, QueryResult<GrHashSet<GraphRecordAttribute>>)>,
    {O} AttributesOperand<NodeIndex, O> => BoxedIterator<'a, (&'a NodeIndex, QueryResult<GraphRecordAttribute>)>,
    {O} AttributesOperand<EdgeIndex, O> => BoxedIterator<'a, (&'a EdgeIndex, QueryResult<GraphRecordAttribute>)>,
    {O} AttributesOperand<Positional, O> => BoxedIterator<'a, (Position, QueryResult<GraphRecordAttribute>)>,
    {O} AttributesOperand<FailureKind, O> => BoxedIterator<'a, (FailureKind, QueryResult<GraphRecordAttribute>)>,
    {O} BareAttributesOperand<O> => BoxedIterator<'a, QueryResult<GraphRecordAttribute>>,
    AttributeOperand<NodeIndex> => Option<(&'a NodeIndex, QueryResult<GraphRecordAttribute>)>,
    AttributeOperand<EdgeIndex> => Option<(&'a EdgeIndex, QueryResult<GraphRecordAttribute>)>,
    AttributeOperand<Positional> => Option<(Position, QueryResult<GraphRecordAttribute>)>,
    AttributeOperand<FailureKind> => Option<(FailureKind, QueryResult<GraphRecordAttribute>)>,
    BareAttributeOperand => Option<QueryResult<GraphRecordAttribute>>,

    {O} FailuresOperand<NodeIndex, O> => BoxedIterator<'a, (&'a NodeIndex, QueryResult<Failure>)>,
    {O} FailuresOperand<EdgeIndex, O> => BoxedIterator<'a, (&'a EdgeIndex, QueryResult<Failure>)>,
    {O} FailuresOperand<Positional, O> => BoxedIterator<'a, (Position, QueryResult<Failure>)>,
    {O} FailuresOperand<FailureKind, O> => BoxedIterator<'a, (FailureKind, QueryResult<Failure>)>,
    {O} FailureKindsOperand<NodeIndex, O> => BoxedIterator<'a, (&'a NodeIndex, QueryResult<FailureKind>)>,
    {O} FailureKindsOperand<EdgeIndex, O> => BoxedIterator<'a, (&'a EdgeIndex, QueryResult<FailureKind>)>,
    {O} FailureKindsOperand<Positional, O> => BoxedIterator<'a, (Position, QueryResult<FailureKind>)>,
    {O} FailureKindsOperand<FailureKind, O> => BoxedIterator<'a, (FailureKind, QueryResult<FailureKind>)>,
    {O} BareFailuresOperand<O> => BoxedIterator<'a, QueryResult<Failure>>,
    {O} BareFailureKindsOperand<O> => BoxedIterator<'a, QueryResult<FailureKind>>,
    FailureOperand<NodeIndex> => Option<(&'a NodeIndex, QueryResult<Failure>)>,
    FailureOperand<EdgeIndex> => Option<(&'a EdgeIndex, QueryResult<Failure>)>,
    FailureOperand<Positional> => Option<(Position, QueryResult<Failure>)>,
    FailureOperand<FailureKind> => Option<(FailureKind, QueryResult<Failure>)>,
    FailureKindOperand<NodeIndex> => Option<(&'a NodeIndex, QueryResult<FailureKind>)>,
    FailureKindOperand<EdgeIndex> => Option<(&'a EdgeIndex, QueryResult<FailureKind>)>,
    FailureKindOperand<Positional> => Option<(Position, QueryResult<FailureKind>)>,
    FailureKindOperand<FailureKind> => Option<(FailureKind, QueryResult<FailureKind>)>,
    BareFailureOperand => Option<QueryResult<Failure>>,
    BareFailureKindOperand => Option<QueryResult<FailureKind>>,
);

impl<'a, T: 'static + Clone, O: OrderState> ReturnOperand<'a>
    for NestedBoolMaskOperand<NodeIndex, T, O>
{
    type ReturnValue = BoxedIterator<'a, (&'a NodeIndex, QueryResult<GrHashMap<T, bool>>)>;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue> {
        <Self as EvaluateOperand>::evaluate(self, graphrecord, cache)
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }
}

impl<'a, T: 'static + Clone, O: OrderState> ReturnOperand<'a>
    for NestedBoolMaskOperand<EdgeIndex, T, O>
{
    type ReturnValue = BoxedIterator<'a, (&'a EdgeIndex, QueryResult<GrHashMap<T, bool>>)>;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue> {
        <Self as EvaluateOperand>::evaluate(self, graphrecord, cache)
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }
}

impl<'a, T: 'static + Clone, O: OrderState> ReturnOperand<'a>
    for NestedBoolMaskOperand<FailureKind, T, O>
{
    type ReturnValue = BoxedIterator<'a, (FailureKind, QueryResult<GrHashMap<T, bool>>)>;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue> {
        <Self as EvaluateOperand>::evaluate(self, graphrecord, cache)
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }
}

impl<'a> ReturnOperand<'a> for IndexOperand<NodeIndex> {
    type ReturnValue = Option<QueryResult<NodeIndex>>;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue> {
        Ok(EvaluateOperand::evaluate(self, graphrecord, cache)?
            .map(|(_index, value)| value.cloned()))
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }
}

impl<'a> ReturnOperand<'a> for IndexOperand<EdgeIndex> {
    type ReturnValue = Option<QueryResult<EdgeIndex>>;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue> {
        Ok(EvaluateOperand::evaluate(self, graphrecord, cache)?
            .map(|(_index, value)| value.copied()))
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }
}

impl<'a> ReturnOperand<'a> for IndexOperand<FailureKind> {
    type ReturnValue = Option<QueryResult<FailureKind>>;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue> {
        Ok(EvaluateOperand::evaluate(self, graphrecord, cache)?.map(|(_index, value)| value))
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }
}

impl<'a, O: OrderState> ReturnOperand<'a> for IndicesOperand<NodeIndex, O> {
    type ReturnValue = BoxedIterator<'a, QueryResult<NodeIndex>>;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue> {
        Ok(Box::new(
            EvaluateOperand::evaluate(self, graphrecord, cache)?
                .map(|(_index, value)| value.cloned()),
        ))
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }
}

impl<'a, O: OrderState> ReturnOperand<'a> for IndicesOperand<EdgeIndex, O> {
    type ReturnValue = BoxedIterator<'a, QueryResult<EdgeIndex>>;

    fn evaluate(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue> {
        Ok(Box::new(
            EvaluateOperand::evaluate(self, graphrecord, cache)?
                .map(|(_index, value)| value.copied()),
        ))
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }
}

impl<'a, O: OrderState> ReturnOperand<'a> for IndicesOperand<FailureKind, O> {
    type ReturnValue = BoxedIterator<'a, QueryResult<FailureKind>>;

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
{
    type ReturnValue =
        GroupedIterator<'a, K::Key<'a>, QueryResult<<O as EvaluateOperand>::ReturnValue<'a>>>;

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
