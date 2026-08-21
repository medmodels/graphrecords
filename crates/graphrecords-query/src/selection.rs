use crate::{
    Bare, Definite, Failure, IndexDomain, IndexValue, Indexed, Mask, Multiple, OrderState,
    Queryable, Series, Single, Unit,
    error::{index::UncoveredIndices, policy::RaisedFailures},
    expressions::ExpressionHandle,
    operations::{Keyed, WithMissing, policy::Drop},
};
use graphrecords_core::{
    GraphRecord, StateView,
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{
        EdgeIndex, EntityDomain, GroupIndex, MultipleSelection, NodeIndex, SingleSelection,
    },
};
use graphrecords_utils::{aliases::GrHashMap, distinct::Distinct};
use std::{hash::Hash, sync::Arc};

const LABEL: &str = "Selection";

impl From<Box<Failure>> for GraphRecordError {
    fn from(failure: Box<Failure>) -> Self {
        Self::QueryFailure {
            cause: Arc::new(*failure),
        }
    }
}

impl<E, O> MultipleSelection<E> for ExpressionHandle<Indexed<E, Unit>, Multiple<O>>
where
    E: EntityDomain + IndexDomain<Owned = E>,
    O: OrderState,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = graphrecord.query(self);
        let selected = series.evaluate().and_then(|elements| {
            let mut collected = Vec::new();
            let mut raised = Vec::new();

            for outcome in elements {
                match outcome {
                    Ok(index) => collected.push(index),
                    Err(failure) => raised.push(*failure),
                }
            }

            if !raised.is_empty() {
                return Err(Failure::new(RaisedFailures::new(raised), LABEL));
            }

            Ok(collected)
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> MultipleSelection<E> for ExpressionHandle<Indexed<E, Unit>, Single>
where
    E: EntityDomain + IndexDomain<Owned = E>,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = graphrecord.query(self);
        let selected = series.evaluate().and_then(|element| match element {
            Some(Ok(index)) => Ok(vec![index]),
            Some(Err(failure)) => Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL)),
            None => Ok(Vec::new()),
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> SingleSelection<E> for ExpressionHandle<Indexed<E, Unit>, Single> where
    E: EntityDomain + IndexDomain<Owned = E>
{
}

impl<E, O> MultipleSelection<E> for Series<ExpressionHandle<Indexed<E, Unit>, Multiple<O>>>
where
    E: EntityDomain + IndexDomain<Owned = E>,
    O: OrderState,
{
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let selected = self.evaluate().and_then(|elements| {
            let mut collected = Vec::new();
            let mut raised = Vec::new();

            for outcome in elements {
                match outcome {
                    Ok(index) => collected.push(index),
                    Err(failure) => raised.push(*failure),
                }
            }

            if !raised.is_empty() {
                return Err(Failure::new(RaisedFailures::new(raised), LABEL));
            }

            Ok(collected)
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> MultipleSelection<E> for Series<ExpressionHandle<Indexed<E, Unit>, Single>>
where
    E: EntityDomain + IndexDomain<Owned = E>,
{
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let selected = self.evaluate().and_then(|element| match element {
            Some(Ok(index)) => Ok(vec![index]),
            Some(Err(failure)) => Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL)),
            None => Ok(Vec::new()),
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> SingleSelection<E> for Series<ExpressionHandle<Indexed<E, Unit>, Single>> where
    E: EntityDomain + IndexDomain<Owned = E>
{
}

impl<E> MultipleSelection<E> for ExpressionHandle<Indexed<E, Unit>, Definite>
where
    E: EntityDomain + IndexDomain<Owned = E>,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = graphrecord.query(self);
        let selected = series.evaluate().and_then(|element| match element {
            Ok(index) => Ok(vec![index]),
            Err(failure) => Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL)),
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> SingleSelection<E> for ExpressionHandle<Indexed<E, Unit>, Definite> where
    E: EntityDomain + IndexDomain<Owned = E>
{
}

impl<E> MultipleSelection<E> for Series<ExpressionHandle<Indexed<E, Unit>, Definite>>
where
    E: EntityDomain + IndexDomain<Owned = E>,
{
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let selected = self.evaluate().and_then(|element| match element {
            Ok(index) => Ok(vec![index]),
            Err(failure) => Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL)),
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> SingleSelection<E> for Series<ExpressionHandle<Indexed<E, Unit>, Definite>> where
    E: EntityDomain + IndexDomain<Owned = E>
{
}

impl<E, O> MultipleSelection<E> for ExpressionHandle<Indexed<E, Mask>, Multiple<O>>
where
    E: EntityDomain + IndexDomain<Owned = E>,
    O: OrderState,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = graphrecord.query(self);
        let selected = series.evaluate().and_then(|elements| {
            let mut collected = Vec::new();
            let mut raised = Vec::new();

            for (index, outcome) in elements {
                match outcome {
                    Ok(true) => collected.push(E::own_index(&index)),
                    Ok(false) => {}
                    Err(failure) => raised.push(*failure),
                }
            }

            if !raised.is_empty() {
                return Err(Failure::new(RaisedFailures::new(raised), LABEL));
            }

            Ok(collected)
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> MultipleSelection<E> for ExpressionHandle<Indexed<E, Mask>, Single>
where
    E: EntityDomain + IndexDomain<Owned = E>,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = graphrecord.query(self);
        let selected = series.evaluate().and_then(|element| match element {
            Some((index, Ok(true))) => Ok(vec![E::own_index(&index)]),
            Some((_, Ok(false))) | None => Ok(Vec::new()),
            Some((_, Err(failure))) => {
                Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL))
            }
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> SingleSelection<E> for ExpressionHandle<Indexed<E, Mask>, Single> where
    E: EntityDomain + IndexDomain<Owned = E>
{
}

impl<O: OrderState> MultipleSelection<NodeIndex>
    for Series<ExpressionHandle<Indexed<NodeIndex, Mask>, Multiple<O>>>
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<NodeIndex>> {
        let selected = self.evaluate().and_then(|elements| {
            let mut values: GrHashMap<_, _> = GrHashMap::default();
            let mut raised = Vec::new();

            for (index, outcome) in elements {
                match outcome {
                    Ok(value) => {
                        values.insert(NodeIndex::own_index(&index), value);
                    }
                    Err(failure) => raised.push(*failure),
                }
            }

            if !raised.is_empty() {
                return Err(Failure::new(RaisedFailures::new(raised), LABEL));
            }

            let state_view = StateView::of(graphrecord);
            let mut collected = Vec::new();
            let mut uncovered = Vec::new();

            for address in state_view.node_addresses() {
                let index = NodeIndex::own_index(&state_view.node_index(address));

                match values.get(&index).copied() {
                    Some(true) => collected.push(index),
                    Some(false) => {}
                    None => uncovered.push(index),
                }
            }

            if !uncovered.is_empty() {
                return Err(Failure::new(
                    UncoveredIndices::<NodeIndex>::new(uncovered),
                    LABEL,
                ));
            }

            Ok(collected)
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<O: OrderState> MultipleSelection<EdgeIndex>
    for Series<ExpressionHandle<Indexed<EdgeIndex, Mask>, Multiple<O>>>
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<EdgeIndex>> {
        let selected = self.evaluate().and_then(|elements| {
            let mut values: GrHashMap<_, _> = GrHashMap::default();
            let mut raised = Vec::new();

            for (index, outcome) in elements {
                match outcome {
                    Ok(value) => {
                        values.insert(EdgeIndex::own_index(&index), value);
                    }
                    Err(failure) => raised.push(*failure),
                }
            }

            if !raised.is_empty() {
                return Err(Failure::new(RaisedFailures::new(raised), LABEL));
            }

            let state_view = StateView::of(graphrecord);
            let mut collected = Vec::new();
            let mut uncovered = Vec::new();

            for address in state_view.edge_addresses() {
                let index = state_view.edge_index(address);

                match values.get(&index).copied() {
                    Some(true) => collected.push(index),
                    Some(false) => {}
                    None => uncovered.push(index),
                }
            }

            if !uncovered.is_empty() {
                return Err(Failure::new(
                    UncoveredIndices::<EdgeIndex>::new(uncovered),
                    LABEL,
                ));
            }

            Ok(collected)
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<O: OrderState> MultipleSelection<GroupIndex>
    for Series<ExpressionHandle<Indexed<GroupIndex, Mask>, Multiple<O>>>
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<GroupIndex>> {
        let selected = self.evaluate().and_then(|elements| {
            let mut values: GrHashMap<_, _> = GrHashMap::default();
            let mut raised = Vec::new();

            for (index, outcome) in elements {
                match outcome {
                    Ok(value) => {
                        values.insert(GroupIndex::own_index(&index), value);
                    }
                    Err(failure) => raised.push(*failure),
                }
            }

            if !raised.is_empty() {
                return Err(Failure::new(RaisedFailures::new(raised), LABEL));
            }

            let state_view = StateView::of(graphrecord);
            let mut collected = Vec::new();
            let mut uncovered = Vec::new();

            for address in state_view.group_addresses() {
                let index = state_view.group_index(address).clone();

                match values.get(&index).copied() {
                    Some(true) => collected.push(index),
                    Some(false) => {}
                    None => uncovered.push(index),
                }
            }

            if !uncovered.is_empty() {
                return Err(Failure::new(
                    UncoveredIndices::<GroupIndex>::new(uncovered),
                    LABEL,
                ));
            }

            Ok(collected)
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> MultipleSelection<E> for Series<ExpressionHandle<Indexed<E, Mask>, Single>>
where
    E: EntityDomain + IndexDomain<Owned = E>,
{
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let selected = self.evaluate().and_then(|element| match element {
            Some((index, Ok(true))) => Ok(vec![E::own_index(&index)]),
            Some((_, Ok(false))) | None => Ok(Vec::new()),
            Some((_, Err(failure))) => {
                Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL))
            }
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> SingleSelection<E> for Series<ExpressionHandle<Indexed<E, Mask>, Single>> where
    E: EntityDomain + IndexDomain<Owned = E>
{
}

impl<E> MultipleSelection<E> for ExpressionHandle<Indexed<E, Mask>, Definite>
where
    E: EntityDomain + IndexDomain<Owned = E>,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = graphrecord.query(self);
        let selected = series.evaluate().and_then(|element| match element {
            (index, Ok(true)) => Ok(vec![E::own_index(&index)]),
            (_, Ok(false)) => Ok(Vec::new()),
            (_, Err(failure)) => Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL)),
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> SingleSelection<E> for ExpressionHandle<Indexed<E, Mask>, Definite> where
    E: EntityDomain + IndexDomain<Owned = E>
{
}

impl<E> MultipleSelection<E> for Series<ExpressionHandle<Indexed<E, Mask>, Definite>>
where
    E: EntityDomain + IndexDomain<Owned = E>,
{
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let selected = self.evaluate().and_then(|element| match element {
            (index, Ok(true)) => Ok(vec![E::own_index(&index)]),
            (_, Ok(false)) => Ok(Vec::new()),
            (_, Err(failure)) => Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL)),
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> SingleSelection<E> for Series<ExpressionHandle<Indexed<E, Mask>, Definite>> where
    E: EntityDomain + IndexDomain<Owned = E>
{
}

impl<E, I, O> MultipleSelection<E> for ExpressionHandle<Indexed<I, IndexValue<E>>, Multiple<O>>
where
    E: EntityDomain + IndexDomain<Owned = E> + Eq + Hash,
    I: IndexDomain,
    O: OrderState,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = graphrecord.query(self);
        let selected = series.evaluate().and_then(|elements| {
            let mut collected = Vec::new();
            let mut raised = Vec::new();

            for (_, outcome) in elements {
                match outcome {
                    Ok(index) => collected.push(E::own_index(&index)),
                    Err(failure) => raised.push(*failure),
                }
            }

            if !raised.is_empty() {
                return Err(Failure::new(RaisedFailures::new(raised), LABEL));
            }

            let collected: Vec<_> = collected.into_iter().collect::<Distinct<_>>().into();

            Ok(collected)
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E, I> MultipleSelection<E> for ExpressionHandle<Indexed<I, IndexValue<E>>, Single>
where
    E: EntityDomain + IndexDomain<Owned = E>,
    I: IndexDomain,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = graphrecord.query(self);
        let selected = series.evaluate().and_then(|element| match element {
            Some((_, Ok(index))) => Ok(vec![E::own_index(&index)]),
            Some((_, Err(failure))) => {
                Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL))
            }
            None => Ok(Vec::new()),
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E, I> SingleSelection<E> for ExpressionHandle<Indexed<I, IndexValue<E>>, Single>
where
    E: EntityDomain + IndexDomain<Owned = E>,
    I: IndexDomain,
{
}

impl<E, I> MultipleSelection<E> for ExpressionHandle<Indexed<I, IndexValue<E>>, Definite>
where
    E: EntityDomain + IndexDomain<Owned = E>,
    I: IndexDomain,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = graphrecord.query(self);
        let selected = series.evaluate().and_then(|element| match element.1 {
            Ok(index) => Ok(vec![E::own_index(&index)]),
            Err(failure) => Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL)),
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E, I> SingleSelection<E> for ExpressionHandle<Indexed<I, IndexValue<E>>, Definite>
where
    E: EntityDomain + IndexDomain<Owned = E>,
    I: IndexDomain,
{
}

impl<E, I, O> MultipleSelection<E>
    for Series<ExpressionHandle<Indexed<I, IndexValue<E>>, Multiple<O>>>
where
    E: EntityDomain + IndexDomain<Owned = E> + Eq + Hash,
    I: IndexDomain,
    O: OrderState,
{
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let selected = self.evaluate().and_then(|elements| {
            let mut collected = Vec::new();
            let mut raised = Vec::new();

            for (_, outcome) in elements {
                match outcome {
                    Ok(index) => collected.push(E::own_index(&index)),
                    Err(failure) => raised.push(*failure),
                }
            }

            if !raised.is_empty() {
                return Err(Failure::new(RaisedFailures::new(raised), LABEL));
            }

            let collected: Vec<_> = collected.into_iter().collect::<Distinct<_>>().into();

            Ok(collected)
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E, I> MultipleSelection<E> for Series<ExpressionHandle<Indexed<I, IndexValue<E>>, Single>>
where
    E: EntityDomain + IndexDomain<Owned = E>,
    I: IndexDomain,
{
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let selected = self.evaluate().and_then(|element| match element {
            Some((_, Ok(index))) => Ok(vec![E::own_index(&index)]),
            Some((_, Err(failure))) => {
                Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL))
            }
            None => Ok(Vec::new()),
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E, I> SingleSelection<E> for Series<ExpressionHandle<Indexed<I, IndexValue<E>>, Single>>
where
    E: EntityDomain + IndexDomain<Owned = E>,
    I: IndexDomain,
{
}

impl<E, I> MultipleSelection<E> for Series<ExpressionHandle<Indexed<I, IndexValue<E>>, Definite>>
where
    E: EntityDomain + IndexDomain<Owned = E>,
    I: IndexDomain,
{
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let selected = self.evaluate().and_then(|element| match element.1 {
            Ok(index) => Ok(vec![E::own_index(&index)]),
            Err(failure) => Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL)),
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E, I> SingleSelection<E> for Series<ExpressionHandle<Indexed<I, IndexValue<E>>, Definite>>
where
    E: EntityDomain + IndexDomain<Owned = E>,
    I: IndexDomain,
{
}

impl<E> MultipleSelection<E> for ExpressionHandle<Bare<IndexValue<E>>, Single>
where
    E: EntityDomain + IndexDomain<Owned = E>,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = graphrecord.query(self);
        let selected = series.evaluate().and_then(|element| match element {
            Some(Ok(index)) => Ok(vec![E::own_index(&index)]),
            Some(Err(failure)) => Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL)),
            None => Ok(Vec::new()),
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> SingleSelection<E> for ExpressionHandle<Bare<IndexValue<E>>, Single> where
    E: EntityDomain + IndexDomain<Owned = E>
{
}

impl<E> MultipleSelection<E> for Series<ExpressionHandle<Bare<IndexValue<E>>, Single>>
where
    E: EntityDomain + IndexDomain<Owned = E>,
{
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let selected = self.evaluate().and_then(|element| match element {
            Some(Ok(index)) => Ok(vec![E::own_index(&index)]),
            Some(Err(failure)) => Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL)),
            None => Ok(Vec::new()),
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> SingleSelection<E> for Series<ExpressionHandle<Bare<IndexValue<E>>, Single>> where
    E: EntityDomain + IndexDomain<Owned = E>
{
}

impl<E, O> MultipleSelection<E> for ExpressionHandle<Bare<IndexValue<E>>, Multiple<O>>
where
    E: EntityDomain + IndexDomain<Owned = E> + Eq + Hash,
    O: OrderState,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = graphrecord.query(self);
        let selected = series.evaluate().and_then(|elements| {
            let mut collected = Vec::new();
            let mut raised = Vec::new();

            for outcome in elements {
                match outcome {
                    Ok(index) => collected.push(E::own_index(&index)),
                    Err(failure) => raised.push(*failure),
                }
            }

            if !raised.is_empty() {
                return Err(Failure::new(RaisedFailures::new(raised), LABEL));
            }

            let collected: Vec<_> = collected.into_iter().collect::<Distinct<_>>().into();

            Ok(collected)
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E, O> MultipleSelection<E> for Series<ExpressionHandle<Bare<IndexValue<E>>, Multiple<O>>>
where
    E: EntityDomain + IndexDomain<Owned = E> + Eq + Hash,
    O: OrderState,
{
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let selected = self.evaluate().and_then(|elements| {
            let mut collected = Vec::new();
            let mut raised = Vec::new();

            for outcome in elements {
                match outcome {
                    Ok(index) => collected.push(E::own_index(&index)),
                    Err(failure) => raised.push(*failure),
                }
            }

            if !raised.is_empty() {
                return Err(Failure::new(RaisedFailures::new(raised), LABEL));
            }

            let collected: Vec<_> = collected.into_iter().collect::<Distinct<_>>().into();

            Ok(collected)
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> MultipleSelection<E> for ExpressionHandle<Bare<IndexValue<E>>, Definite>
where
    E: EntityDomain + IndexDomain<Owned = E>,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = graphrecord.query(self);
        let selected = series.evaluate().and_then(|element| match element {
            Ok(index) => Ok(vec![E::own_index(&index)]),
            Err(failure) => Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL)),
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> SingleSelection<E> for ExpressionHandle<Bare<IndexValue<E>>, Definite> where
    E: EntityDomain + IndexDomain<Owned = E>
{
}

impl<E> MultipleSelection<E> for Series<ExpressionHandle<Bare<IndexValue<E>>, Definite>>
where
    E: EntityDomain + IndexDomain<Owned = E>,
{
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let selected = self.evaluate().and_then(|element| match element {
            Ok(index) => Ok(vec![E::own_index(&index)]),
            Err(failure) => Err(Failure::new(RaisedFailures::new(vec![*failure]), LABEL)),
        });

        selected.map_err(GraphRecordError::from)
    }
}

impl<E> SingleSelection<E> for Series<ExpressionHandle<Bare<IndexValue<E>>, Definite>> where
    E: EntityDomain + IndexDomain<Owned = E>
{
}

impl<E, O> MultipleSelection<E>
    for WithMissing<Keyed<E>, Series<ExpressionHandle<Indexed<E, Unit>, Multiple<O>>>, Drop>
where
    E: EntityDomain + IndexDomain<Owned = E>,
    O: OrderState,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = self.into_inner();
        let selected = series.evaluate().and_then(|elements| {
            let mut collected = Vec::new();
            let mut raised = Vec::new();

            for outcome in elements {
                match outcome {
                    Ok(index) => collected.push(index),
                    Err(failure) => raised.push(*failure),
                }
            }

            if !raised.is_empty() {
                return Err(Failure::new(RaisedFailures::new(raised), LABEL));
            }

            Ok(collected)
        });

        selected
            .map(|selected| {
                selected
                    .into_iter()
                    .filter(|index| E::contains(graphrecord, index))
                    .collect()
            })
            .map_err(GraphRecordError::from)
    }
}

impl<E, O> MultipleSelection<E>
    for WithMissing<Keyed<E>, Series<ExpressionHandle<Indexed<E, Mask>, Multiple<O>>>, Drop>
where
    E: EntityDomain + IndexDomain<Owned = E>,
    O: OrderState,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = self.into_inner();
        let selected = series.evaluate().and_then(|elements| {
            let mut collected = Vec::new();
            let mut raised = Vec::new();

            for (index, outcome) in elements {
                match outcome {
                    Ok(true) => collected.push(E::own_index(&index)),
                    Ok(false) => {}
                    Err(failure) => raised.push(*failure),
                }
            }

            if !raised.is_empty() {
                return Err(Failure::new(RaisedFailures::new(raised), LABEL));
            }

            Ok(collected)
        });

        selected
            .map(|selected| {
                selected
                    .into_iter()
                    .filter(|index| E::contains(graphrecord, index))
                    .collect()
            })
            .map_err(GraphRecordError::from)
    }
}

impl<E, I, O> MultipleSelection<E>
    for WithMissing<
        Keyed<I>,
        Series<ExpressionHandle<Indexed<I, IndexValue<E>>, Multiple<O>>>,
        Drop,
    >
where
    E: EntityDomain + IndexDomain<Owned = E> + Eq + Hash,
    I: IndexDomain,
    O: OrderState,
{
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        let series = self.into_inner();
        let selected = series.evaluate().and_then(|elements| {
            let mut collected = Vec::new();
            let mut raised = Vec::new();

            for (_, outcome) in elements {
                match outcome {
                    Ok(index) => collected.push(E::own_index(&index)),
                    Err(failure) => raised.push(*failure),
                }
            }

            if !raised.is_empty() {
                return Err(Failure::new(RaisedFailures::new(raised), LABEL));
            }

            Ok(collected)
        });

        selected
            .map(|selected| {
                selected
                    .into_iter()
                    .filter(|index| E::contains(graphrecord, index))
                    .collect::<Distinct<_>>()
                    .into()
            })
            .map_err(GraphRecordError::from)
    }
}
