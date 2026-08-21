use super::{
    DynArgumentLane, DynArityContainer, DynExpression, DynIndexOwned, DynTerminal, DynTerminalLane,
    DynValue,
};
use crate::{
    Failure, Mask, Queryable, Series,
    error::{index::UncoveredIndices, policy::RaisedFailures},
    registry::{
        ArityDescriptor, ExpressionDescriptor, IndexDescriptor, LaneShapeDescriptor, ValueRole,
    },
    selection::LABEL,
};
use graphrecords_core::{
    GraphRecord, StateView,
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{EdgeIndex, EntityDomain, GroupIndex, NodeIndex},
};
use graphrecords_utils::{aliases::GrHashMap, distinct::Distinct};

#[derive(Clone, Copy)]
enum SelectionKind {
    Unit,
    Mask,
    IndexValue,
}

impl SelectionKind {
    fn of<E: 'static>(descriptor: &ExpressionDescriptor) -> Option<Self> {
        let ExpressionDescriptor::Lane { shape, .. } = descriptor else {
            return None;
        };

        match shape {
            LaneShapeDescriptor::Indexed { index, value } => {
                if let ValueRole::Index(IndexDescriptor::Domain(domain)) = value.role()
                    && domain.is::<E>()
                {
                    return Some(Self::IndexValue);
                }

                let IndexDescriptor::Domain(domain) = index else {
                    return None;
                };
                if !domain.is::<E>() {
                    return None;
                }

                match value.role() {
                    ValueRole::Unit => Some(Self::Unit),
                    ValueRole::Value if value.domain().is::<Mask>() => Some(Self::Mask),
                    _ => None,
                }
            }
            LaneShapeDescriptor::Bare { value } => {
                if let ValueRole::Index(IndexDescriptor::Domain(domain)) = value.role()
                    && domain.is::<E>()
                {
                    return Some(Self::IndexValue);
                }

                None
            }
        }
    }

    fn dropping<E: 'static>(descriptor: &ExpressionDescriptor) -> Option<Self> {
        let ExpressionDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed { .. },
            arity: ArityDescriptor::Multiple { .. },
        } = descriptor
        else {
            return None;
        };

        Self::of::<E>(descriptor)
    }
}

impl Series<DynExpression> {
    fn lane_terminal(&self) -> Result<DynTerminalLane, Box<Failure>> {
        let terminal = self.evaluate()?;

        let DynTerminal::Lane(lane) = terminal else {
            panic!("a lane selection descriptor must yield a lane terminal")
        };

        Ok(lane)
    }
}

macro_rules! implement_dynamic_selection {
    ($resolve:ident, $entity:ty, $arm:ident, $addresses:ident, $owned:expr) => {
        impl Series<DynExpression> {
            pub fn $resolve(
                &self,
                graphrecord: &GraphRecord,
            ) -> Option<GraphRecordResult<Vec<$entity>>> {
                let kind = SelectionKind::of::<$entity>(self.expression().descriptor())?;

                let selected = self.lane_terminal().and_then(|lane| match kind {
                    SelectionKind::Unit => {
                        let DynTerminalLane::IndexedUnit(container) = lane else {
                            panic!("a unit selection descriptor must yield a unit lane")
                        };

                        let mut collected = Vec::new();
                        let mut raised = Vec::new();

                        for (index, outcome) in container.into_elements() {
                            let DynIndexOwned::$arm(index) = index else {
                                panic!("a selection lane must carry its own index domain")
                            };

                            match outcome {
                                Ok(()) => collected.push(index),
                                Err(failure) => raised.push(*failure),
                            }
                        }

                        if !raised.is_empty() {
                            return Err(Failure::new(RaisedFailures::new(raised), LABEL));
                        }

                        Ok(collected)
                    }
                    SelectionKind::Mask => {
                        let DynTerminalLane::IndexedMask(container) = lane else {
                            panic!("a mask selection descriptor must yield a mask lane")
                        };

                        let elements = match container {
                            DynArityContainer::MultipleOrdered(elements)
                            | DynArityContainer::MultipleUnordered(elements) => elements,
                            DynArityContainer::Single(element) => {
                                let selected = match element {
                                    Some((index, Ok(true))) => {
                                        let DynIndexOwned::$arm(index) = index else {
                                            panic!(
                                                "a selection lane must carry its own index domain"
                                            )
                                        };

                                        vec![index]
                                    }
                                    Some((_, Ok(false))) | None => Vec::new(),
                                    Some((_, Err(failure))) => {
                                        return Err(Failure::new(
                                            RaisedFailures::new(vec![*failure]),
                                            LABEL,
                                        ));
                                    }
                                };

                                return Ok(selected);
                            }
                            DynArityContainer::Definite((index, outcome)) => {
                                let selected = match outcome {
                                    Ok(true) => {
                                        let DynIndexOwned::$arm(index) = index else {
                                            panic!(
                                                "a selection lane must carry its own index domain"
                                            )
                                        };

                                        vec![index]
                                    }
                                    Ok(false) => Vec::new(),
                                    Err(failure) => {
                                        return Err(Failure::new(
                                            RaisedFailures::new(vec![*failure]),
                                            LABEL,
                                        ));
                                    }
                                };

                                return Ok(selected);
                            }
                        };

                        let mut values: GrHashMap<$entity, bool> = GrHashMap::default();
                        let mut raised = Vec::new();

                        for (index, outcome) in elements {
                            let DynIndexOwned::$arm(index) = index else {
                                panic!("a selection lane must carry its own index domain")
                            };

                            match outcome {
                                Ok(value) => {
                                    values.insert(index, value);
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

                        for address in state_view.$addresses() {
                            let index = $owned(&state_view, address);

                            match values.get(&index).copied() {
                                Some(true) => collected.push(index),
                                Some(false) => {}
                                None => uncovered.push(index),
                            }
                        }

                        if !uncovered.is_empty() {
                            return Err(Failure::new(
                                UncoveredIndices::<$entity>::new(uncovered),
                                LABEL,
                            ));
                        }

                        Ok(collected)
                    }
                    SelectionKind::IndexValue => {
                        let outcomes = match lane {
                            DynTerminalLane::IndexedValue(container) => container
                                .into_elements()
                                .into_iter()
                                .map(|element| element.1)
                                .collect(),
                            DynTerminalLane::BareValue(container) => container.into_elements(),
                            _ => {
                                panic!(
                                    "an index-value selection descriptor must yield a value lane"
                                )
                            }
                        };

                        let mut collected = Vec::new();
                        let mut raised = Vec::new();

                        for outcome in outcomes {
                            match outcome {
                                Ok(DynValue::Index(DynIndexOwned::$arm(index))) => {
                                    collected.push(index);
                                }
                                Ok(_) => {
                                    panic!("a selection lane must carry its own index domain")
                                }
                                Err(failure) => raised.push(*failure),
                            }
                        }

                        if !raised.is_empty() {
                            return Err(Failure::new(RaisedFailures::new(raised), LABEL));
                        }

                        let collected: Vec<_> =
                            collected.into_iter().collect::<Distinct<_>>().into();

                        Ok(collected)
                    }
                });

                Some(selected.map_err(GraphRecordError::from))
            }
        }

        impl DynExpression {
            pub fn $resolve(
                &self,
                graphrecord: &GraphRecord,
            ) -> Option<GraphRecordResult<Vec<$entity>>> {
                graphrecord.query(self.clone()).$resolve(graphrecord)
            }
        }
    };
}

implement_dynamic_selection!(
    resolve_nodes,
    NodeIndex,
    Node,
    node_addresses,
    |state_view: &StateView<'_>, address| NodeIndex::from(state_view.node_index(address))
);
implement_dynamic_selection!(
    resolve_edges,
    EdgeIndex,
    Edge,
    edge_addresses,
    |state_view: &StateView<'_>, address| state_view.edge_index(address)
);
implement_dynamic_selection!(
    resolve_groups,
    GroupIndex,
    Group,
    group_addresses,
    |state_view: &StateView<'_>, address| state_view.group_index(address).clone()
);

macro_rules! implement_dynamic_dropping_selection {
    ($resolve:ident, $entity:ty, $arm:ident) => {
        impl Series<DynExpression> {
            pub fn $resolve(
                &self,
                graphrecord: &GraphRecord,
            ) -> Option<GraphRecordResult<Vec<$entity>>> {
                let kind = SelectionKind::dropping::<$entity>(self.expression().descriptor())?;

                let selected = self.lane_terminal().and_then(|lane| match kind {
                    SelectionKind::Unit => {
                        let DynTerminalLane::IndexedUnit(container) = lane else {
                            panic!("a unit selection descriptor must yield a unit lane")
                        };

                        let mut collected = Vec::new();
                        let mut raised = Vec::new();

                        for (index, outcome) in container.into_elements() {
                            let DynIndexOwned::$arm(index) = index else {
                                panic!("a selection lane must carry its own index domain")
                            };

                            match outcome {
                                Ok(()) => collected.push(index),
                                Err(failure) => raised.push(*failure),
                            }
                        }

                        if !raised.is_empty() {
                            return Err(Failure::new(RaisedFailures::new(raised), LABEL));
                        }

                        Ok(collected)
                    }
                    SelectionKind::Mask => {
                        let DynTerminalLane::IndexedMask(container) = lane else {
                            panic!("a mask selection descriptor must yield a mask lane")
                        };

                        let mut collected = Vec::new();
                        let mut raised = Vec::new();

                        for (index, outcome) in container.into_elements() {
                            match outcome {
                                Ok(true) => {
                                    let DynIndexOwned::$arm(index) = index else {
                                        panic!("a selection lane must carry its own index domain")
                                    };

                                    collected.push(index);
                                }
                                Ok(false) => {}
                                Err(failure) => raised.push(*failure),
                            }
                        }

                        if !raised.is_empty() {
                            return Err(Failure::new(RaisedFailures::new(raised), LABEL));
                        }

                        Ok(collected)
                    }
                    SelectionKind::IndexValue => {
                        let DynTerminalLane::IndexedValue(container) = lane else {
                            panic!("an index-value selection descriptor must yield a value lane")
                        };

                        let mut collected = Vec::new();
                        let mut raised = Vec::new();

                        for element in container.into_elements() {
                            match element.1 {
                                Ok(DynValue::Index(DynIndexOwned::$arm(index))) => {
                                    collected.push(index);
                                }
                                Ok(_) => {
                                    panic!("a selection lane must carry its own index domain")
                                }
                                Err(failure) => raised.push(*failure),
                            }
                        }

                        if !raised.is_empty() {
                            return Err(Failure::new(RaisedFailures::new(raised), LABEL));
                        }

                        Ok(collected)
                    }
                });

                let retained = selected.map(|selected| {
                    let present = selected
                        .into_iter()
                        .filter(|index| <$entity>::contains(graphrecord, index));

                    match kind {
                        SelectionKind::IndexValue => present.collect::<Distinct<_>>().into(),
                        SelectionKind::Unit | SelectionKind::Mask => present.collect(),
                    }
                });

                Some(retained.map_err(GraphRecordError::from))
            }
        }

        impl DynExpression {
            pub fn $resolve(
                &self,
                graphrecord: &GraphRecord,
            ) -> Option<GraphRecordResult<Vec<$entity>>> {
                graphrecord.query(self.clone()).$resolve(graphrecord)
            }
        }

        impl DynArgumentLane {
            pub fn $resolve(
                &self,
                graphrecord: &GraphRecord,
            ) -> Option<GraphRecordResult<Vec<$entity>>> {
                match self {
                    Self::Expression(expression) => expression.$resolve(graphrecord),
                    Self::Series(series) => series.$resolve(graphrecord),
                }
            }
        }
    };
}

implement_dynamic_dropping_selection!(resolve_dropping_nodes, NodeIndex, Node);
implement_dynamic_dropping_selection!(resolve_dropping_edges, EdgeIndex, Edge);
implement_dynamic_dropping_selection!(resolve_dropping_groups, GroupIndex, Group);
