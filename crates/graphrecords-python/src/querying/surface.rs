macro_rules! expression_surface {
    ($type:ty { $($extra:item)* }) => {
        #[pyo3::pymethods]
        impl $type {
            $($extra)*

            fn on_missing_drop(&self) -> PyResult<PyArgument> {
                PyExpression::dropping_argument(self.lane())
            }

            fn on_missing_replace(
                &self,
                replacement: &Bound<'_, PyAny>,
            ) -> PyResult<PyArgument> {
                PyExpression::replacing_argument(self.lane(), replacement)
            }

            fn filter(&self, mask: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("filter", &[PyExpression::mask_argument(mask)?])
            }

            fn and_(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("and", &[PyExpression::mask_argument(other)?])
            }

            fn or_(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("or", &[PyExpression::mask_argument(other)?])
            }

            fn xor(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("xor", &[PyExpression::mask_argument(other)?])
            }

            fn not_(&self) -> PyResult<Self> {
                self.invoke("not", &[])
            }

            fn first(&self) -> PyResult<Self> {
                self.invoke("first", &[])
            }

            fn last(&self) -> PyResult<Self> {
                self.invoke("last", &[])
            }

            fn reverse_order(&self) -> PyResult<Self> {
                self.invoke("reverse_order", &[])
            }

            fn shuffle(&self) -> PyResult<Self> {
                self.invoke("shuffle", &[])
            }

            fn unorder(&self) -> PyResult<Self> {
                self.invoke("unorder", &[])
            }

            fn sort(&self) -> PyResult<Self> {
                self.invoke("sort", &[])
            }

            fn sort_by(&self, key: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("sort_by", &[PyExpression::scalar_argument(key)?])
            }

            fn drop_duplicates(&self) -> PyResult<Self> {
                self.invoke("drop_duplicates", &[])
            }

            fn is_duplicated(&self) -> PyResult<Self> {
                self.invoke("is_duplicated", &[])
            }

            fn unique(&self) -> PyResult<Self> {
                self.invoke("unique", &[])
            }

            fn take(&self, elements: usize) -> PyResult<Self> {
                self.invoke("take", &[DynInvokeArgument::Position(elements)])
            }

            fn is_bool(&self) -> PyResult<Self> {
                self.invoke("is_bool", &[])
            }

            fn is_datetime(&self) -> PyResult<Self> {
                self.invoke("is_datetime", &[])
            }

            fn is_duration(&self) -> PyResult<Self> {
                self.invoke("is_duration", &[])
            }

            fn is_float(&self) -> PyResult<Self> {
                self.invoke("is_float", &[])
            }

            fn is_null(&self) -> PyResult<Self> {
                self.invoke("is_null", &[])
            }

            fn is_int(&self) -> PyResult<Self> {
                self.invoke("is_int", &[])
            }

            fn is_string(&self) -> PyResult<Self> {
                self.invoke("is_string", &[])
            }

            fn abs(&self) -> PyResult<Self> {
                self.invoke("abs", &[])
            }

            fn neg(&self) -> PyResult<Self> {
                self.invoke("neg", &[])
            }

            fn sign(&self) -> PyResult<Self> {
                self.invoke("sign", &[])
            }

            fn ceil(&self) -> PyResult<Self> {
                self.invoke("ceil", &[])
            }

            fn cbrt(&self) -> PyResult<Self> {
                self.invoke("cbrt", &[])
            }

            fn exp(&self) -> PyResult<Self> {
                self.invoke("exp", &[])
            }

            fn floor(&self) -> PyResult<Self> {
                self.invoke("floor", &[])
            }

            fn log(&self) -> PyResult<Self> {
                self.invoke("log", &[])
            }

            fn round(&self) -> PyResult<Self> {
                self.invoke("round", &[])
            }

            fn sqrt(&self) -> PyResult<Self> {
                self.invoke("sqrt", &[])
            }

            fn trim(&self) -> PyResult<Self> {
                self.invoke("trim", &[])
            }

            fn trim_start(&self) -> PyResult<Self> {
                self.invoke("trim_start", &[])
            }

            fn trim_end(&self) -> PyResult<Self> {
                self.invoke("trim_end", &[])
            }

            fn lowercase(&self) -> PyResult<Self> {
                self.invoke("lowercase", &[])
            }

            fn uppercase(&self) -> PyResult<Self> {
                self.invoke("uppercase", &[])
            }

            fn reverse(&self) -> PyResult<Self> {
                self.invoke("reverse", &[])
            }

            fn length(&self) -> PyResult<Self> {
                self.invoke("length", &[])
            }

            fn slice(&self, start: usize, end: usize) -> PyResult<Self> {
                self.invoke(
                    "slice",
                    &[
                        DynInvokeArgument::Position(start),
                        DynInvokeArgument::Position(end),
                    ],
                )
            }

            fn starts_with(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("starts_with", &[PyExpression::scalar_argument(argument)?])
            }

            fn ends_with(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("ends_with", &[PyExpression::scalar_argument(argument)?])
            }

            fn contains(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("contains", &[PyExpression::scalar_argument(argument)?])
            }

            fn matches(&self, pattern: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("matches", &[PyExpression::scalar_argument(pattern)?])
            }

            fn strip_prefix(&self, prefix: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("strip_prefix", &[PyExpression::scalar_argument(prefix)?])
            }

            fn strip_suffix(&self, suffix: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("strip_suffix", &[PyExpression::scalar_argument(suffix)?])
            }

            fn replace(&self, old: &Bound<'_, PyAny>, new: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "replace",
                    &[
                        PyExpression::scalar_argument(old)?,
                        PyExpression::scalar_argument(new)?,
                    ],
                )
            }

            fn replace_all(
                &self,
                old: &Bound<'_, PyAny>,
                new: &Bound<'_, PyAny>,
            ) -> PyResult<Self> {
                self.invoke(
                    "replace_all",
                    &[
                        PyExpression::scalar_argument(old)?,
                        PyExpression::scalar_argument(new)?,
                    ],
                )
            }

            fn pad_start(&self, width: usize, character: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "pad_start",
                    &[
                        DynInvokeArgument::Position(width),
                        PyExpression::scalar_argument(character)?,
                    ],
                )
            }

            fn pad_end(&self, width: usize, character: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "pad_end",
                    &[
                        DynInvokeArgument::Position(width),
                        PyExpression::scalar_argument(character)?,
                    ],
                )
            }

            fn split(&self, delimiter: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("split", &[PyExpression::scalar_argument(delimiter)?])
            }

            fn attribute(&self, attribute: PyAttributeName) -> PyResult<Self> {
                self.invoke(
                    "attribute",
                    &[DynInvokeArgument::Attribute(attribute.into())],
                )
            }

            fn attributes(&self) -> PyResult<Self> {
                self.invoke("attributes", &[])
            }

            fn resolve(&self) -> PyResult<Self> {
                self.invoke("resolve", &[])
            }

            fn select(&self) -> PyResult<Self> {
                self.invoke("select", &[])
            }

            fn parent_index(&self) -> PyResult<Self> {
                self.invoke("parent_index", &[])
            }

            fn child_index(&self) -> PyResult<Self> {
                self.invoke("child_index", &[])
            }

            fn has_attribute(&self, attribute: PyAttributeName) -> PyResult<Self> {
                self.invoke(
                    "has_attribute",
                    &[DynInvokeArgument::Attribute(attribute.into())],
                )
            }

            fn in_group(&self, group_index: PyGroupIndex) -> PyResult<Self> {
                self.invoke("in_group", &[DynInvokeArgument::Group(group_index.into())])
            }

            fn add(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "add",
                    &[PyExpression::value_argument(self.expression(), argument)?],
                )
            }

            fn subtract(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "subtract",
                    &[PyExpression::value_argument(self.expression(), argument)?],
                )
            }

            fn multiply(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "multiply",
                    &[PyExpression::value_argument(self.expression(), argument)?],
                )
            }

            fn power(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "power",
                    &[PyExpression::value_argument(self.expression(), argument)?],
                )
            }

            fn modulo(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "modulo",
                    &[PyExpression::value_argument(self.expression(), argument)?],
                )
            }

            fn divide(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "divide",
                    &[PyExpression::value_argument(self.expression(), argument)?],
                )
            }

            fn clip(&self, lower: &Bound<'_, PyAny>, upper: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "clip",
                    &[
                        PyExpression::value_argument(self.expression(), lower)?,
                        PyExpression::value_argument(self.expression(), upper)?,
                    ],
                )
            }

            fn cast(&self, target: PyCastTarget) -> PyResult<Self> {
                self.invoke("cast", &[DynInvokeArgument::CastTarget(target.into())])
            }

            fn equal_to(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "equal_to",
                    &[PyExpression::value_argument(self.expression(), argument)?],
                )
            }

            fn not_equal_to(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "not_equal_to",
                    &[PyExpression::value_argument(self.expression(), argument)?],
                )
            }

            fn greater_than(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "greater_than",
                    &[PyExpression::value_argument(self.expression(), argument)?],
                )
            }

            fn greater_than_or_equal_to(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "greater_than_or_equal_to",
                    &[PyExpression::value_argument(self.expression(), argument)?],
                )
            }

            fn less_than(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "less_than",
                    &[PyExpression::value_argument(self.expression(), argument)?],
                )
            }

            fn less_than_or_equal_to(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "less_than_or_equal_to",
                    &[PyExpression::value_argument(self.expression(), argument)?],
                )
            }

            fn is_in(&self, argument: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "is_in",
                    &[PyExpression::set_argument(self.expression(), argument)?],
                )
            }

            fn index(&self) -> PyResult<Self> {
                self.invoke("index", &[])
            }

            fn discard_index(&self) -> PyResult<Self> {
                self.invoke("discard_index", &[])
            }

            fn discard_value(&self) -> PyResult<Self> {
                self.invoke("discard_value", &[])
            }

            fn enumerate(&self) -> PyResult<Self> {
                self.invoke("enumerate", &[])
            }

            fn errors(&self) -> PyResult<Self> {
                self.invoke("errors", &[])
            }

            fn on_error_raise(&self) -> PyResult<Self> {
                self.invoke("on_error_raise", &[])
            }

            fn on_error_drop(&self) -> PyResult<Self> {
                self.invoke("on_error_drop", &[])
            }

            fn on_error_replace(&self, replacement: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "on_error_replace",
                    &[PyExpression::value_argument(self.expression(), replacement)?],
                )
            }

            fn on_error_raise_when(&self, condition: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "on_error_raise_when",
                    &[PyExpression::mask_argument(condition)?],
                )
            }

            fn kind(&self) -> PyResult<Self> {
                self.invoke("kind", &[])
            }

            fn name(&self) -> PyResult<Self> {
                self.invoke("name", &[])
            }

            fn count(&self) -> PyResult<Self> {
                self.invoke("count", &[])
            }

            fn sum(&self) -> PyResult<Self> {
                self.invoke("sum", &[])
            }

            fn mean(&self) -> PyResult<Self> {
                self.invoke("mean", &[])
            }

            fn std(&self) -> PyResult<Self> {
                self.invoke("std", &[])
            }

            fn var(&self) -> PyResult<Self> {
                self.invoke("var", &[])
            }

            fn all(&self) -> PyResult<Self> {
                self.invoke("all", &[])
            }

            fn any(&self) -> PyResult<Self> {
                self.invoke("any", &[])
            }

            fn max(&self) -> PyResult<Self> {
                self.invoke("max", &[])
            }

            fn min(&self) -> PyResult<Self> {
                self.invoke("min", &[])
            }

            fn median(&self) -> PyResult<Self> {
                self.invoke("median", &[])
            }

            fn mode(&self) -> PyResult<Self> {
                self.invoke("mode", &[])
            }

            fn product(&self) -> PyResult<Self> {
                self.invoke("product", &[])
            }

            fn n_unique(&self) -> PyResult<Self> {
                self.invoke("n_unique", &[])
            }

            fn random(&self) -> PyResult<Self> {
                self.invoke("random", &[])
            }

            #[pyo3(signature = (direction=None))]
            fn edges(&self, direction: Option<PyEdgeDirection>) -> PyResult<Self> {
                if PyExpression::group_entity_lane(self.expression().descriptor()) {
                    if direction.is_some() {
                        return Err(pyo3::exceptions::PyTypeError::new_err(
                            "Group edges carry no direction",
                        ));
                    }

                    return self.invoke("edges", &[]);
                }

                let direction = direction.unwrap_or(PyEdgeDirection::Both);

                self.invoke("edges", &[DynInvokeArgument::Direction(direction.into())])
            }

            #[pyo3(signature = (direction=PyEdgeDirection::Both))]
            fn neighbors(&self, direction: PyEdgeDirection) -> PyResult<Self> {
                self.invoke(
                    "neighbors",
                    &[DynInvokeArgument::Direction(direction.into())],
                )
            }

            #[pyo3(signature = (direction=None))]
            fn via_edges(&self, direction: Option<PyEdgeDirection>) -> PyResult<Self> {
                if PyExpression::group_entity_lane(self.expression().descriptor()) {
                    if direction.is_some() {
                        return Err(pyo3::exceptions::PyTypeError::new_err(
                            "Group edges carry no direction",
                        ));
                    }

                    return self.invoke("via_edges", &[]);
                }

                let direction = direction.unwrap_or(PyEdgeDirection::Both);

                self.invoke(
                    "via_edges",
                    &[DynInvokeArgument::Direction(direction.into())],
                )
            }

            #[pyo3(signature = (direction=PyEdgeDirection::Both))]
            fn via_neighbors(&self, direction: PyEdgeDirection) -> PyResult<Self> {
                self.invoke(
                    "via_neighbors",
                    &[DynInvokeArgument::Direction(direction.into())],
                )
            }

            fn nodes(&self) -> PyResult<Self> {
                self.invoke("nodes", &[])
            }

            fn via_nodes(&self) -> PyResult<Self> {
                self.invoke("via_nodes", &[])
            }

            fn groups(&self) -> PyResult<Self> {
                self.invoke("groups", &[])
            }

            fn via_groups(&self) -> PyResult<Self> {
                self.invoke("via_groups", &[])
            }

            fn node_count(&self) -> PyResult<Self> {
                self.invoke("node_count", &[])
            }

            fn edge_count(&self) -> PyResult<Self> {
                self.invoke("edge_count", &[])
            }

            fn source_node(&self) -> PyResult<Self> {
                self.via_source_node()?.select()
            }

            fn target_node(&self) -> PyResult<Self> {
                self.via_target_node()?.select()
            }

            fn via_source_node(&self) -> PyResult<Self> {
                self.invoke("via_source_node", &[])
            }

            fn via_target_node(&self) -> PyResult<Self> {
                self.invoke("via_target_node", &[])
            }

            fn group_by(&self, key: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("group_by", &[PyExpression::scalar_argument(key)?])
            }

            fn having(&self, predicate: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("having", &[PyExpression::mask_argument(predicate)?])
            }

            fn keys(&self) -> PyResult<Self> {
                self.invoke("keys", &[])
            }

            fn ungroup(&self) -> PyResult<Self> {
                self.invoke("ungroup", &[])
            }

            fn ungroup_keyed(&self) -> PyResult<Self> {
                self.invoke("ungroup_keyed", &[])
            }

            fn broadcast(&self) -> PyResult<Self> {
                self.invoke("broadcast", &[])
            }

            fn broadcast_via(&self, via: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke(
                    "broadcast_via",
                    &[PyExpression::expression_argument(via)?],
                )
            }

            fn bucket_errors(&self) -> PyResult<Self> {
                self.invoke("bucket_errors", &[])
            }

            fn key_errors(&self) -> PyResult<Self> {
                self.invoke("key_errors", &[])
            }

            fn on_bucket_error_drop(&self) -> PyResult<Self> {
                self.invoke("on_bucket_error_drop", &[])
            }

            fn on_bucket_error_raise(&self) -> PyResult<Self> {
                self.invoke("on_bucket_error_raise", &[])
            }

            fn on_key_error_drop(&self) -> PyResult<Self> {
                self.invoke("on_key_error_drop", &[])
            }

            fn on_key_error_raise(&self) -> PyResult<Self> {
                self.invoke("on_key_error_raise", &[])
            }

            fn transition(&self, target: PyValueTarget) -> PyResult<Self> {
                self.invoke(
                    "transition",
                    &[DynInvokeArgument::ValueTarget(target.into())],
                )
            }

            fn inherit(&self, values: &Bound<'_, PyAny>) -> PyResult<Self> {
                self.invoke("inherit", &[PyExpression::scalar_argument(values)?])
            }
        }
    };
}

pub(super) use expression_surface;
