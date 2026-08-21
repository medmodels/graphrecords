use super::model::{
    Field, Kernel, KernelInput, Manifest, Parameter, Policy, PolicyCall, Scope, ValueArgument,
    ViaArgument, type_application, type_ident,
};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, Ident, Path, Result, Type};

pub fn applier(manifest: &Manifest, kernel: &Kernel, query: &Path) -> Result<TokenStream> {
    let applier = Applier {
        manifest,
        kernel,
        query,
    };

    match manifest.scope {
        Scope::Lane => applier.lane(),
        Scope::Element => applier.element(),
        Scope::Group => applier.group(),
    }
}

struct Applier<'a> {
    manifest: &'a Manifest,
    kernel: &'a Kernel,
    query: &'a Path,
}

struct CaptureRoad<'a> {
    policy: Option<&'a Policy>,
    selector: Option<&'a Path>,
    receiver: Option<&'a Ident>,
    fields: &'a [Field],
}

struct Expansion<'a> {
    index: &'a Type,
    value: &'a Type,
    child: &'a Type,
    out_value: &'a Type,
    order: &'a Type,
}

struct SourceLane<'a> {
    arities: &'a [(TokenStream, TokenStream)],
    series: bool,
}

impl Applier<'_> {
    fn lane(&self) -> Result<TokenStream> {
        let query = self.query;
        let KernelInput::Lane { arity, .. } = &self.kernel.input else {
            return Err(Error::new_spanned(
                &self.kernel.output,
                "a lane kernel input must pair a shape with an arity",
            ));
        };

        if let Some(arguments) = type_application(arity, "Multiple") {
            let [order] = arguments[..] else {
                return Err(Error::new_spanned(arity, "the arity has no lane applier"));
            };
            let Some(order) = type_ident(order) else {
                return Err(Error::new_spanned(arity, "the arity has no lane applier"));
            };

            return match order.to_string().as_str() {
                "Ordered" => self.lane_function(
                    &quote!(#query::Ordered),
                    &quote!(#query::Multiple<#query::Ordered>),
                ),
                "Unordered" => self.lane_function(
                    &quote!(#query::Unordered),
                    &quote!(#query::Multiple<#query::Unordered>),
                ),
                _ => self.lane_runtime_order(),
            };
        }

        match type_ident(arity).map(Ident::to_string).as_deref() {
            Some("Single") => {
                self.lane_function(&quote!(#query::Unordered), &quote!(#query::Single))
            }
            Some("Definite") => {
                self.lane_function(&quote!(#query::Unordered), &quote!(#query::Definite))
            }
            _ => Err(Error::new_spanned(arity, "the arity has no lane applier")),
        }
    }

    fn lane_function(&self, order: &TokenStream, arity: &TokenStream) -> Result<TokenStream> {
        let body = self.entity_dispatch(|entity| self.lane_entity(entity, order, arity))?;

        Ok(self.apply_function(&body))
    }

    fn lane_runtime_order(&self) -> Result<TokenStream> {
        let query = self.query;
        let ordered = self.entity_dispatch(|entity| {
            self.lane_entity(
                entity,
                &quote!(#query::Ordered),
                &quote!(#query::Multiple<#query::Ordered>),
            )
        })?;
        let unordered = self.entity_dispatch(|entity| {
            self.lane_entity(
                entity,
                &quote!(#query::Unordered),
                &quote!(#query::Multiple<#query::Unordered>),
            )
        })?;

        let body = quote! {
            match input.descriptor().lane_arity() {
                #query::registry::ArityDescriptor::Multiple {
                    order: #query::registry::OrderDescriptor::Ordered,
                } => #ordered,
                #query::registry::ArityDescriptor::Multiple {
                    order: #query::registry::OrderDescriptor::Unordered,
                } => #unordered,
                _ => panic!("registry selected a multiple-lane operation for a different arity"),
            }
        };

        Ok(self.apply_function(&body))
    }

    fn lane_entity(
        &self,
        entity: &TokenStream,
        order: &TokenStream,
        arity: &TokenStream,
    ) -> Result<TokenStream> {
        self.receiver_dispatch(|dynamic_value| self.lane_build(dynamic_value, entity, order, arity))
    }

    fn lane_build(
        &self,
        dynamic_value: &TokenStream,
        entity: &TokenStream,
        order: &TokenStream,
        arity: &TokenStream,
    ) -> Result<TokenStream> {
        let query = self.query;
        let KernelInput::Lane { shape, .. } = &self.kernel.input else {
            return Err(Error::new_spanned(
                &self.kernel.output,
                "a lane kernel input must pair a shape with an arity",
            ));
        };
        let out_expression = &self.kernel.output;
        let apply = self.lane_apply(&quote!(#shape), arity, out_expression);
        let road = CaptureRoad {
            policy: self.manifest.policy.as_ref(),
            selector: None,
            receiver: None,
            fields: &self.kernel.fields,
        };

        if self.manifest.method == "group_by"
            && self.kernel.fields.is_empty()
            && let Some((index, value, key, input_order)) = self.group_by_parameters()
            && let Some(argument) = self.kernel.plain_value_argument()
        {
            let input_order = input_order.map(|input_order| quote!(type #input_order = #order;));
            let selected = self.selected_argument(&apply, &road, argument, Some(key), true)?;

            return Ok(quote! {
                {
                    type #index = #query::dynamic::DynIndex;
                    type #value = #dynamic_value;
                    #input_order

                    #selected
                }
            });
        }

        let aliases = self.aliases(
            order,
            entity,
            &quote!(#query::Indexed<#query::dynamic::DynIndex, #dynamic_value>),
            arity,
        );
        let arguments = self.arguments_road(&apply, &road)?;

        Ok(quote! {
            {
                #aliases
                #arguments
            }
        })
    }

    fn group_by_parameters(&self) -> Option<(&Ident, &Ident, &Ident, Option<&Ident>)> {
        match &self.kernel.parameters[..] {
            [index, value, key]
                if index.is_bare("IndexDomain")
                    && value.is_bare("ValueDomain")
                    && key.is_bare("ValueGrouping") =>
            {
                Some((&index.name, &value.name, &key.name, None))
            }
            [index, value, key, input_order]
                if index.is_bare("IndexDomain")
                    && value.is_bare("ValueDomain")
                    && key.is_bare("ValueGrouping")
                    && input_order.is_bare("OrderState") =>
            {
                Some((&index.name, &value.name, &key.name, Some(&input_order.name)))
            }
            _ => None,
        }
    }

    fn lane_apply(
        &self,
        shape: &TokenStream,
        arity: &TokenStream,
        out_expression: &Type,
    ) -> TokenStream {
        let query = self.query;
        let operation = &self.manifest.operation;

        quote! {
            {
                type DynamicOperation =
                    #query::dynamic::DynLaneOperation<#operation, #shape, #arity, #out_expression>;
                type DynamicShape = <#shape as #query::dynamic::DynShapeProjection>::Dynamic;

                let operation = DynamicOperation::new(operation);

                let #query::dynamic::DynHandle::Lane(handle) = &input.handle else {
                    return #query::dynamic::apply_grouped_operation::<DynamicShape, #arity, _>(
                        input, operation, output,
                    );
                };

                let handles = <DynamicShape as #query::dynamic::DynLaneState>::handles(handle);

                #query::dynamic::apply_lane_operation::<DynamicShape, #arity, _>(
                    handles, operation, output,
                )
            }
        }
    }

    fn element(&self) -> Result<TokenStream> {
        if self.kernel.set_argument().is_some() {
            let body = self.receiver_dispatch(|dynamic_value| self.set_build(dynamic_value))?;

            return Ok(self.apply_function(&body));
        }

        let expansion = self.expansion_form();
        let body = self.entity_dispatch(|entity| {
            self.receiver_dispatch(|dynamic_value| {
                self.element_build(dynamic_value, entity, expansion.as_ref())
            })
        })?;

        Ok(self.apply_function(&body))
    }

    fn expansion_form(&self) -> Option<Expansion<'_>> {
        let input = type_application(self.kernel.shape(), "Indexed")?;
        let [index, value] = input[..] else {
            return None;
        };

        let output = type_application(&self.kernel.output, "Indexed")?;
        let [expanded, out_value] = output[..] else {
            return None;
        };
        let expanded = type_application(expanded, "ExpandedIndex")?;
        let [_, child] = expanded[..] else {
            return None;
        };

        let emission = type_application(self.kernel.emission.as_ref()?, "Expanding")?;
        let [order] = emission[..] else {
            return None;
        };

        Some(Expansion {
            index,
            value,
            child,
            out_value,
            order,
        })
    }

    fn element_build(
        &self,
        dynamic_value: &TokenStream,
        entity: &TokenStream,
        expansion: Option<&Expansion<'_>>,
    ) -> Result<TokenStream> {
        let query = self.query;
        let shape = self.kernel.shape();
        let out_shape = &self.kernel.output;
        let road = CaptureRoad {
            policy: self.manifest.policy.as_ref(),
            selector: self.kernel.selector.as_ref(),
            receiver: self.kernel.receiver.as_ref(),
            fields: &self.kernel.fields,
        };

        if self.manifest.method == "inherit"
            && expansion.is_none()
            && road.selector.is_none()
            && road.receiver.is_none()
            && road.fields.is_empty()
            && let Some((parent, child, value, out_value)) = self.inherit_parameters()
            && let Some(argument) = self.kernel.plain_value_argument()
        {
            let apply = self.element_apply(shape, out_shape);
            let selected =
                self.selected_argument(&apply, &road, argument, Some(out_value), false)?;

            return Ok(quote! {
                {
                    type #parent = #query::dynamic::DynIndex;
                    type #child = #query::dynamic::DynIndex;
                    type #value = #dynamic_value;

                    #selected
                }
            });
        }

        let aliases = self.aliases(
            &quote!(#query::Unordered),
            entity,
            &quote!(#query::Indexed<#query::dynamic::DynIndex, #dynamic_value>),
            &quote!(#query::Multiple<#query::Unordered>),
        );
        let apply = expansion.map_or_else(
            || self.element_apply(shape, out_shape),
            |expansion| self.expansion_apply(expansion),
        );
        let arguments = self.arguments_road(&apply, &road)?;

        Ok(quote! {
            {
                #aliases
                #arguments
            }
        })
    }

    fn inherit_parameters(&self) -> Option<(&Ident, &Ident, &Ident, &Ident)> {
        match &self.kernel.parameters[..] {
            [parent, child, value, out_value]
                if parent.is_bare("IndexDomain")
                    && child.is_bare("IndexDomain")
                    && value.is_bare("ValueDomain")
                    && out_value.is_bare("ValueDomain") =>
            {
                Some((&parent.name, &child.name, &value.name, &out_value.name))
            }
            _ => None,
        }
    }

    fn set_build(&self, dynamic_value: &TokenStream) -> Result<TokenStream> {
        let query = self.query;
        let shape = self.kernel.shape();
        let out_shape = &self.kernel.output;
        let name = &self
            .kernel
            .set_argument()
            .expect("the set road requires a set argument")
            .name;
        let road = CaptureRoad {
            policy: None,
            selector: None,
            receiver: None,
            fields: &[],
        };

        let aliases = self.aliases(
            &quote!(#query::Unordered),
            &quote!(#query::dynamic::DynIndex),
            &quote!(#query::Indexed<#query::dynamic::DynIndex, #dynamic_value>),
            &quote!(#query::Multiple<#query::Unordered>),
        );
        let capture = self.capture(&road, &[quote!(set)])?;
        let apply = self.element_apply(shape, out_shape);
        let bare_shape = quote!(#query::Bare<#dynamic_value>);
        let indexed_shape = quote!(#query::Indexed<#query::dynamic::DynIndex, #dynamic_value>);
        let multiple_arities = [
            (
                quote!(MultipleOrdered),
                quote!(#query::Multiple<#query::Ordered>),
            ),
            (
                quote!(MultipleUnordered),
                quote!(#query::Multiple<#query::Unordered>),
            ),
        ];
        let bare_arities = [
            (quote!(Single), quote!(#query::Single)),
            (quote!(Definite), quote!(#query::Definite)),
        ];
        let every_arity = [
            multiple_arities[0].clone(),
            multiple_arities[1].clone(),
            bare_arities[0].clone(),
            bare_arities[1].clone(),
        ];
        let expression_lane = SourceLane {
            arities: &every_arity,
            series: false,
        };
        let bare_series_lane = SourceLane {
            arities: &every_arity,
            series: true,
        };
        let indexed_series_lane = SourceLane {
            arities: &every_arity,
            series: true,
        };
        let bare_expression =
            self.set_arity(&road, name, &bare_shape, shape, out_shape, &expression_lane)?;
        let indexed_expression = self.set_arity(
            &road,
            name,
            &indexed_shape,
            shape,
            out_shape,
            &expression_lane,
        )?;
        let bare_series = self.set_arity(
            &road,
            name,
            &bare_shape,
            shape,
            out_shape,
            &bare_series_lane,
        )?;
        let indexed_series = self.set_arity(
            &road,
            name,
            &indexed_shape,
            shape,
            out_shape,
            &indexed_series_lane,
        )?;

        Ok(quote! {
            {
                #aliases

                let source = #query::dynamic::invoke_argument_source(arguments, 0);

                if source.is_literal_set() {
                    type #name = Vec<<#dynamic_value as #query::dynamic::DynSetLiteral>::Element>;

                    let set = <#dynamic_value as #query::dynamic::DynSetLiteral>::literal(source);
                    let operation = #capture;

                    return #apply;
                }

                match source.as_lane() {
                    #query::dynamic::DynArgumentLane::Expression(expression) => {
                        let #query::dynamic::DynHandle::Lane(lane) = &expression.handle else {
                            panic!("registry admitted a grouped expression where a dynamic set source is required")
                        };

                        match lane {
                            #query::dynamic::DynLaneHandle::BareValue(_)
                            | #query::dynamic::DynLaneHandle::BareMask(_) => {
                                #bare_expression
                            }
                            _ => {
                                #indexed_expression
                            }
                        }
                    }
                    #query::dynamic::DynArgumentLane::Series(series) => {
                        let #query::dynamic::DynHandle::Lane(lane) = &series.expression().handle else {
                            panic!("registry admitted a grouped series where a dynamic set source is required")
                        };

                        match lane {
                            #query::dynamic::DynLaneHandle::BareValue(_)
                            | #query::dynamic::DynLaneHandle::BareMask(_) => {
                                #bare_series
                            }
                            _ => {
                                #indexed_series
                            }
                        }
                    }
                }
            }
        })
    }

    fn set_arity(
        &self,
        road: &CaptureRoad<'_>,
        name: &Ident,
        set_shape: &TokenStream,
        shape: &Type,
        out_shape: &Type,
        lane: &SourceLane<'_>,
    ) -> Result<TokenStream> {
        let query = self.query;
        let mut branches = TokenStream::new();

        for (handle, arity) in lane.arities {
            let capture = self.capture(road, &[quote!(set)])?;
            let apply = self.element_apply(shape, out_shape);
            let source = if lane.series {
                quote! {
                    type #name = #query::Series<
                        #query::expressions::ExpressionHandle<#set_shape, #arity>,
                    >;

                    let set = series.bind(handle.clone());
                }
            } else {
                quote! {
                    type #name = #query::expressions::ExpressionHandle<#set_shape, #arity>;

                    let set = handle.clone();
                }
            };

            branches.extend(quote! {
                #query::dynamic::DynArityHandle::#handle(handle) => {
                    #source

                    let operation = #capture;

                    #apply
                },
            });
        }

        Ok(quote! {
            match <#set_shape as #query::dynamic::DynLaneState>::handles(lane) {
                #branches
            }
        })
    }

    fn element_apply(&self, shape: &Type, out_shape: &Type) -> TokenStream {
        let query = self.query;
        let operation = &self.manifest.operation;
        let apply = self.shape_apply(&quote!(DynamicOperation::new(operation)));

        quote! {
            {
                type Emission = <#operation as #query::operations::ElementKernel<#shape>>::Emission;
                type DynamicOperation =
                    #query::dynamic::DynElementOperation<#operation, #shape, #out_shape, Emission>;
                type DynamicShape = <#shape as #query::dynamic::DynShapeProjection>::Dynamic;

                #apply
            }
        }
    }

    fn expansion_apply(&self, expansion: &Expansion<'_>) -> TokenStream {
        let query = self.query;
        let operation = &self.manifest.operation;
        let index = expansion.index;
        let value = expansion.value;
        let child = expansion.child;
        let out_value = expansion.out_value;
        let order = expansion.order;
        let apply = self.shape_apply(&quote!(DynamicOperation::new(operation)));

        quote! {
            {
                type DynamicOperation = #query::dynamic::DynExpansionOperation<
                    #operation,
                    #index,
                    #value,
                    #child,
                    #out_value,
                    #order,
                >;
                type Shape = #query::Indexed<#index, #value>;
                type DynamicShape = <Shape as #query::dynamic::DynShapeProjection>::Dynamic;

                #apply
            }
        }
    }

    fn shape_apply(&self, operation_value: &TokenStream) -> TokenStream {
        let query = self.query;

        quote! {
            {
                let operation = #operation_value;
                let output = output;

                let #query::dynamic::DynHandle::Lane(handle) = &input.handle else {
                    return match input.descriptor().lane_arity() {
                        #query::registry::ArityDescriptor::Multiple {
                            order: #query::registry::OrderDescriptor::Ordered,
                        } => #query::dynamic::apply_grouped_operation::<
                            DynamicShape,
                            #query::Multiple<#query::Ordered>,
                            _,
                        >(input, operation, output),
                        #query::registry::ArityDescriptor::Multiple {
                            order: #query::registry::OrderDescriptor::Unordered,
                        } => #query::dynamic::apply_grouped_operation::<
                            DynamicShape,
                            #query::Multiple<#query::Unordered>,
                            _,
                        >(input, operation, output),
                        #query::registry::ArityDescriptor::Single => {
                            #query::dynamic::apply_grouped_operation::<DynamicShape, #query::Single, _>(
                                input, operation, output,
                            )
                        }
                        #query::registry::ArityDescriptor::Definite => {
                            #query::dynamic::apply_grouped_operation::<DynamicShape, #query::Definite, _>(
                                input, operation, output,
                            )
                        }
                    };
                };

                let handles = <DynamicShape as #query::dynamic::DynLaneState>::handles(handle);

                match handles {
                    #query::dynamic::DynArityHandle::MultipleOrdered(_) => {
                        #query::dynamic::apply_lane_operation::<
                            DynamicShape,
                            #query::Multiple<#query::Ordered>,
                            _,
                        >(handles, operation, output)
                    }
                    #query::dynamic::DynArityHandle::MultipleUnordered(_) => {
                        #query::dynamic::apply_lane_operation::<
                            DynamicShape,
                            #query::Multiple<#query::Unordered>,
                            _,
                        >(handles, operation, output)
                    }
                    #query::dynamic::DynArityHandle::Single(_) => {
                        #query::dynamic::apply_lane_operation::<DynamicShape, #query::Single, _>(
                            handles, operation, output,
                        )
                    }
                    #query::dynamic::DynArityHandle::Definite(_) => {
                        #query::dynamic::apply_lane_operation::<DynamicShape, #query::Definite, _>(
                            handles, operation, output,
                        )
                    }
                }
            }
        }
    }

    fn group(&self) -> Result<TokenStream> {
        if let Some(policy) = &self.manifest.policy
            && policy.constructor.is_none()
            && self.lane_payload().is_some()
            && self.kernel.arguments.is_empty()
            && self.kernel.where_owned.is_none()
            && type_ident(self.kernel.shape()).is_some()
        {
            return Ok(self.group_policy(policy));
        }

        if let Some(via) = self.kernel.via_argument() {
            return self.group_via(via);
        }

        self.group_kernel()
    }

    fn lane_payload(&self) -> Option<&Ident> {
        match &self.kernel.parameters[..] {
            [payload] if payload.is_bare("Lane") => Some(&payload.name),
            _ => None,
        }
    }

    fn group_keys(&self) -> Result<(&Ident, &Ident)> {
        let Some(group) = &self.kernel.group else {
            return Err(Error::new_spanned(
                &self.kernel.output,
                "a group kernel must declare its group domains",
            ));
        };

        Ok((&group.member, &group.key))
    }

    fn group_road_error(&self) -> Error {
        Error::new_spanned(&self.kernel.output, "the kernel has no group applier")
    }

    fn group_policy(&self, policy: &Policy) -> TokenStream {
        let query = self.query;
        let operation = &self.manifest.operation;
        let method = &self.manifest.method;
        let policy = &policy.path;

        let body = quote! {
            let _ = arguments;
            let capture = #query::dynamic::OperationCapture::<#operation>::capture();
            let operation = capture.#method(#policy).operation();

            #query::dynamic::apply_group_operation(input, operation, output)
        };

        self.apply_function(&body)
    }

    fn group_via(&self, via: &ViaArgument) -> Result<TokenStream> {
        let query = self.query;
        let (member, key) = self.group_keys()?;
        let via_index = &via.index;
        let via_value = &via.value;

        let handle = type_application(self.kernel.shape(), "ExpressionHandle")
            .ok_or_else(|| self.group_road_error())?;
        let [payload, payload_arity] = handle[..] else {
            return Err(self.group_road_error());
        };
        let payload_arity = type_ident(payload_arity).ok_or_else(|| self.group_road_error())?;

        if let Some(indexed) = type_application(payload, "Indexed") {
            let [payload_index, payload_value] = indexed[..] else {
                return Err(self.group_road_error());
            };
            let payload_index = type_ident(payload_index).ok_or_else(|| self.group_road_error())?;
            let payload_value = type_ident(payload_value).ok_or_else(|| self.group_road_error())?;

            let dispatch = self.via_dispatch(
                via,
                &quote!(#query::Indexed<#payload_index, #payload_value>),
                payload_arity,
            );
            let roads = self.via_body(payload_value, via_value, &dispatch)?;
            let body = quote! {
                type #member = #query::dynamic::DynIndex;
                type #key = #query::dynamic::DynIndex;
                type #via_index = #query::dynamic::DynIndex;
                type #payload_index = #query::dynamic::DynIndex;

                let _ = std::marker::PhantomData::<
                    fn() -> (#member, #key, #via_index, #payload_index),
                >;

                #roads
            };

            return Ok(self.apply_function(&body));
        }

        if let Some(bare) = type_application(payload, "Bare") {
            let [payload_value] = bare[..] else {
                return Err(self.group_road_error());
            };
            let payload_value = type_ident(payload_value).ok_or_else(|| self.group_road_error())?;

            let dispatch =
                self.via_dispatch(via, &quote!(#query::Bare<#payload_value>), payload_arity);
            let roads = self.via_body(payload_value, via_value, &dispatch)?;
            let body = quote! {
                type #member = #query::dynamic::DynIndex;
                type #key = #query::dynamic::DynIndex;
                type #via_index = #query::dynamic::DynIndex;

                let _ = std::marker::PhantomData::<fn() -> (#member, #key, #via_index)>;

                #roads
            };

            return Ok(self.apply_function(&body));
        }

        Err(self.group_road_error())
    }

    fn via_body(
        &self,
        payload_value: &Ident,
        via_value: &Ident,
        dispatch: &TokenStream,
    ) -> Result<TokenStream> {
        let query = self.query;
        let roads = self.receiver_dispatch(|dynamic_value| {
            Ok(quote! {
                {
                    type #payload_value = #dynamic_value;

                    #dispatch
                }
            })
        })?;

        Ok(quote! {
            let via = #query::dynamic::invoke_lane(arguments, 0);
            let transitioned;
            let via = if matches!(
                #query::dynamic::innermost_lane_kind(via.descriptor()),
                #query::dynamic::DynLaneKind::IndexedMask
            ) {
                transitioned = via.erase_mask_lane();
                &transitioned
            } else {
                via
            };

            type #via_value = #query::dynamic::DynValue;

            #roads
        })
    }

    fn via_dispatch(
        &self,
        via: &ViaArgument,
        payload_shape: &TokenStream,
        payload_arity: &Ident,
    ) -> TokenStream {
        let query = self.query;
        let via_value = &via.value;
        let multiple_arities = [
            (
                quote!(MultipleOrdered),
                quote!(#query::Multiple<#query::Ordered>),
            ),
            (
                quote!(MultipleUnordered),
                quote!(#query::Multiple<#query::Unordered>),
            ),
        ];
        let every_arity = [
            multiple_arities[0].clone(),
            multiple_arities[1].clone(),
            (quote!(Single), quote!(#query::Single)),
            (quote!(Definite), quote!(#query::Definite)),
        ];
        let expression_lane = SourceLane {
            arities: &every_arity,
            series: false,
        };
        let series_lane = SourceLane {
            arities: &multiple_arities,
            series: true,
        };
        let expression = self.via_arities(via, payload_shape, payload_arity, &expression_lane);
        let series = self.via_arities(via, payload_shape, payload_arity, &series_lane);

        quote! {
            {
                type ViaShape = #query::Indexed<#query::dynamic::DynIndex, #via_value>;

                match via {
                    #query::dynamic::DynArgumentLane::Expression(expression) => {
                        let #query::dynamic::DynHandle::Lane(lane) = &expression.handle else {
                            panic!("registry admitted a grouped expression where a dynamic via lane is required")
                        };

                        #expression
                    }
                    #query::dynamic::DynArgumentLane::Series(series) => {
                        let #query::dynamic::DynHandle::Lane(lane) = &series.expression().handle else {
                            panic!("registry admitted a grouped series where a dynamic via lane is required")
                        };

                        #series
                    }
                }
            }
        }
    }

    fn via_arities(
        &self,
        via: &ViaArgument,
        payload_shape: &TokenStream,
        payload_arity: &Ident,
        lane: &SourceLane<'_>,
    ) -> TokenStream {
        let query = self.query;
        let mut branches = TokenStream::new();

        for (handle, arity) in lane.arities {
            let arm = self.via_arm(via, arity, payload_shape, payload_arity, lane.series);
            branches.extend(quote! {
                #query::dynamic::DynArityHandle::#handle(handle) => #arm,
            });
        }

        if lane.series {
            branches.extend(quote! {
                _ => panic!("registry admitted a series via source outside the series arity roster"),
            });
        }

        quote! {
            match <ViaShape as #query::dynamic::DynLaneState>::handles(lane) {
                #branches
            }
        }
    }

    fn via_arm(
        &self,
        via: &ViaArgument,
        arity: &TokenStream,
        payload_shape: &TokenStream,
        payload_arity: &Ident,
        series: bool,
    ) -> TokenStream {
        let query = self.query;
        let operation = &self.manifest.operation;
        let method = &self.manifest.method;
        let via_arity = &via.arity;
        let argument = &via.name;
        let source = if series {
            quote! {
                type #argument = #query::Series<
                    #query::expressions::ExpressionHandle<ViaShape, #via_arity>,
                >;

                let via_source = series.bind(handle.clone());
            }
        } else {
            quote! {
                type #argument = #query::expressions::ExpressionHandle<ViaShape, #via_arity>;

                let via_source = handle.clone();
            }
        };

        quote! {
            {
                type #via_arity = #arity;
                #source

                let capture = #query::dynamic::OperationCapture::<#operation>::capture();
                let operation = capture.#method(via_source).operation();
                let operation = #query::dynamic::DynGroupOperation::<
                    #operation,
                    #payload_shape,
                    #payload_arity,
                >::new(operation);

                #query::dynamic::apply_group_operation(input, operation, output)
            }
        }
    }

    fn group_kernel(&self) -> Result<TokenStream> {
        match &self.kernel.parameters[..] {
            [payload] if payload.is_bare("Lane") => self.group_payload(&payload.name),
            [value] if value.is_bare("BareValueDomain") => self.group_bare(&value.name),
            [index, value] if index.is_bare("IndexDomain") && value.is_bare("ValueDomain") => {
                self.group_indexed(&index.name, &value.name)
            }
            [index, value, order]
                if index.is_bare("IndexDomain")
                    && value.is_bare("ValueDomain")
                    && order.is_bare("OrderState") =>
            {
                self.group_indexed_multiple(&index.name, &value.name, &order.name)
            }
            [value, order] if value.is_bare("BareValueDomain") && order.is_bare("OrderState") => {
                self.group_bare_multiple(&value.name, &order.name)
            }
            [shape, arity] if shape.is_bare("ElementShape") && arity.is_bare("Arity") => {
                self.group_shapes(&shape.name, &arity.name)
            }
            _ => Err(self.group_road_error()),
        }
    }

    fn group_payload(&self, payload: &Ident) -> Result<TokenStream> {
        let query = self.query;
        let (member, key) = self.group_keys()?;

        if type_ident(self.kernel.shape()).is_none() {
            return Err(self.group_road_error());
        }

        let road = CaptureRoad {
            policy: self.manifest.policy.as_ref(),
            selector: None,
            receiver: None,
            fields: &[],
        };
        let apply = self.group_payload_apply();
        let arguments = self.arguments_road(&apply, &road)?;

        let body = quote! {
            type #member = #query::dynamic::DynIndex;
            type #key = #query::dynamic::DynIndex;
            type #payload = #query::dynamic::DynPayload;

            let _ = std::marker::PhantomData::<fn() -> (#member, #key, #payload)>;

            #arguments
        };

        Ok(self.apply_function(&body))
    }

    fn group_payload_apply(&self) -> TokenStream {
        let query = self.query;

        quote! {
            { #query::dynamic::apply_group_operation(input, operation, output) }
        }
    }

    fn group_lane_apply(&self, shape: &TokenStream, arity: &TokenStream) -> TokenStream {
        let query = self.query;
        let operation = &self.manifest.operation;

        quote! {
            {
                type DynamicOperation = #query::dynamic::DynGroupOperation<#operation, #shape, #arity>;

                let operation = DynamicOperation::new(operation);

                #query::dynamic::apply_group_operation(input, operation, output)
            }
        }
    }

    fn group_lane_build(&self, shape: &TokenStream, arity: &TokenStream) -> Result<TokenStream> {
        let road = CaptureRoad {
            policy: self.manifest.policy.as_ref(),
            selector: None,
            receiver: None,
            fields: &[],
        };
        let apply = self.group_lane_apply(shape, arity);

        self.arguments_road(&apply, &road)
    }

    fn group_indexed(&self, index: &Ident, value: &Ident) -> Result<TokenStream> {
        let query = self.query;
        let (member, key) = self.group_keys()?;
        let (payload, arity) = self.group_input()?;
        self.indexed_payload(payload)?;
        let arity = quote!(#arity);

        let shape = quote!(#query::Indexed<#index, #value>);
        let general = self.group_lane_build(&shape, &arity)?;
        let mask = self.group_lane_build(&shape, &arity)?;
        let unit = self.group_lane_build(&shape, &arity)?;

        let body = quote! {
            type #member = #query::dynamic::DynIndex;
            type #key = #query::dynamic::DynIndex;
            type #index = #query::dynamic::DynIndex;

            let _ = std::marker::PhantomData::<fn() -> (#member, #key, #index)>;

            match #query::dynamic::innermost_lane_kind(input.descriptor()) {
                #query::dynamic::DynLaneKind::IndexedValue => {
                    type #value = #query::dynamic::DynValue;

                    #general
                }
                #query::dynamic::DynLaneKind::IndexedMask => {
                    type #value = #query::Mask;

                    #mask
                }
                #query::dynamic::DynLaneKind::IndexedUnit => {
                    type #value = #query::Unit;

                    #unit
                }
                _ => panic!("registry selected an indexed group kernel for a different lane shape"),
            }
        };

        Ok(self.apply_function(&body))
    }

    fn group_bare(&self, value: &Ident) -> Result<TokenStream> {
        let query = self.query;
        let (member, key) = self.group_keys()?;
        let (payload, arity) = self.group_input()?;
        self.bare_payload(payload)?;
        let arity = quote!(#arity);

        let shape = quote!(#query::Bare<#value>);
        let general = self.group_lane_build(&shape, &arity)?;
        let mask = self.group_lane_build(&shape, &arity)?;

        let body = quote! {
            type #member = #query::dynamic::DynIndex;
            type #key = #query::dynamic::DynIndex;

            let _ = std::marker::PhantomData::<fn() -> (#member, #key)>;

            match #query::dynamic::innermost_lane_kind(input.descriptor()) {
                #query::dynamic::DynLaneKind::BareValue => {
                    type #value = #query::dynamic::DynValue;

                    #general
                }
                #query::dynamic::DynLaneKind::BareMask => {
                    type #value = #query::Mask;

                    #mask
                }
                _ => panic!("registry selected a bare group kernel for a different lane shape"),
            }
        };

        Ok(self.apply_function(&body))
    }

    fn group_input(&self) -> Result<(&Type, &Type)> {
        let handle = type_application(self.kernel.shape(), "ExpressionHandle")
            .ok_or_else(|| self.group_road_error())?;
        let [payload, arity] = handle[..] else {
            return Err(self.group_road_error());
        };

        Ok((payload, arity))
    }

    fn indexed_payload(&self, payload: &Type) -> Result<()> {
        let indexed =
            type_application(payload, "Indexed").ok_or_else(|| self.group_road_error())?;
        let [index, value] = indexed[..] else {
            return Err(self.group_road_error());
        };
        if type_ident(index).is_none() || type_ident(value).is_none() {
            return Err(self.group_road_error());
        }

        Ok(())
    }

    fn bare_payload(&self, payload: &Type) -> Result<()> {
        let bare = type_application(payload, "Bare").ok_or_else(|| self.group_road_error())?;
        let [value] = bare[..] else {
            return Err(self.group_road_error());
        };
        if type_ident(value).is_none() {
            return Err(self.group_road_error());
        }

        Ok(())
    }

    fn multiple_input_arity(&self, arity: &Type) -> Result<()> {
        let multiple =
            type_application(arity, "Multiple").ok_or_else(|| self.group_road_error())?;
        let [order] = multiple[..] else {
            return Err(self.group_road_error());
        };
        if type_ident(order).is_none() {
            return Err(self.group_road_error());
        }

        Ok(())
    }

    fn group_indexed_multiple(
        &self,
        index: &Ident,
        value: &Ident,
        order: &Ident,
    ) -> Result<TokenStream> {
        let query = self.query;
        let (member, key) = self.group_keys()?;
        let (payload, arity) = self.group_input()?;
        self.indexed_payload(payload)?;
        self.multiple_input_arity(arity)?;

        let shape = quote!(#query::Indexed<#index, #value>);
        let kinds = [
            (quote!(IndexedValue), quote!(#query::dynamic::DynValue)),
            (quote!(IndexedMask), quote!(#query::Mask)),
            (quote!(IndexedUnit), quote!(#query::Unit)),
        ];
        let arms = self.group_multiple_arms(&shape, value, order, &kinds)?;

        let body = quote! {
            type #member = #query::dynamic::DynIndex;
            type #key = #query::dynamic::DynIndex;
            type #index = #query::dynamic::DynIndex;

            let _ = std::marker::PhantomData::<fn() -> (#member, #key, #index)>;

            match (
                #query::dynamic::innermost_lane_kind(input.descriptor()),
                input.descriptor().lane_arity(),
            ) {
                #arms
                _ => panic!("registry selected an indexed multiple group kernel for a different lane state"),
            }
        };

        Ok(self.apply_function(&body))
    }

    fn group_bare_multiple(&self, value: &Ident, order: &Ident) -> Result<TokenStream> {
        let query = self.query;
        let (member, key) = self.group_keys()?;
        let (payload, arity) = self.group_input()?;
        self.bare_payload(payload)?;
        self.multiple_input_arity(arity)?;

        let shape = quote!(#query::Bare<#value>);
        let kinds = [
            (quote!(BareValue), quote!(#query::dynamic::DynValue)),
            (quote!(BareMask), quote!(#query::Mask)),
        ];
        let arms = self.group_multiple_arms(&shape, value, order, &kinds)?;

        let body = quote! {
            type #member = #query::dynamic::DynIndex;
            type #key = #query::dynamic::DynIndex;

            let _ = std::marker::PhantomData::<fn() -> (#member, #key)>;

            match (
                #query::dynamic::innermost_lane_kind(input.descriptor()),
                input.descriptor().lane_arity(),
            ) {
                #arms
                _ => panic!("registry selected a bare multiple group kernel for a different lane state"),
            }
        };

        Ok(self.apply_function(&body))
    }

    fn group_multiple_arms(
        &self,
        shape: &TokenStream,
        value: &Ident,
        order: &Ident,
        kinds: &[(TokenStream, TokenStream)],
    ) -> Result<TokenStream> {
        let query = self.query;
        let orders = [
            (quote!(Ordered), quote!(#query::Ordered)),
            (quote!(Unordered), quote!(#query::Unordered)),
        ];

        let mut arms = TokenStream::new();
        for (kind, value_domain) in kinds {
            for (order_descriptor, order_state) in &orders {
                let build = self.group_lane_build(shape, &quote!(#query::Multiple<#order>))?;
                arms.extend(quote! {
                    (
                        #query::dynamic::DynLaneKind::#kind,
                        #query::registry::ArityDescriptor::Multiple {
                            order: #query::registry::OrderDescriptor::#order_descriptor,
                        },
                    ) => {
                        type #value = #value_domain;
                        type #order = #order_state;

                        #build
                    },
                });
            }
        }

        Ok(arms)
    }

    fn group_shapes(&self, shape: &Ident, arity: &Ident) -> Result<TokenStream> {
        let query = self.query;

        let handle = type_application(self.kernel.shape(), "ExpressionHandle")
            .ok_or_else(|| self.group_road_error())?;
        let [input_shape, input_arity] = handle[..] else {
            return Err(self.group_road_error());
        };
        if type_ident(input_shape).is_none() || type_ident(input_arity).is_none() {
            return Err(self.group_road_error());
        }

        let kinds = [
            (
                quote!(IndexedValue),
                quote!(#query::Indexed<#query::dynamic::DynIndex, #query::dynamic::DynValue>),
            ),
            (
                quote!(IndexedMask),
                quote!(#query::Indexed<#query::dynamic::DynIndex, #query::Mask>),
            ),
            (
                quote!(IndexedUnit),
                quote!(#query::Indexed<#query::dynamic::DynIndex, #query::Unit>),
            ),
            (
                quote!(BareValue),
                quote!(#query::Bare<#query::dynamic::DynValue>),
            ),
            (quote!(BareMask), quote!(#query::Bare<#query::Mask>)),
        ];

        let mut arms = TokenStream::new();
        for (kind, dynamic_shape) in kinds {
            let all_arities = self.group_all_arities(shape, arity, &dynamic_shape)?;
            arms.extend(quote! {
                #query::dynamic::DynLaneKind::#kind => {
                    #all_arities
                }
            });
        }

        let body = quote! {
            match #query::dynamic::innermost_lane_kind(input.descriptor()) {
                #arms
            }
        };

        Ok(self.apply_function(&body))
    }

    fn group_all_arities(
        &self,
        shape: &Ident,
        arity: &Ident,
        dynamic_shape: &TokenStream,
    ) -> Result<TokenStream> {
        let query = self.query;
        let (member, key) = self.group_keys()?;
        let arities = [
            (
                quote! {
                    #query::registry::ArityDescriptor::Multiple {
                        order: #query::registry::OrderDescriptor::Ordered,
                    }
                },
                quote!(#query::Multiple<#query::Ordered>),
            ),
            (
                quote! {
                    #query::registry::ArityDescriptor::Multiple {
                        order: #query::registry::OrderDescriptor::Unordered,
                    }
                },
                quote!(#query::Multiple<#query::Unordered>),
            ),
            (
                quote!(#query::registry::ArityDescriptor::Single),
                quote!(#query::Single),
            ),
            (
                quote!(#query::registry::ArityDescriptor::Definite),
                quote!(#query::Definite),
            ),
        ];

        let mut arms = TokenStream::new();
        for (pattern, arity_state) in arities {
            let build = self.group_lane_build(&quote!(#shape), &quote!(#arity))?;
            arms.extend(quote! {
                #pattern => {
                    type #arity = #arity_state;

                    #build
                }
            });
        }

        Ok(quote! {
            {
                type #member = #query::dynamic::DynIndex;
                type #key = #query::dynamic::DynIndex;
                type #shape = #dynamic_shape;

                let _ = std::marker::PhantomData::<fn() -> (#member, #key)>;

                match input.descriptor().lane_arity() {
                    #arms
                }
            }
        })
    }

    fn entity_dispatch<F>(&self, build: F) -> Result<TokenStream>
    where
        F: Fn(&TokenStream) -> Result<TokenStream>,
    {
        let query = self.query;
        let mut dispatched = false;
        let mut group_capable = true;

        for parameter in &self.kernel.parameters {
            match parameter.bound.to_string().as_str() {
                "EntityIndexDomain" => dispatched = true,
                "EntityAttributes" | "GroupMembership" => {
                    dispatched = true;
                    group_capable = false;
                }
                _ => {}
            }
        }

        if !dispatched {
            return build(&quote!(#query::dynamic::DynIndex));
        }

        let node = build(&quote!(#query::dynamic::NodeIndex))?;
        let edge = build(&quote!(#query::dynamic::EdgeIndex))?;
        let group = if group_capable {
            build(&quote!(#query::dynamic::GroupIndex))?
        } else {
            quote! {
                panic!(
                    "registry selected an entity operation the group index domain does not support"
                )
            }
        };

        Ok(quote! {
            match #query::dynamic::entity_domain(input) {
                #query::dynamic::DynEntityDomain::Node => {
                    #node
                }
                #query::dynamic::DynEntityDomain::Edge => {
                    #edge
                }
                #query::dynamic::DynEntityDomain::Group => {
                    #group
                }
            }
        })
    }

    fn receiver_dispatch<F>(&self, build: F) -> Result<TokenStream>
    where
        F: Fn(&TokenStream) -> Result<TokenStream>,
    {
        let query = self.query;
        let with_unit = self
            .kernel
            .parameters
            .iter()
            .find_map(Self::receiver_value_domains);

        let Some(with_unit) = with_unit else {
            return build(&quote!(#query::dynamic::DynValue));
        };

        let mask = build(&quote!(#query::Mask))?;
        let general = build(&quote!(#query::dynamic::DynValue))?;

        if with_unit {
            let unit = build(&quote!(#query::Unit))?;

            return Ok(quote! {
                match #query::dynamic::innermost_lane_kind(input.descriptor()) {
                    #query::dynamic::DynLaneKind::IndexedMask
                    | #query::dynamic::DynLaneKind::BareMask => {
                        #mask
                    }
                    #query::dynamic::DynLaneKind::IndexedUnit => {
                        #unit
                    }
                    _ => #general,
                }
            });
        }

        Ok(quote! {
            match #query::dynamic::innermost_lane_kind(input.descriptor()) {
                #query::dynamic::DynLaneKind::IndexedMask
                | #query::dynamic::DynLaneKind::BareMask => {
                    #mask
                }
                _ => #general,
            }
        })
    }

    fn receiver_value_domains(parameter: &Parameter) -> Option<bool> {
        let target = parameter
            .target
            .as_ref()
            .map(|target| quote!(#target).to_string().replace(' ', ""));

        match (parameter.bound.to_string().as_str(), target.as_deref()) {
            ("ValueDomain", _) => Some(true),
            (
                "BareValueDomain" | "ValueEquality" | "ValueEquivalence" | "ValueGrouping"
                | "ValueMode",
                _,
            )
            | ("ValueTransition", Some("Scalar" | "(IndexValue<Value>)" | "(IndexValue<bool>)")) => {
                Some(false)
            }
            _ => None,
        }
    }

    fn aliases(
        &self,
        order: &TokenStream,
        entity: &TokenStream,
        shape: &TokenStream,
        arity: &TokenStream,
    ) -> TokenStream {
        let query = self.query;
        let mut statements = TokenStream::new();

        for parameter in &self.kernel.parameters {
            let name = &parameter.name;
            let bare = parameter.target.is_none() && parameter.additional.is_empty();
            let alias = match parameter.bound.to_string().as_str() {
                "IndexDomain" | "EnsureSortable" if bare => quote!(#query::dynamic::DynIndex),
                "EntityIndexDomain" | "EntityAttributes" | "GroupMembership" if bare => {
                    entity.clone()
                }
                "ElementShape" if bare => shape.clone(),
                "OrderState" if bare => order.clone(),
                "Arity" if bare => arity.clone(),
                "Lane" if bare => quote!(#query::dynamic::DynPayload),
                _ => quote!(<#shape as #query::ElementShape>::ValueDomain),
            };
            statements.extend(quote!(type #name = #alias;));
        }

        let names = self
            .kernel
            .parameters
            .iter()
            .map(|parameter| &parameter.name);
        statements.extend(quote! {
            let _ = std::marker::PhantomData::<fn() -> (#(#names,)*)>;
        });

        statements
    }

    fn arguments_road(&self, apply: &TokenStream, road: &CaptureRoad<'_>) -> Result<TokenStream> {
        let query = self.query;
        let arguments = self.kernel.value_arguments()?;

        match arguments[..] {
            [] => {
                let capture = self.capture(road, &[])?;

                Ok(quote! {
                    {
                        let operation = #capture;
                        #apply
                    }
                })
            }
            [argument] if argument.value.is_some() && argument.retention.is_some() => {
                let name = &argument.name;
                let alignment = &argument.alignment;
                let value = &argument.value;
                let retention = &argument.retention;
                let argument_position = road.fields.len();
                let argument_type =
                    self.argument_type(alignment, value.as_ref(), &quote!(#retention));
                let argument_value = self.argument_value(
                    &quote!(source),
                    alignment,
                    value.as_ref(),
                    &quote!(#retention),
                );
                let capture = self.capture(road, &[quote!(first_argument)])?;

                Ok(quote! {
                    {
                        type #name = #argument_type;

                        let source = #query::dynamic::invoke_argument_source(arguments, #argument_position);
                        let first_argument = #argument_value;
                        let operation = #capture;
                        #apply
                    }
                })
            }
            [argument]
                if self.manifest.method == "sort_by"
                    && argument.value.is_none()
                    && argument.retention.is_none() =>
            {
                self.selected_argument(apply, road, argument, None, false)
            }
            [argument] => {
                let name = &argument.name;
                let alignment = &argument.alignment;
                let value = argument.value.as_ref();
                let argument_position = road.fields.len();
                let dropping = self.argument_retention(
                    apply,
                    road,
                    name,
                    alignment,
                    value,
                    &quote!(#query::dynamic::Dropping),
                )?;
                let preserving = self.argument_retention(
                    apply,
                    road,
                    name,
                    alignment,
                    value,
                    &quote!(#query::dynamic::Preserving),
                )?;

                Ok(quote! {
                    {
                        let source = #query::dynamic::invoke_argument_source(arguments, #argument_position);
                        if source.is_dropping() {
                            #dropping
                        } else {
                            #preserving
                        }
                    }
                })
            }
            [first, second] if first.retention.is_none() && second.retention.is_none() => {
                let pair = |first_retention: &TokenStream,
                            second_retention: &TokenStream|
                 -> Result<TokenStream> {
                    let first_name = &first.name;
                    let second_name = &second.name;
                    let first_type =
                        self.argument_type(&first.alignment, first.value.as_ref(), first_retention);
                    let second_type = self.argument_type(
                        &second.alignment,
                        second.value.as_ref(),
                        second_retention,
                    );
                    let first_value = self.argument_value(
                        &quote!(first_source),
                        &first.alignment,
                        first.value.as_ref(),
                        first_retention,
                    );
                    let second_value = self.argument_value(
                        &quote!(second_source),
                        &second.alignment,
                        second.value.as_ref(),
                        second_retention,
                    );
                    let pair_road = CaptureRoad {
                        policy: None,
                        selector: road.selector,
                        receiver: road.receiver,
                        fields: road.fields,
                    };
                    let capture = self.capture(
                        &pair_road,
                        &[quote!(first_argument), quote!(second_argument)],
                    )?;

                    Ok(quote! {
                        {
                            type #first_name = #first_type;
                            type #second_name = #second_type;
                            let first_argument = #first_value;
                            let second_argument = #second_value;
                            let operation = #capture;
                            #apply
                        }
                    })
                };

                let preserving = quote!(#query::dynamic::Preserving);
                let dropping = quote!(#query::dynamic::Dropping);
                let preserved_preserved = pair(&preserving, &preserving)?;
                let preserved_dropped = pair(&preserving, &dropping)?;
                let dropped_preserved = pair(&dropping, &preserving)?;
                let dropped_dropped = pair(&dropping, &dropping)?;

                let first_position = road.fields.len();
                let second_position = first_position + 1;

                Ok(quote! {
                    {
                        let first_source = #query::dynamic::invoke_argument_source(arguments, #first_position);
                        let second_source = #query::dynamic::invoke_argument_source(arguments, #second_position);
                        match (first_source.is_dropping(), second_source.is_dropping()) {
                            (false, false) => #preserved_preserved,
                            (false, true) => #preserved_dropped,
                            (true, false) => #dropped_preserved,
                            (true, true) => #dropped_dropped,
                        }
                    }
                })
            }
            _ => Err(Error::new(
                self.manifest.method.span(),
                "the arguments have no dynamic road",
            )),
        }
    }

    fn argument_retention(
        &self,
        apply: &TokenStream,
        road: &CaptureRoad<'_>,
        name: &Ident,
        alignment: &Type,
        value: Option<&Type>,
        retention: &TokenStream,
    ) -> Result<TokenStream> {
        let argument_type = self.argument_type(alignment, value, retention);
        let argument_value = self.argument_value(&quote!(source), alignment, value, retention);
        let capture = self.capture(road, &[quote!(first_argument)])?;

        Ok(quote! {
            type #name = #argument_type;
            let first_argument = #argument_value;
            let operation = #capture;
            #apply
        })
    }

    fn argument_type(
        &self,
        alignment: &Type,
        value: Option<&Type>,
        retention: &TokenStream,
    ) -> TokenStream {
        let query = self.query;

        if let Some(value) = value {
            return quote! {
                #query::operations::Argument<
                    #alignment,
                    <#value as #query::dynamic::DynArgumentBuilder<#alignment, #retention>>::Dynamic,
                    #retention,
                >
            };
        }

        quote! {
            #query::operations::Argument<
                #alignment,
                #query::dynamic::DynValue,
                #retention,
            >
        }
    }

    fn argument_value(
        &self,
        source: &TokenStream,
        alignment: &Type,
        value: Option<&Type>,
        retention: &TokenStream,
    ) -> TokenStream {
        let query = self.query;

        if let Some(value) = value {
            return quote! {
                <#value as #query::dynamic::DynArgumentBuilder<#alignment, #retention>>::build(#source)
            };
        }

        quote! {
            {
                type DynamicValue = #query::dynamic::DynValue;

                <DynamicValue as #query::dynamic::DynArgumentBuilder<#alignment, #retention>>::build(
                    #source,
                )
            }
        }
    }

    fn selected_argument(
        &self,
        apply: &TokenStream,
        road: &CaptureRoad<'_>,
        argument: &ValueArgument,
        value_alias: Option<&Ident>,
        keyable: bool,
    ) -> Result<TokenStream> {
        let query = self.query;
        let mask_alias = value_alias.map(|alias| quote!(type #alias = #query::Mask;));
        let general_alias =
            value_alias.map(|alias| quote!(type #alias = #query::dynamic::DynValue;));
        let masked = self.selected_road(apply, road, argument, &quote!(#query::Mask), false)?;
        let unmasked = self.selected_road(
            apply,
            road,
            argument,
            &quote!(#query::dynamic::DynValue),
            keyable,
        )?;

        Ok(quote! {
            {
                let source = #query::dynamic::invoke_argument_source(arguments, 0);

                if source.is_mask() {
                    #mask_alias

                    #masked
                } else {
                    #general_alias

                    #unmasked
                }
            }
        })
    }

    fn selected_road(
        &self,
        apply: &TokenStream,
        road: &CaptureRoad<'_>,
        argument: &ValueArgument,
        value: &TokenStream,
        keyable: bool,
    ) -> Result<TokenStream> {
        let query = self.query;
        let dropping = self.selected_retention(
            apply,
            road,
            argument,
            value,
            &quote!(#query::dynamic::Dropping),
            keyable,
        )?;
        let preserving = self.selected_retention(
            apply,
            road,
            argument,
            value,
            &quote!(#query::dynamic::Preserving),
            keyable,
        )?;

        Ok(quote! {
            {
                let source = #query::dynamic::invoke_argument_source(arguments, 0);

                if source.is_dropping() {
                    #dropping
                } else {
                    #preserving
                }
            }
        })
    }

    fn selected_retention(
        &self,
        apply: &TokenStream,
        road: &CaptureRoad<'_>,
        argument: &ValueArgument,
        value: &TokenStream,
        retention: &TokenStream,
        keyable: bool,
    ) -> Result<TokenStream> {
        let query = self.query;
        let name = &argument.name;
        let alignment = &argument.alignment;
        let argument_type = quote! {
            #query::operations::Argument<
                #alignment,
                <#value as #query::dynamic::DynArgumentBuilder<#alignment, #retention>>::Dynamic,
                #retention,
            >
        };
        let argument_value = quote! {
            <#value as #query::dynamic::DynArgumentBuilder<#alignment, #retention>>::build(source)
        };
        let capture = self.capture(road, &[quote!(first_argument)])?;

        if keyable {
            return Ok(quote! {
                type DynamicArgument = #argument_type;
                type #name = #query::dynamic::Keyable<DynamicArgument>;

                let argument = #argument_value;
                let first_argument = #query::dynamic::Keyable::new(argument);
                let operation = #capture;

                #apply
            });
        }

        Ok(quote! {
            type #name = #argument_type;

            let first_argument = #argument_value;
            let operation = #capture;

            #apply
        })
    }

    fn capture(&self, road: &CaptureRoad<'_>, values: &[TokenStream]) -> Result<TokenStream> {
        let query = self.query;
        let operation = &self.manifest.operation;
        let method = &self.manifest.method;

        if let Some(policy) = road.policy {
            let path = &policy.path;

            return match (&policy.constructor, values) {
                (None, []) => Ok(quote! {
                    {
                        let _ = arguments;
                        let capture = #query::dynamic::OperationCapture::<#operation>::capture();
                        let captured = capture.#method(#path);
                        captured.operation()
                    }
                }),
                (Some(constructor), [first]) => {
                    let owner = &constructor.owner;
                    let construction = match &constructor.call {
                        PolicyCall::Path(function) => quote!(#owner::#function(#first)),
                        PolicyCall::Dot(function) => quote!(#owner.#function(#first)),
                        PolicyCall::Tuple => quote!(#owner(#first)),
                    };

                    Ok(quote! {
                        {
                            let capture = #query::dynamic::OperationCapture::<#operation>::capture();
                            let captured = capture.#method(#construction);
                            captured.operation()
                        }
                    })
                }
                _ => Err(Error::new(
                    method.span(),
                    "the policy has no dynamic capture",
                )),
            };
        }

        if let Some(selector) = road.selector {
            if !values.is_empty() {
                return Err(Error::new(
                    method.span(),
                    "the selector has no dynamic capture",
                ));
            }

            if method == "transition" {
                return Ok(quote! {
                    {
                        let _ = arguments;
                        let capture = #query::dynamic::OperationCapture::<#operation>::capture();
                        let captured = capture.transition::<#selector>();
                        captured.operation()
                    }
                });
            }

            return Ok(quote! {
                {
                    let _ = arguments;
                    let capture = #query::dynamic::OperationCapture::<#operation>::capture();
                    let captured = capture.#method(#selector);
                    captured.operation()
                }
            });
        }

        if let Some(receiver) = road.receiver {
            let [first] = values else {
                return Err(Error::new(
                    receiver.span(),
                    "the receiver has no dynamic capture",
                ));
            };

            return Ok(quote! {
                {
                    let capture = #query::dynamic::OperationCapture::<#operation>::capture();
                    let captured = #first.#method(&capture);
                    captured.operation()
                }
            });
        }

        if !road.fields.is_empty() {
            return match (road.fields, values) {
                ([field], []) => {
                    let invoke = self.field_invoke(field, &quote!(0))?;

                    Ok(quote! {
                        {
                            let capture = #query::dynamic::OperationCapture::<#operation>::capture();
                            let field = #invoke;
                            let captured = capture.#method(field);
                            captured.operation()
                        }
                    })
                }
                ([first, second], []) => {
                    let first_invoke = self.field_invoke(first, &quote!(0))?;
                    let second_invoke = self.field_invoke(second, &quote!(1))?;

                    Ok(quote! {
                        {
                            let capture = #query::dynamic::OperationCapture::<#operation>::capture();
                            let first_field = #first_invoke;
                            let second_field = #second_invoke;
                            let captured = capture.#method(first_field, second_field);
                            captured.operation()
                        }
                    })
                }
                ([field], [value]) => {
                    let invoke = self.field_invoke(field, &quote!(0))?;

                    Ok(quote! {
                        {
                            let capture = #query::dynamic::OperationCapture::<#operation>::capture();
                            let field = #invoke;
                            let captured = capture.#method(field, #value);
                            captured.operation()
                        }
                    })
                }
                _ => Err(Error::new(
                    method.span(),
                    "the fields have no dynamic capture",
                )),
            };
        }

        match values {
            [] => Ok(quote! {
                {
                    let _ = arguments;
                    let capture = #query::dynamic::OperationCapture::<#operation>::capture();
                    let captured = capture.#method();
                    captured.operation()
                }
            }),
            [first] => Ok(quote! {
                {
                    let capture = #query::dynamic::OperationCapture::<#operation>::capture();
                    let captured = capture.#method(#first);
                    captured.operation()
                }
            }),
            [first, second] => Ok(quote! {
                {
                    let capture = #query::dynamic::OperationCapture::<#operation>::capture();
                    let captured = capture.#method(#first, #second);
                    captured.operation()
                }
            }),
            _ => Err(Error::new(
                method.span(),
                "the values have no dynamic capture",
            )),
        }
    }

    fn field_invoke(&self, field: &Field, position: &TokenStream) -> Result<TokenStream> {
        let query = self.query;

        let function = match field.field_type.to_string().as_str() {
            "AttributeName" => quote!(invoke_attribute),
            "GroupIndex" => quote!(invoke_group),
            "EdgeDirection" => quote!(invoke_direction),
            "usize" => quote!(invoke_position),
            _ => {
                return Err(Error::new(
                    field.field_type.span(),
                    "the field type has no dynamic invocation",
                ));
            }
        };

        Ok(quote!(#query::dynamic::#function(arguments, #position)))
    }

    fn apply_function(&self, body: &TokenStream) -> TokenStream {
        let query = self.query;

        quote! {
            {
                fn apply(
                    input: &#query::dynamic::DynExpression,
                    arguments: &[#query::dynamic::DynInvokeArgument],
                    output: #query::registry::ExpressionDescriptor,
                ) -> #query::dynamic::DynExpression {
                    #body
                }

                apply
            }
        }
    }
}
