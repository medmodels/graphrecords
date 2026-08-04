mod attribute;
mod query;

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use syn::{DeriveInput, Error, Path, Result, parse_macro_input, parse_str};

#[proc_macro_derive(PlanNode, attributes(plan, input, argument))]
pub fn derive_plan_node(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    query::optimizer::plan::node::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(MatchInputs, attributes(plan, input, argument))]
pub fn derive_match_inputs(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    query::optimizer::plan::match_inputs::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(PlanIdentity, attributes(plan, input, argument))]
pub fn derive_plan_identity(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    query::optimizer::plan::plan_identity::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(PlanInputs, attributes(plan, input, argument))]
pub fn derive_plan_inputs(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    query::optimizer::plan::plan_inputs::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(OperationInputs, attributes(plan, input, argument))]
pub fn derive_operation_inputs(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    query::optimizer::plan::operation_inputs::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(OptimizePlan, attributes(plan, input, argument))]
pub fn derive_optimize_plan(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    query::optimizer::plan::optimize_inputs::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(Explain, attributes(explain, input, argument))]
pub fn derive_explain(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    query::explain::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(OptimizerHints, attributes(plan, input, argument))]
pub fn derive_optimizer_hints(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    query::optimizer::plan::optimizer_hints::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(Operation, attributes(operation, plan, input, argument))]
pub fn derive_operation(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    query::operation::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(PhaseLabel, attributes(phase_label))]
pub fn derive_phase_label(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    query::optimizer::phase_label::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

pub(crate) fn resolve_crate_path(package: &str, facade_module: &str) -> Result<Path> {
    const FACADE: &str = "graphrecords";

    match crate_name(package) {
        Ok(FoundCrate::Itself) => parse_str("crate"),
        Ok(FoundCrate::Name(name)) => parse_str(&format!("::{name}")),
        Err(_) => match crate_name(FACADE) {
            Ok(FoundCrate::Itself) => parse_str(&format!("crate::{facade_module}")),
            Ok(FoundCrate::Name(name)) => parse_str(&format!("::{name}::{facade_module}")),
            Err(error) => Err(Error::new(
                Span::call_site(),
                format!(
                    "`{FACADE}` or `{package}` must be a dependency to derive this macro: {error}"
                ),
            )),
        },
    }
}
