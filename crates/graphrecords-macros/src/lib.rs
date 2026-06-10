mod attribute;
mod explain;
mod has_inputs;
mod operand;
mod optimize_inputs;
mod optimizer_hints;
mod phase_label;
mod plan;
mod plan_node;

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use syn::{DeriveInput, Error, Path, Result, parse_macro_input, parse_str};

#[proc_macro_derive(PlanNode, attributes(plan, input))]
pub fn derive_plan_node(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    plan_node::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(HasInputs, attributes(plan, input))]
pub fn derive_has_inputs(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    has_inputs::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(OptimizeInputs, attributes(plan, input))]
pub fn derive_optimize_inputs(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    optimize_inputs::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(Explain, attributes(explain, input))]
pub fn derive_explain(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    explain::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(OptimizerHints, attributes(plan, input))]
pub fn derive_optimizer_hints(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    optimizer_hints::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(Operand, attributes(operand))]
pub fn derive_operand(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    operand::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(PhaseLabel, attributes(phase_label))]
pub fn derive_phase_label(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    phase_label::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

pub(crate) fn resolve_query_crate_path() -> Result<Path> {
    resolve_crate_path("graphrecords-query", "query")
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
