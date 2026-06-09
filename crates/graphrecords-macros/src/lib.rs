mod explain;
mod operand;
mod optimizer_hints;
mod phase_label;
mod plan_node;

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use syn::{
    Attribute, DeriveInput, Error, Field, LitStr, Path, Result, meta::ParseNestedMeta,
    parse_macro_input, parse_str,
};

#[proc_macro_derive(PlanNode, attributes(plan_node))]
pub fn derive_plan_node(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    plan_node::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(Explain, attributes(explain))]
pub fn derive_explain(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    explain::expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(OptimizerHints, attributes(optimizer_hints))]
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

pub(crate) fn crate_path(
    attributes: &[Attribute],
    attribute_name: &str,
    mut handle: impl FnMut(&ParseNestedMeta) -> Result<bool>,
) -> Result<Path> {
    let mut crate_path = None;

    for attribute in attributes {
        if !attribute.path().is_ident(attribute_name) {
            continue;
        }

        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("crate") {
                crate_path = Some(meta.value()?.parse::<LitStr>()?.parse()?);
                Ok(())
            } else if handle(&meta)? {
                Ok(())
            } else {
                Err(meta.error(format!("unknown {attribute_name} attribute")))
            }
        })?;
    }

    match crate_path {
        Some(path) => Ok(path),
        None => resolve_crate_path(),
    }
}

fn resolve_crate_path() -> Result<Path> {
    match crate_name("graphrecords-query") {
        Ok(FoundCrate::Itself) => parse_str("crate"),
        Ok(FoundCrate::Name(name)) => parse_str(&format!("::{name}")),
        Err(_) => match crate_name("graphrecords") {
            Ok(FoundCrate::Itself) => parse_str("crate::query"),
            Ok(FoundCrate::Name(name)) => parse_str(&format!("::{name}::query")),
            Err(error) => Err(Error::new(
                Span::call_site(),
                format!("`graphrecords` must be a dependency to derive this macro: {error}"),
            )),
        },
    }
}

pub(crate) fn has_flag(field: &Field, attribute_name: &str, flag: &str) -> Result<bool> {
    let mut present = false;

    for attribute in &field.attrs {
        if !attribute.path().is_ident(attribute_name) {
            continue;
        }

        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident(flag) {
                present = true;
                Ok(())
            } else {
                Err(meta.error(format!("unknown {attribute_name} attribute")))
            }
        })?;
    }

    Ok(present)
}
