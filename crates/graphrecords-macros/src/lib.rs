mod operand;
mod phase_label;
mod plan_node;

use proc_macro::TokenStream;
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
        None => parse_str("::graphrecords::query"),
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
