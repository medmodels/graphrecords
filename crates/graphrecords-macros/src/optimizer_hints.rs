use crate::crate_path;
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::{DeriveInput, Result};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let name = &input.ident;

    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();

    let mut commutes_with_filter = false;
    let mut allows_limit_pushdown = false;
    let mut is_distinct = false;
    let mut is_volatile = false;
    let mut empty_rule: Option<TokenStream> = None;

    let crate_path = crate_path(&input.attrs, "optimizer_hints", |meta| {
        if meta.path.is_ident("commutes_with_filter") {
            commutes_with_filter = true;
            Ok(true)
        } else if meta.path.is_ident("allows_limit_pushdown") {
            allows_limit_pushdown = true;
            Ok(true)
        } else if meta.path.is_ident("distinct") {
            is_distinct = true;
            Ok(true)
        } else if meta.path.is_ident("volatile") {
            is_volatile = true;
            Ok(true)
        } else if meta.path.is_ident("empty") {
            let rule: Ident = meta.value()?.parse()?;

            empty_rule = Some(match rule.to_string().as_str() {
                "never" => quote!(Never),
                "if_any" => quote!(IfAnyInput),
                "if_all" => quote!(IfAllInputs),
                other => {
                    return Err(meta.error(format!(
                        "unknown empty rule `{other}`, expected `never`, `if_any`, or `if_all`"
                    )));
                }
            });

            Ok(true)
        } else {
            Ok(false)
        }
    })?;

    let empty_rule = empty_rule.unwrap_or_else(|| quote!(Never));

    Ok(quote! {
        impl #impl_generics #crate_path::optimizer::OptimizerHints for #name #type_generics #where_clause {
            fn commutes_with_filter(&self) -> bool {
                #commutes_with_filter
            }

            fn allows_limit_pushdown(&self) -> bool {
                #allows_limit_pushdown
            }

            fn is_distinct(&self) -> bool {
                #is_distinct
            }

            fn is_volatile(&self) -> bool {
                #is_volatile
            }

            fn empty_rule(&self) -> #crate_path::optimizer::EmptyRule {
                #crate_path::optimizer::EmptyRule::#empty_rule
            }
        }
    })
}
