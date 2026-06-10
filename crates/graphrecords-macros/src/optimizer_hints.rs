use crate::plan::{Hints, PlanModel};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Result};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let PlanModel {
        ident,
        generics,
        crate_path,
        hints,
        ..
    } = PlanModel::parse(input)?;

    let Hints {
        commutes_with_filter,
        allows_limit_pushdown,
        distinct,
        volatile,
        empty,
    } = hints;

    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics #crate_path::optimizer::OptimizerHints for #ident #type_generics #where_clause {
            fn commutes_with_filter(&self) -> bool {
                #commutes_with_filter
            }

            fn allows_limit_pushdown(&self) -> bool {
                #allows_limit_pushdown
            }

            fn is_distinct(&self) -> bool {
                #distinct
            }

            fn is_volatile(&self) -> bool {
                #volatile
            }

            fn empty_rule(&self) -> #crate_path::optimizer::EmptyRule {
                #crate_path::optimizer::EmptyRule::#empty
            }
        }
    })
}
