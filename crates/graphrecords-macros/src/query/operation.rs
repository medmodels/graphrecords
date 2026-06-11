use super::optimizer::plan::PlanModel;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Result, parse_quote};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let PlanModel {
        ident,
        mut generics,
        crate_path,
        ..
    } = PlanModel::parse(input)?;

    let bound = quote! {
        #crate_path::operations::Prepare
            + #crate_path::optimizer::OperationInputs
            + #crate_path::Explain
    };

    generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(Self: #bound));

    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #crate_path::operations::Operation
            for #ident #type_generics #where_clause {}
    })
}
