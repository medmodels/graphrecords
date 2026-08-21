use super::{
    optimizer::plan::{PlanModel, with_bounds},
    resolve_core_crate_path,
};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Error, Result};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let PlanModel {
        ident,
        generics,
        crate_path,
        arguments,
        argument_types,
        ..
    } = PlanModel::parse(input)?;
    let core_path = resolve_core_crate_path()?;

    let bound = quote!(#crate_path::operations::Prepare);
    let generics = with_bounds(&generics, &argument_types, &bound);
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    let (prepared, graphrecord, cache, body) = match (&arguments[..], &argument_types[..]) {
        ([], []) => (
            quote!(()),
            quote!(_graphrecord),
            quote!(_cache),
            quote!(::core::result::Result::Ok(())),
        ),
        ([argument], [argument_type]) => (
            quote!(<#argument_type as #bound>::Prepared<'a>),
            quote!(graphrecord),
            quote!(cache),
            quote!(#bound::prepare(&self.#argument, graphrecord, cache)),
        ),
        _ => {
            return Err(Error::new_spanned(
                input,
                "`Prepare` can only be derived for at most one `#[argument]` field",
            ));
        }
    };

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #bound for #ident #type_generics #where_clause {
            type Prepared<'a> = #prepared where Self: 'a;

            fn prepare<'a>(
                &'a self,
                #graphrecord: &'a #core_path::GraphRecord,
                #cache: &'a #crate_path::execution::EvaluationCache,
            ) -> #crate_path::QueryResult<Self::Prepared<'a>> {
                #body
            }
        }
    })
}
