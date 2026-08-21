mod applier;
mod entry;
mod model;
mod witness;

use crate::query::resolve_query_crate_path;
pub use model::Manifest;
use proc_macro2::TokenStream;
use quote::quote;
use syn::Result;

pub fn expand(manifest: &Manifest) -> Result<TokenStream> {
    let query = resolve_query_crate_path()?;

    let witness = witness::witness(manifest, &query)?;
    let function = entry::manifest_function(manifest, &query)?;

    Ok(quote! {
        #witness
        #function
    })
}
