use crate::crate_path;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{Data, DeriveInput, Error, Index, LitStr, Result};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let name = &input.ident;

    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();

    let mut label: Option<LitStr> = None;

    let crate_path = crate_path(&input.attrs, "explain", |meta| {
        if meta.path.is_ident("label") {
            label = Some(meta.value()?.parse::<LitStr>()?);
            Ok(true)
        } else {
            Ok(false)
        }
    })?;

    let name_label = label.unwrap_or_else(|| LitStr::new(&name.to_string(), name.span()));

    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            input,
            "Explain can only be derived for structs",
        ));
    };

    let mut describe_names = Vec::new();
    let mut describe_accessors = Vec::new();

    for (index, field) in data.fields.iter().enumerate() {
        let is_described = field
            .attrs
            .iter()
            .any(|attribute| attribute.path().is_ident("explain"));

        if !is_described {
            continue;
        }

        let accessor = if let Some(identifier) = &field.ident {
            quote!(#identifier)
        } else {
            let index = Index::from(index);
            quote!(#index)
        };

        let field_name = match &field.ident {
            Some(identifier) => identifier.to_string(),
            None => index.to_string(),
        };

        describe_names.push(LitStr::new(&field_name, Span::call_site()));
        describe_accessors.push(accessor);
    }

    Ok(quote! {
        impl #impl_generics #crate_path::optimizer::Explain for #name #type_generics #where_clause {
            fn describe(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                ::core::fmt::Formatter::write_str(formatter, #name_label)?;
                #( ::core::write!(formatter, " {}={}", #describe_names, &self.#describe_accessors)?; )*
                ::core::result::Result::Ok(())
            }
        }
    })
}
