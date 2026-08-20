pub mod match_inputs;
pub mod node;
pub mod operation_inputs;
pub mod optimize_plan;
pub mod optimizer_hints;
pub mod plan_identity;
pub mod plan_inputs;

use crate::{attribute::FromAttributes, query::resolve_query_crate_path};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Error, Generics, Ident, Index, LitStr, Path, Result, Type,
    meta::ParseNestedMeta, parse_quote,
};

pub struct Hints {
    pub commutes_with_filter: TokenStream,
    pub allows_limit_pushdown: TokenStream,
    pub volatile: TokenStream,
    pub empty: TokenStream,
}

impl Default for Hints {
    fn default() -> Self {
        Self {
            commutes_with_filter: quote!(false),
            allows_limit_pushdown: quote!(false),
            volatile: quote!(false),
            empty: quote!(Never),
        }
    }
}

impl Hints {
    fn parse(meta: &ParseNestedMeta) -> Result<Self> {
        let mut hints = Self::default();

        meta.parse_nested_meta(|hint| {
            if hint.path.is_ident("commutes_with_filter") {
                hints.commutes_with_filter = quote!(true);
                Ok(())
            } else if hint.path.is_ident("allows_limit_pushdown") {
                hints.allows_limit_pushdown = quote!(true);
                Ok(())
            } else if hint.path.is_ident("volatile") {
                hints.volatile = quote!(true);
                Ok(())
            } else if hint.path.is_ident("empty") {
                let rule: Ident = hint.value()?.parse()?;
                hints.empty = match rule.to_string().as_str() {
                    "never" => quote!(Never),
                    "if_any" => quote!(IfAnyInput),
                    "if_all" => quote!(IfAllInputs),
                    other => {
                        return Err(hint.error(format!(
                            "unknown empty rule `{other}`, expected `never`, `if_any`, or `if_all`"
                        )));
                    }
                };
                Ok(())
            } else {
                Err(hint.error("unknown optimizer hint"))
            }
        })?;

        Ok(hints)
    }
}

pub struct PlanModel {
    pub ident: Ident,
    pub generics: Generics,
    pub crate_path: Path,
    pub expression: Option<Type>,
    pub hints: Hints,
    pub inputs: Vec<TokenStream>,
    pub input_types: Vec<Type>,
    pub arguments: Vec<TokenStream>,
    pub argument_types: Vec<Type>,
    pub payload: Vec<TokenStream>,
}

impl PlanModel {
    pub fn parse(input: &DeriveInput) -> Result<Self> {
        let Data::Struct(data) = &input.data else {
            return Err(Error::new_spanned(
                input,
                "plan derives can only be applied to structs",
            ));
        };

        let attribute = PlanAttribute::from_attributes(&input.attrs)?;

        let crate_path = match attribute.crate_path {
            Some(path) => path,
            None => resolve_query_crate_path()?,
        };

        let mut inputs = Vec::new();
        let mut input_types = Vec::new();
        let mut arguments = Vec::new();
        let mut argument_types = Vec::new();
        let mut payload = Vec::new();

        for (index, field) in data.fields.iter().enumerate() {
            let accessor = if let Some(name) = &field.ident {
                quote!(#name)
            } else {
                let index = Index::from(index);
                quote!(#index)
            };

            if field
                .attrs
                .iter()
                .any(|attribute| attribute.path().is_ident("input"))
            {
                inputs.push(accessor);
                input_types.push(field.ty.clone());
            } else if field
                .attrs
                .iter()
                .any(|attribute| attribute.path().is_ident("argument"))
            {
                arguments.push(accessor);
                argument_types.push(field.ty.clone());
            } else {
                payload.push(accessor);
            }
        }

        Ok(Self {
            ident: input.ident.clone(),
            generics: input.generics.clone(),
            crate_path,
            expression: attribute.expression,
            hints: attribute.hints,
            inputs,
            input_types,
            arguments,
            argument_types,
            payload,
        })
    }
}

pub fn with_bounds(generics: &Generics, types: &[Type], bound: &TokenStream) -> Generics {
    let mut generics = generics.clone();

    if types.is_empty() {
        return generics;
    }

    let where_clause = generics.make_where_clause();

    for ty in types {
        where_clause.predicates.push(parse_quote!(#ty: #bound));
    }

    generics
}

#[derive(Default)]
struct PlanAttribute {
    expression: Option<Type>,
    crate_path: Option<Path>,
    hints: Hints,
}

impl FromAttributes for PlanAttribute {
    const NAMESPACE: &'static str = "plan";

    fn parse_meta(&mut self, meta: ParseNestedMeta) -> Result<()> {
        if meta.path.is_ident("expression") {
            self.expression = Some(meta.value()?.parse::<Type>()?);
            Ok(())
        } else if meta.path.is_ident("crate") {
            self.crate_path = Some(meta.value()?.parse::<LitStr>()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("optimizer_hints") {
            self.hints = Hints::parse(&meta)?;
            Ok(())
        } else {
            Err(meta.error("unknown plan attribute"))
        }
    }
}
