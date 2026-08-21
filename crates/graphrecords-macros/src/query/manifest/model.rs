use syn::{
    Error, Ident, LitStr, Path, Result, Token, Type,
    parse::{Parse, ParseStream},
    token,
};

mod keyword {
    syn::custom_keyword!(method);
    syn::custom_keyword!(policy);
    syn::custom_keyword!(scope);
    syn::custom_keyword!(kernel);
    syn::custom_keyword!(group);
    syn::custom_keyword!(parameters);
    syn::custom_keyword!(selector);
    syn::custom_keyword!(field);
    syn::custom_keyword!(argument);
    syn::custom_keyword!(receiver);
    syn::custom_keyword!(input);
    syn::custom_keyword!(output);
    syn::custom_keyword!(emission);
    syn::custom_keyword!(ArgumentSource);
    syn::custom_keyword!(SetSource);
    syn::custom_keyword!(IndexedElementSource);
    syn::custom_keyword!(Indexed);
    syn::custom_keyword!(Retention);
    syn::custom_keyword!(IndexDomain);
    syn::custom_keyword!(ValueDomain);
    syn::custom_keyword!(Owned);
}

pub struct Manifest {
    pub operation: Type,
    pub registry_name: Option<LitStr>,
    pub method_trait: Ident,
    pub trait_arguments: Option<Vec<Type>>,
    pub method: Ident,
    pub policy: Option<Policy>,
    pub scope: Scope,
    pub kernels: Vec<Kernel>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Element,
    Lane,
    Group,
}

pub struct Policy {
    pub path: Path,
    pub constructor: Option<PolicyConstructor>,
}

pub struct PolicyConstructor {
    pub owner: Ident,
    pub call: PolicyCall,
}

pub enum PolicyCall {
    Dot(Ident),
    Path(Ident),
    Tuple,
}

pub struct Kernel {
    pub group: Option<GroupKeys>,
    pub parameters: Vec<Parameter>,
    pub selector: Option<Path>,
    pub fields: Vec<Field>,
    pub arguments: Vec<Argument>,
    pub receiver: Option<Ident>,
    pub input: KernelInput,
    pub output: Type,
    pub emission: Option<Type>,
    pub where_owned: Option<WhereOwned>,
}

pub struct GroupKeys {
    pub member: Ident,
    pub key: Ident,
}

pub struct Parameter {
    pub name: Ident,
    pub bound: Ident,
    pub target: Option<Type>,
    pub additional: Vec<Ident>,
}

pub struct Field {
    pub field_type: Ident,
}

pub enum Argument {
    Value(Box<ValueArgument>),
    Set(SetArgument),
    Via(ViaArgument),
}

pub struct ValueArgument {
    pub name: Ident,
    pub alignment: Type,
    pub value: Option<Type>,
    pub retention: Option<Type>,
    pub capability: Option<Ident>,
}

pub struct SetArgument {
    pub name: Ident,
    pub value: Type,
}

pub struct ViaArgument {
    pub name: Ident,
    pub index: Ident,
    pub value: Ident,
    pub arity: Ident,
}

pub struct WhereOwned {
    pub owner: Ident,
    pub bounds: Vec<Ident>,
}

pub enum KernelInput {
    Lane { shape: Type, arity: Box<Type> },
    Shape(Type),
}

impl Kernel {
    pub fn value_arguments(&self) -> Result<Vec<&ValueArgument>> {
        self.arguments
            .iter()
            .map(|argument| match argument {
                Argument::Value(value) => Ok(value.as_ref()),
                Argument::Set(set) => Err(Error::new(
                    set.name.span(),
                    "a set argument must be the kernel's only argument",
                )),
                Argument::Via(via) => Err(Error::new(
                    via.name.span(),
                    "a via argument must be the kernel's only argument",
                )),
            })
            .collect()
    }

    pub fn set_argument(&self) -> Option<&SetArgument> {
        match &self.arguments[..] {
            [Argument::Set(set)] => Some(set),
            _ => None,
        }
    }

    pub fn via_argument(&self) -> Option<&ViaArgument> {
        match &self.arguments[..] {
            [Argument::Via(via)] => Some(via),
            _ => None,
        }
    }

    pub fn retention_argument(&self) -> Option<&ValueArgument> {
        match &self.arguments[..] {
            [Argument::Value(value)] if value.retention.is_some() => Some(value),
            _ => None,
        }
    }

    pub fn plain_value_argument(&self) -> Option<&ValueArgument> {
        match &self.arguments[..] {
            [Argument::Value(value)] if value.value.is_some() && value.retention.is_none() => {
                Some(value)
            }
            _ => None,
        }
    }

    pub const fn shape(&self) -> &Type {
        match &self.input {
            KernelInput::Lane { shape, .. } | KernelInput::Shape(shape) => shape,
        }
    }
}

impl Parameter {
    pub fn is_bare(&self, bound: &str) -> bool {
        self.bound == bound && self.target.is_none() && self.additional.is_empty()
    }
}

pub fn type_ident(candidate: &Type) -> Option<&Ident> {
    let Type::Path(path) = candidate else {
        return None;
    };

    if path.qself.is_some() || path.path.segments.len() != 1 {
        return None;
    }

    let segment = &path.path.segments[0];
    segment.arguments.is_none().then_some(&segment.ident)
}

pub fn type_application<'a>(candidate: &'a Type, name: &str) -> Option<Vec<&'a Type>> {
    let Type::Path(path) = candidate else {
        return None;
    };

    if path.qself.is_some() || path.path.segments.len() != 1 {
        return None;
    }

    let segment = &path.path.segments[0];
    if segment.ident != name {
        return None;
    }

    let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };

    arguments
        .args
        .iter()
        .map(|argument| match argument {
            syn::GenericArgument::Type(inner) => Some(inner),
            _ => None,
        })
        .collect()
}

impl Parse for Manifest {
    fn parse(input: ParseStream) -> Result<Self> {
        let operation = input.parse()?;
        let registry_name = if input.peek(Token![as]) {
            input.parse::<Token![as]>()?;
            Some(input.parse()?)
        } else {
            None
        };

        let body;
        syn::braced!(body in input);

        body.parse::<keyword::method>()?;
        body.parse::<Token![:]>()?;
        let method_trait = body.parse()?;
        let trait_arguments = if body.peek(Token![<]) {
            Some(parse_trait_arguments(&body)?)
        } else {
            None
        };
        body.parse::<Token![::]>()?;
        let method = body.parse()?;
        body.parse::<Token![;]>()?;

        let policy = if body.peek(keyword::policy) {
            body.parse::<keyword::policy>()?;
            body.parse::<Token![:]>()?;
            Some(Policy::parse_declaration(&body)?)
        } else {
            None
        };

        body.parse::<keyword::scope>()?;
        body.parse::<Token![:]>()?;
        let scope = Scope::parse_name(&body)?;
        body.parse::<Token![;]>()?;

        let mut kernels = Vec::new();
        while body.peek(keyword::kernel) {
            body.parse::<keyword::kernel>()?;
            let block;
            syn::braced!(block in body);
            kernels.push(Kernel::parse_block(&block)?);
        }

        if kernels.is_empty() {
            return Err(body.error("a manifest must declare at least one kernel"));
        }

        Ok(Self {
            operation,
            registry_name,
            method_trait,
            trait_arguments,
            method,
            policy,
            scope,
            kernels,
        })
    }
}

fn parse_trait_arguments(body: ParseStream) -> Result<Vec<Type>> {
    body.parse::<Token![<]>()?;

    let mut arguments = vec![body.parse()?];
    while body.peek(Token![,]) {
        body.parse::<Token![,]>()?;
        if body.peek(Token![>]) {
            break;
        }
        arguments.push(body.parse()?);
    }
    body.parse::<Token![>]>()?;

    Ok(arguments)
}

impl Policy {
    fn parse_declaration(body: ParseStream) -> Result<Self> {
        let path = body.parse()?;
        let constructor = if body.peek(Token![=]) {
            body.parse::<Token![=]>()?;
            let owner = body.parse()?;
            let call = if body.peek(Token![.]) {
                body.parse::<Token![.]>()?;
                PolicyCall::Dot(body.parse()?)
            } else if body.peek(Token![::]) {
                body.parse::<Token![::]>()?;
                PolicyCall::Path(body.parse()?)
            } else {
                PolicyCall::Tuple
            };
            let argument;
            syn::parenthesized!(argument in body);
            argument.parse::<Ident>()?;
            Some(PolicyConstructor { owner, call })
        } else {
            None
        };
        body.parse::<Token![;]>()?;

        Ok(Self { path, constructor })
    }
}

impl Scope {
    fn parse_name(body: ParseStream) -> Result<Self> {
        let name = body.parse::<Ident>()?;

        match name.to_string().as_str() {
            "element" => Ok(Self::Element),
            "lane" => Ok(Self::Lane),
            "group" => Ok(Self::Group),
            _ => Err(Error::new(
                name.span(),
                "scope must be element, lane, or group",
            )),
        }
    }
}

impl Kernel {
    fn parse_block(block: ParseStream) -> Result<Self> {
        let group = if block.peek(keyword::group) {
            Some(GroupKeys::parse_declaration(block)?)
        } else {
            None
        };

        block.parse::<keyword::parameters>()?;
        block.parse::<Token![:]>()?;
        block.parse::<Token![<]>()?;
        let mut parameters = Vec::new();
        while !block.peek(Token![>]) {
            parameters.push(Parameter::parse_entry(block)?);
            if block.peek(Token![,]) {
                block.parse::<Token![,]>()?;
            } else {
                break;
            }
        }
        block.parse::<Token![>]>()?;
        block.parse::<Token![;]>()?;

        let selector = if block.peek(keyword::selector) {
            block.parse::<keyword::selector>()?;
            block.parse::<Token![:]>()?;
            let path = block.parse()?;
            block.parse::<Token![;]>()?;
            Some(path)
        } else {
            None
        };

        let mut fields = Vec::new();
        while block.peek(keyword::field) {
            block.parse::<keyword::field>()?;
            block.parse::<Token![:]>()?;
            block.parse::<Ident>()?;
            block.parse::<Token![:]>()?;
            let field_type = block.parse()?;
            block.parse::<Token![;]>()?;
            fields.push(Field { field_type });
        }

        let mut arguments = Vec::new();
        while block.peek(keyword::argument) {
            arguments.push(Argument::parse_entry(block)?);
        }

        let receiver = if block.peek(keyword::receiver) {
            block.parse::<keyword::receiver>()?;
            block.parse::<Token![:]>()?;
            let name = block.parse()?;
            block.parse::<Token![;]>()?;
            Some(name)
        } else {
            None
        };

        block.parse::<keyword::input>()?;
        block.parse::<Token![:]>()?;
        let input = if block.peek(token::Paren) {
            let pair;
            syn::parenthesized!(pair in block);
            let shape = pair.parse()?;
            pair.parse::<Token![,]>()?;
            let arity = pair.parse()?;
            KernelInput::Lane {
                shape,
                arity: Box::new(arity),
            }
        } else {
            KernelInput::Shape(block.parse()?)
        };
        block.parse::<Token![;]>()?;

        block.parse::<keyword::output>()?;
        block.parse::<Token![:]>()?;
        let output = block.parse()?;
        block.parse::<Token![;]>()?;

        let emission = if block.peek(keyword::emission) {
            block.parse::<keyword::emission>()?;
            block.parse::<Token![:]>()?;
            let emission = block.parse()?;
            block.parse::<Token![;]>()?;
            Some(emission)
        } else {
            None
        };

        let where_owned = if block.peek(Token![where]) {
            Some(WhereOwned::parse_declaration(block)?)
        } else {
            None
        };

        Ok(Self {
            group,
            parameters,
            selector,
            fields,
            arguments,
            receiver,
            input,
            output,
            emission,
            where_owned,
        })
    }
}

impl GroupKeys {
    fn parse_declaration(block: ParseStream) -> Result<Self> {
        block.parse::<keyword::group>()?;
        block.parse::<Token![:]>()?;
        block.parse::<Token![<]>()?;
        let member = block.parse()?;
        block.parse::<Token![:]>()?;
        block.parse::<keyword::IndexDomain>()?;
        block.parse::<Token![,]>()?;
        let key = block.parse()?;
        block.parse::<Token![:]>()?;
        block.parse::<keyword::IndexDomain>()?;
        if block.peek(Token![,]) {
            block.parse::<Token![,]>()?;
        }
        block.parse::<Token![>]>()?;
        block.parse::<Token![;]>()?;

        Ok(Self { member, key })
    }
}

impl Parameter {
    fn parse_entry(block: ParseStream) -> Result<Self> {
        let name = block.parse()?;
        block.parse::<Token![:]>()?;
        let bound = block.parse()?;
        let target = if block.peek(Token![<]) {
            block.parse::<Token![<]>()?;
            let target = block.parse()?;
            block.parse::<Token![>]>()?;
            Some(target)
        } else {
            None
        };
        let mut additional = Vec::new();
        while block.peek(Token![+]) {
            block.parse::<Token![+]>()?;
            additional.push(block.parse()?);
        }

        Ok(Self {
            name,
            bound,
            target,
            additional,
        })
    }
}

impl Argument {
    fn parse_entry(block: ParseStream) -> Result<Self> {
        block.parse::<keyword::argument>()?;
        block.parse::<Token![:]>()?;
        let name = block.parse::<Ident>()?;
        block.parse::<Token![:]>()?;

        if block.peek(keyword::SetSource) {
            block.parse::<keyword::SetSource>()?;
            block.parse::<Token![<]>()?;
            let value = block.parse()?;
            block.parse::<Token![>]>()?;
            block.parse::<Token![;]>()?;
            return Ok(Self::Set(SetArgument { name, value }));
        }

        if block.peek(keyword::IndexedElementSource) {
            block.parse::<keyword::IndexedElementSource>()?;
            block.parse::<Token![<]>()?;
            block.parse::<keyword::Indexed>()?;
            block.parse::<Token![<]>()?;
            let index = block.parse()?;
            block.parse::<Token![,]>()?;
            let value = block.parse()?;
            block.parse::<Token![>]>()?;
            block.parse::<Token![,]>()?;
            let arity = block.parse()?;
            block.parse::<Token![>]>()?;
            block.parse::<Token![;]>()?;
            return Ok(Self::Via(ViaArgument {
                name,
                index,
                value,
                arity,
            }));
        }

        block.parse::<keyword::ArgumentSource>()?;
        block.parse::<Token![<]>()?;
        let alignment = block.parse()?;
        let mut value = None;
        let mut retention = None;
        while block.peek(Token![,]) {
            block.parse::<Token![,]>()?;
            if block.peek(keyword::Retention) {
                block.parse::<keyword::Retention>()?;
                block.parse::<Token![=]>()?;
                retention = Some(block.parse()?);
            } else {
                value = Some(block.parse()?);
            }
        }
        block.parse::<Token![>]>()?;

        let capability = if block.peek(Token![where]) {
            block.parse::<Token![where]>()?;
            block.parse::<Ident>()?;
            block.parse::<Token![::]>()?;
            block.parse::<keyword::ValueDomain>()?;
            block.parse::<Token![:]>()?;
            Some(block.parse()?)
        } else {
            None
        };
        block.parse::<Token![;]>()?;

        Ok(Self::Value(Box::new(ValueArgument {
            name,
            alignment,
            value,
            retention,
            capability,
        })))
    }
}

impl WhereOwned {
    fn parse_declaration(block: ParseStream) -> Result<Self> {
        block.parse::<Token![where]>()?;
        let owner = block.parse()?;
        block.parse::<Token![::]>()?;
        block.parse::<keyword::Owned>()?;
        block.parse::<Token![:]>()?;
        let mut bounds = vec![block.parse()?];
        while block.peek(Token![+]) {
            block.parse::<Token![+]>()?;
            bounds.push(block.parse()?);
        }
        block.parse::<Token![;]>()?;

        Ok(Self { owner, bounds })
    }
}
