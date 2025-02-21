use proc_macro::TokenStream;
use proc_macro_error::{proc_macro_error, abort};
use quote::quote;
use syn::{
    parse_macro_input, punctuated::Punctuated, token::Comma, FnArg, Ident, ItemFn, Pat, PatType,
    ReturnType, Type, Lit, Meta, Token, FnArg::Typed,
};
use serde_json;
use async_trait::async_trait;

const TYPE: &str = "type";
const OBJECT: &str = "object";
const DESCRIPTION: &str = "description";
const PROPERTIES: &str = "properties";
const REQUIRED: &str = "required";
const RETURNS: &str = "returns";

fn map_type_to_schema(ty: &Type) -> serde_json::Value {
    match ty {
        Type::Path(tp) => {
            let segments = &tp.path.segments;
            let last_segment = segments.last().unwrap();
            let type_name = last_segment.ident.to_string();
            match type_name.as_str() {
                "String" => serde_json::json!({ "type": "string" }),
                "i32" | "u32" | "i64" | "u64" => serde_json::json!({ "type": "integer" }),
                "f32" | "f64" => serde_json::json!({ "type": "number" }),
                "bool" => serde_json::json!({ "type": "boolean" }),
                "Vec" => {
                    if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                        if let Some(arg) = args.args.first() {
                            if let syn::GenericArgument::Type(inner_type) = arg {
                                let inner_schema = map_type_to_schema(inner_type);
                                return serde_json::json!({
                                    "type": "array",
                                    "items": inner_schema
                                });
                            }
                        }
                    }
                    serde_json::json!({ "type": "array" })
                },
                "Option" => {
                    if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                        if let Some(arg) = args.args.first() {
                            if let syn::GenericArgument::Type(inner_type) = arg {
                                return map_type_to_schema(inner_type);
                            }
                        }
                    }
                    serde_json::json!({ "type": "null" })
                },
                _ => serde_json::json!({ "type": "object", "additionalProperties": false }),
            }
        }
        _ => serde_json::json!({ "type": "object", "additionalProperties": false }),
    }
}

#[proc_macro_attribute]
#[proc_macro_error]
pub fn action(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr_args = syn::parse::Parser::parse2(Punctuated::<Meta, Token![,]>::parse_terminated, attr.into())
        .unwrap_or_else(|e| abort!(e.span(), "Failed to parse action attributes: {}", e));

    let description = attr_args.iter().find_map(|meta| {
        if let Meta::NameValue(nv) = meta {
            if nv.path.is_ident(DESCRIPTION) {
                if let syn::Expr::Lit(expr_lit) = &nv.value {
                    if let Lit::Str(lit) = &expr_lit.lit {
                        return Some(lit.value());
                    } else {
                        abort!(expr_lit, "Expected a string literal for the description");
                    }
                } else {
                    abort!(nv.value, "Expected a string literal for the description");
                }
            }
        }
        None
    }).unwrap_or("No description provided".to_string());

    if attr_args.len() > 1 {
        abort!(attr_args[1], "Multiple #[action] attributes are not supported");
    }

    let input = parse_macro_input!(item as ItemFn);
    let fn_name = &input.sig.ident;
    let fn_name_str = fn_name.to_string();
    let fn_vis = &input.vis;
    let struct_name = syn::Ident::new(&format!("__{}Action", fn_name_str), fn_name.span());
    let static_name = syn::Ident::new(&fn_name_str.to_uppercase(), fn_name.span());

    // Parse all inputs, including additional parameters
    let inputs: Vec<(String, Type, Option<String>, bool)> = input.sig.inputs.iter().filter_map(|arg| {
        if let Typed(PatType { pat, ty, attrs, .. }) = arg {
            let param_name = match pat.as_ref() {
                Pat::Ident(pat_ident) => pat_ident.ident.to_string(),
                _ => return None,
            };
            let is_optional = matches!(&**ty, Type::Path(tp) if tp.path.segments.iter().any(|seg| seg.ident == "Option"));
            let description = attrs.iter().find_map(|attr| {
                if attr.path().is_ident("param") {
                    if let Ok(syn::Meta::NameValue(nv)) = attr.parse_args() {
                        if nv.path.is_ident(DESCRIPTION) {
                            if let syn::Expr::Lit(expr_lit) = &nv.value {
                                if let syn::Lit::Str(lit) = &expr_lit.lit {
                                    return Some(lit.value());
                                } else {
                                    abort!(expr_lit, "Expected a string literal for param description");
                                }
                            } else {
                                abort!(nv.value, "Expected a string literal for param description");
                            }
                        } else {
                            abort!(nv.path, "Only 'description' is supported in #[param]");
                        }
                    } else {
                        abort!(attr, "Expected key-value #[param] attributes");
                    }
                }
                None
            });
            Some((param_name, *ty.clone(), description, is_optional))
        } else {
            None
        }
    }).collect();

    // Parse return type
    let return_type = match &input.sig.output {
        ReturnType::Default => syn::parse_quote!(()),
        ReturnType::Type(_, ty) => (*ty).clone(),
    };
    let return_type_schema = map_type_to_schema(&return_type);

    // Check for standard DAG executor parameters
    let has_executor_params = inputs.len() >= 3 &&
        inputs[0].0 == "_executor" && inputs[1].0 == "node" && inputs[2].0 == "cache";
    let extra_params = if has_executor_params {
        &inputs[3..] // Skip _executor, node, cache
    } else {
        &inputs[..] // All params are extra if no standard params
    };

    // Build properties for extra parameters
    let properties: serde_json::Map<String, serde_json::Value> = extra_params
        .iter()
        .map(|(name, ty, desc, is_optional)| {
            let type_schema = map_type_to_schema(ty);
            let schema = if *is_optional {
                serde_json::json!({
                    "description": desc.as_ref().unwrap_or(&String::new()),
                    "type": ["null", type_schema["type"]]
                })
            } else {
                serde_json::json!({
                    "description": desc.as_ref().unwrap_or(&String::new()),
                    "type": type_schema["type"]
                })
            };
            (name.clone(), schema)
        })
        .collect();

    let required: Vec<String> = extra_params
        .iter()
        .filter(|(_, _, _, is_opt)| !is_opt)
        .map(|(name, _, _, _)| name.clone())
        .collect();

    let params_schema = if extra_params.len() == 1 && !extra_params[0].3 {
        map_type_to_schema(&extra_params[0].1)
    } else {
        serde_json::json!({
            TYPE: OBJECT,
            PROPERTIES: properties,
            REQUIRED: required,
            "additionalProperties": false
        })
    };

    let full_schema = serde_json::json!({
        "name": fn_name_str,
        DESCRIPTION: description,
        "parameters": params_schema,
        RETURNS: return_type_schema,
        "additionalProperties": false
    });

    let schema_string = serde_json::to_string(&full_schema)
        .unwrap_or_else(|err| abort!(input.sig.ident, "Failed to serialize schema to JSON: {}", err));

    // Generate argument list for calling the function
    let arg_names: Vec<Ident> = inputs.iter().map(|(name, _, _, _)| Ident::new(name, fn_name.span())).collect();
    let execute_call = if has_executor_params {
        let extra_args = &arg_names[3..];
        quote! {
            #fn_name(executor, node, cache, #(#extra_args),*).await
        }
    } else {
        quote! {
            #fn_name(#(#arg_names),*).await
        }
    };

    let expanded = quote! {
        #input

        #[allow(non_camel_case_types)]
        #fn_vis struct #struct_name;

        impl Clone for #struct_name {
            fn clone(&self) -> Self { #struct_name {} }
        }

        #[async_trait]
        impl dagger::NodeAction for #struct_name {
            fn name(&self) -> String { #fn_name_str.to_string() }
            async fn execute(&self, executor: &mut dagger::DagExecutor, node: &dagger::Node, cache: &dagger::Cache) -> anyhow::Result<()> {
                let result = #execute_call?;
                dagger::insert_value(cache, &node.id, "result", &result)?;
                Ok(())
            }
            fn schema(&self) -> serde_json::Value {
                serde_json::from_str(#schema_string).expect("Invalid JSON generated internally")
            }
        }

        #fn_vis static #static_name: #struct_name = #struct_name {};
    };

    TokenStream::from(expanded)
}