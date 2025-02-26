use proc_macro::TokenStream;
use proc_macro_error::{abort, proc_macro_error};
use quote::quote;
use serde_json;
use syn::{
    parse_macro_input, punctuated::Punctuated, token::Comma, FnArg, FnArg::Typed, Ident, ItemFn,
    Lit, Meta, Pat, PatType, ReturnType, Token, Type,
};

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
                }
                "Option" => {
                    if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                        if let Some(arg) = args.args.first() {
                            if let syn::GenericArgument::Type(inner_type) = arg {
                                return map_type_to_schema(inner_type);
                            }
                        }
                    }
                    serde_json::json!({ "type": "null" })
                }
                _ => serde_json::json!({ "type": "object", "additionalProperties": false }),
            }
        }
        _ => serde_json::json!({ "type": "object", "additionalProperties": false }),
    }
}

#[proc_macro_attribute]
#[proc_macro_error]
pub fn action(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr_args =
        syn::parse::Parser::parse2(Punctuated::<Meta, Token![,]>::parse_terminated, attr.into())
            .unwrap_or_else(|e| abort!(e.span(), "Failed to parse action attributes: {}", e));

    let description = attr_args
        .iter()
        .find_map(|meta| {
            if let Meta::NameValue(nv) = meta {
                if nv.path.is_ident(DESCRIPTION) {
                    if let syn::Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Str(lit) = &expr_lit.lit {
                            return Some(lit.value());
                        } else {
                            abort!(expr_lit, "Expected a string literal for description");
                        }
                    } else {
                        abort!(nv.value, "Expected a string literal for description");
                    }
                }
            }
            None
        })
        .unwrap_or("No description provided".to_string());

    if attr_args.len() > 1 {
        abort!(
            attr_args[1],
            "Multiple #[action] attributes are not supported"
        );
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
    let has_executor_params = inputs.len() >= 3
        && inputs[0].0 == "_executor"
        && inputs[1].0 == "node"
        && inputs[2].0 == "cache";
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

    let schema_string = serde_json::to_string(&full_schema).unwrap_or_else(|err| {
        abort!(
            input.sig.ident,
            "Failed to serialize schema to JSON: {}",
            err
        )
    });

    // Generate argument list for calling the function
    let arg_names: Vec<Ident> = inputs
        .iter()
        .map(|(name, _, _, _)| Ident::new(name, fn_name.span()))
        .collect();
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

/// Proc macro to define a PubSubAgent from a function.
///
/// # Attributes
/// - `name`: A string identifier for the agent (required).
/// - `description`: A string describing the agent.
/// - `subscribe`: A comma-separated list of subscription channels (e.g., `"tasks, updates"`).
/// - `publish`: A comma-separated list of publication channels (e.g., `"results"`).
/// - `input_schema`: A JSON string defining the input schema.
/// - `output_schema`: A JSON string defining the output schema.
///
/// # Example
/// ```rust
/// #[pubsub_agent(
///     name = "TaskProcessor",
///     description = "Processes tasks and publishes results",
///     subscribe = "tasks",
///     publish = "results",
///     input_schema = r#"{"type": "object", "properties": {"task": {"type": "string"}}}"#,
///     output_schema = r#"{"type": "object", "properties": {"result": {"type": "string"}}}"#
/// )]
/// async fn task_processor(
///     node_id: &str,
///     channel: &str,
///     message: Message,
///     executor: &mut PubSubExecutor,
///     cache: &Cache
/// ) -> Result<()> {
///     let task = message.payload["task"].as_str().ok_or(anyhow!("Missing task"))?;
///     let result_msg = Message::new(node_id.to_string(), json!({"result": format!("Processed: {}", task)}));
///     executor.publish("results", result_msg, cache).await?;
///     Ok(())
/// }
/// ```

#[proc_macro_attribute]
#[proc_macro_error]
pub fn pubsub_agent(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr_args =
        syn::parse::Parser::parse2(Punctuated::<Meta, Token![,]>::parse_terminated, attr.into())
            .unwrap_or_else(|e| abort!(e.span(), "Failed to parse pubsub_agent attributes: {}", e));

    let name = attr_args
        .iter()
        .find_map(|meta| {
            if let Meta::NameValue(nv) = meta {
                if nv.path.is_ident("name") {
                    if let syn::Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Str(lit) = &expr_lit.lit {
                            return Some(lit.value());
                        } else {
                            abort!(expr_lit, "Expected a string literal for name");
                        }
                    } else {
                        abort!(nv.value, "Expected a string literal for name");
                    }
                }
            }
            None
        })
        .unwrap_or_else(|| {
            abort!(
                attr_args,
                "Missing required 'name' attribute for pubsub_agent"
            )
        });

    let description = attr_args
        .iter()
        .find_map(|meta| {
            if let Meta::NameValue(nv) = meta {
                if nv.path.is_ident(DESCRIPTION) {
                    if let syn::Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Str(lit) = &expr_lit.lit {
                            return Some(lit.value());
                        } else {
                            abort!(expr_lit, "Expected a string literal for description");
                        }
                    } else {
                        abort!(nv.value, "Expected a string literal for description");
                    }
                }
            }
            None
        })
        .unwrap_or("No description provided".to_string());

    let subscribe = attr_args
        .iter()
        .find_map(|meta| {
            if let Meta::NameValue(nv) = meta {
                if nv.path.is_ident("subscribe") {
                    if let syn::Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Str(lit) = &expr_lit.lit {
                            return Some(
                                lit.value()
                                    .split(',')
                                    .map(|s| s.trim().to_string())
                                    .collect::<Vec<_>>(),
                            );
                        } else {
                            abort!(expr_lit, "Expected a string literal for subscribe");
                        }
                    } else {
                        abort!(nv.value, "Expected a string literal for subscribe");
                    }
                }
            }
            None
        })
        .unwrap_or_else(|| vec![]);

    let publish = attr_args
        .iter()
        .find_map(|meta| {
            if let Meta::NameValue(nv) = meta {
                if nv.path.is_ident("publish") {
                    if let syn::Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Str(lit) = &expr_lit.lit {
                            return Some(
                                lit.value()
                                    .split(',')
                                    .map(|s| s.trim().to_string())
                                    .collect::<Vec<_>>(),
                            );
                        } else {
                            abort!(expr_lit, "Expected a string literal for publish");
                        }
                    } else {
                        abort!(nv.value, "Expected a string literal for publish");
                    }
                }
            }
            None
        })
        .unwrap_or_else(|| vec![]);

    let input_schema_str = attr_args
        .iter()
        .find_map(|meta| {
            if let Meta::NameValue(nv) = meta {
                if nv.path.is_ident("input_schema") {
                    if let syn::Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Str(lit) = &expr_lit.lit {
                            return Some(lit.value());
                        } else {
                            abort!(expr_lit, "Expected a string literal for input_schema");
                        }
                    } else {
                        abort!(nv.value, "Expected a string literal for input_schema");
                    }
                }
            }
            None
        })
        .unwrap_or(r#"{"type": "object", "additionalProperties": true}"#.to_string());

    let output_schema_str = attr_args
        .iter()
        .find_map(|meta| {
            if let Meta::NameValue(nv) = meta {
                if nv.path.is_ident("output_schema") {
                    if let syn::Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Str(lit) = &expr_lit.lit {
                            return Some(lit.value());
                        } else {
                            abort!(expr_lit, "Expected a string literal for output_schema");
                        }
                    } else {
                        abort!(nv.value, "Expected a string literal for output_schema");
                    }
                }
            }
            None
        })
        .unwrap_or(r#"{"type": "object", "additionalProperties": true}"#.to_string());

    let input = parse_macro_input!(item as ItemFn);
    let fn_name = &input.sig.ident;
    let fn_name_str = fn_name.to_string();
    let fn_vis = &input.vis;
    let struct_name = syn::Ident::new(&format!("__{}Agent", fn_name_str), fn_name.span());

    // Validate schemas at compile time
    if let Err(e) = serde_json::from_str::<serde_json::Value>(&input_schema_str) {
        abort!(input.sig, "Invalid input_schema JSON: {}", e);
    }
    if let Err(e) = serde_json::from_str::<serde_json::Value>(&output_schema_str) {
        abort!(input.sig, "Invalid output_schema JSON: {}", e);
    }

    // Parse function inputs dynamically
    let inputs: Vec<(String, Type)> = input
        .sig
        .inputs
        .iter()
        .filter_map(|arg| {
            if let syn::FnArg::Typed(PatType { pat, ty, .. }) = arg {
                let param_name = match pat.as_ref() {
                    Pat::Ident(pat_ident) => pat_ident.ident.to_string(),
                    _ => return None,
                };
                Some((param_name, *ty.clone()))
            } else {
                None
            }
        })
        .collect();

    // Check that required parameters are present, regardless of order
    let has_executor = inputs.iter().any(|(name, _)| name == "executor");
    let has_message = inputs.iter().any(|(name, _)| name == "message");

    if !has_executor || !has_message {
        abort!(input.sig.inputs, "Function must include both `executor: &mut PubSubExecutor` and `message: Message` parameters");
    }

    // Generate call with all parameters, adjusting for cache dereference
    let arg_names: Vec<Ident> = inputs
        .iter()
        .map(|(name, _)| Ident::new(name, fn_name.span()))
        .collect();
    let fn_args: Vec<proc_macro2::TokenStream> = inputs
        .iter()
        .enumerate()
        .map(|(i, (name, _))| {
            match name.as_str() {
                "executor" => quote! { executor },
                "message" => quote! { message },
                "cache" => quote! { &*cache }, // Dereference Arc<Cache> to &Cache
                "channel" => quote! { channel },
                "node_id" => quote! { node_id },
                _ => abort!(input.sig.inputs, "Unsupported parameter: {}", name), // Error out gracefully for unsupported parameters
            }
        })
        .collect();
    let fn_call = quote! { #fn_name(#(#fn_args),*).await };

    let subscriptions = subscribe.iter().map(|s| quote!(#s.to_string()));
    let publications = publish.iter().map(|s| quote!(#s.to_string()));

    let expanded = quote! {
            #input

            #[allow(non_camel_case_types)]
            #fn_vis struct #struct_name {
                input_schema: jsonschema::JSONSchema,
                input_schema_value: serde_json::Value,
                output_schema: jsonschema::JSONSchema,
                output_schema_value: serde_json::Value,
            }

            impl #struct_name {
                pub fn new() -> Self {
                    let input_schema_value: serde_json::Value = serde_json::from_str(#input_schema_str)
                        .unwrap_or_else(|e| panic!("Failed to parse input schema: {}", e));
                    let output_schema_value: serde_json::Value = serde_json::from_str(#output_schema_str)
                        .unwrap_or_else(|e| panic!("Failed to parse output schema: {}", e));
                    Self {
                        input_schema: jsonschema::JSONSchema::compile(&input_schema_value)
                            .unwrap_or_else(|e| panic!("Failed to compile input schema: {}", e)),
                        input_schema_value,
                        output_schema: jsonschema::JSONSchema::compile(&output_schema_value)
                            .unwrap_or_else(|e| panic!("Failed to compile output schema: {}", e)),
                        output_schema_value,
                    }
                }

                // Helper method for publishing with automatic source tracking
                pub async fn publish_message(
                    &self,
                    node_id: &str,
                    channel: &str,
                    payload: serde_json::Value,
                    executor: &mut dagger::PubSubExecutor,
                    cache: &std::sync::Arc<dagger::Cache>
                ) -> anyhow::Result<String> {
                    // Use the current agent ID if available, otherwise fall back to node_id
                    let source_id = executor.get_current_agent_id().unwrap_or_else(|| node_id.to_string());
                    let msg = dagger::Message::new(source_id, payload);
                    executor.publish(channel, msg, cache).await.map_err(|e| anyhow::anyhow!(e))
                }
            }

            #[async_trait::async_trait]
            impl dagger::PubSubAgent for #struct_name {
                fn name(&self) -> String { #name.to_string() }
                fn description(&self) -> String { #description.to_string() }
                fn subscriptions(&self) -> Vec<String> { vec![#(#subscriptions),*] }
                fn publications(&self) -> Vec<String> { vec![#(#publications),*] }
                fn input_schema(&self) -> serde_json::Value { self.input_schema_value.clone() }
                fn output_schema(&self) -> serde_json::Value { self.output_schema_value.clone() }

                async fn process_message(&self, node_id: &str, channel: &str, message: dagger::Message, executor: &mut dagger::PubSubExecutor, cache: std::sync::Arc<dagger::Cache>) -> anyhow::Result<()> {
                    // Record this execution in the tree

                    // Generate the unique refit ID outside the tree block scope so we can use it later
                    let refit = format!("{}_{}", self.name(), chrono::Utc::now().timestamp_millis());
                    
                   let agent_idx = {
                        let mut tree = executor.execution_tree.write().await;
                        
                        let agent_node = dagger::NodeSnapshot {
                            node_id: refit.clone(), // Ensure unique, stable node ID
                            outcome: dagger::NodeExecutionOutcome {
                                node_id: self.name(),
                                success: true,  // Optimistic initially
                                retry_messages: Vec::new(),
                                final_error: None,
                            },
                            cache_ref: refit.clone(),
                            timestamp: chrono::Local::now().naive_local(),
                            channel: Some(channel.to_string()),
                            message_id: Some(message.source.clone()),
                        };

                        let agent_idx = tree.add_node(agent_node);

                        // Connect to source node if available - improved lookup
                        if !message.source.is_empty() {
                            
                            // Use more robust source node finding logic
                            if let Some(source_idx) = tree.node_indices().find(|idx|
                                tree.node_weight(*idx).map_or(false, |node|
                                    node.node_id == message.source || node.message_id.as_ref() == Some(&message.source)
                                )
                            ) {
                                tree.add_edge(
                                    source_idx,
                                    agent_idx,
                                    dagger::ExecutionEdge {
                                        parent: message.source.clone(),
                                        label: format!("processed_message"), // Avoid channel name in the label
                                    },
                                );
                            }
                        }
                        agent_idx
                    };
                    
                    // Store the refit ID in the cache for use in the function
                    executor.set_current_agent_id(refit.clone());
                    
                    // Validate input
                    self.validate_input(&message.payload)?;

                    // Process the message
                    let result = match #fn_call {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            // Update node with error info
                            let mut tree = executor.execution_tree.write().await;
                            if let Some(node) = tree.node_weight_mut(agent_idx) {
                                node.outcome.success = false;
                                node.outcome.final_error = Some(e.to_string());
                            }
                            Err(anyhow::anyhow!(e))
                        }
                    }?;

                    // Clear the current agent ID
                    executor.clear_current_agent_id();
                    
                    Ok(())
                }

                fn validate_input(&self, payload: &serde_json::Value) -> anyhow::Result<()> {
                    if let Err(errors) = self.input_schema.validate(payload) {
                        let error_messages: Vec<String> = errors.collect::<Vec<_>>().iter().map(|e| e.to_string()).collect();
                        Err(anyhow::anyhow!("Input validation failed: {}", error_messages.join(", ")))
                    } else {
                        Ok(())
                    }
                }

                fn validate_output(&self, payload: &serde_json::Value) -> anyhow::Result<()> {
                    if let Err(errors) = self.output_schema.validate(payload) {
                        let error_messages: Vec<String> = errors.collect::<Vec<_>>().iter().map(|e| e.to_string()).collect();
                        Err(anyhow::anyhow!("Output validation failed: {}", error_messages.join(", ")))
                    } else {
                        Ok(())
                    }
                }
            }
        };

    TokenStream::from(expanded)
}
