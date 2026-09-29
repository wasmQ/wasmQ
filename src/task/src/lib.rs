use proc_macro::TokenStream;
use quote::quote;
use syn::{FnArg, ItemFn, ReturnType, Type, parse_macro_input};

#[proc_macro_attribute]
pub fn wasmq_object(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as syn::DeriveInput);
    let name = &input.ident;

    let expanded = quote! {
        #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
        #input

        const _: () = {
            fn assert_impl<T: serde::Serialize + serde::de::DeserializeOwned + Clone + std::fmt::Debug>() {}
            fn assert_type() {
                assert_impl::<#name>();
            }
        };
    };

    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn wasmq_handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_vis = &input_fn.vis;
    let fn_block = &input_fn.block;
    let fn_attrs = &input_fn.attrs;
    let fn_sig = &input_fn.sig;

    // Extract the input type from the first parameter
    let input_type = match fn_sig.inputs.first() {
        Some(FnArg::Typed(pat_type)) => &pat_type.ty,
        _ => {
            return syn::Error::new_spanned(
                &fn_sig.inputs,
                "Handler function must have at least one parameter",
            )
            .to_compile_error()
            .into();
        }
    };

    // Extract the output type from the return type
    let output_type = match &fn_sig.output {
        ReturnType::Type(_, ty) => {
            // Handle Result<T, E> -> extract T
            if let Type::Path(type_path) = ty.as_ref() {
                if let Some(segment) = type_path.path.segments.last() {
                    if segment.ident == "Result" {
                        // Extract the Ok type from Result<T, E>
                        if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                            if let Some(syn::GenericArgument::Type(ok_type)) = args.args.first() {
                                ok_type
                            } else {
                                ty.as_ref()
                            }
                        } else {
                            ty.as_ref()
                        }
                    } else {
                        ty.as_ref()
                    }
                } else {
                    ty.as_ref()
                }
            } else {
                ty.as_ref()
            }
        }
        ReturnType::Default => {
            return syn::Error::new_spanned(
                &fn_sig.output,
                "Handler function must return a Result type",
            )
            .to_compile_error()
            .into();
        }
    };

    let expanded = quote! {
        use anyhow::Result;
        use wit_bindgen;

        mod bindings {
            wit_bindgen::generate!({
                async: true,
                inline: r"
                    package wasmq:runtime@0.1.0;

                    world wasmq {
                        export handler: async func(data: string) -> result<string, string>;
                    }",
            });
        }

        struct Wasmq;

        impl bindings::Guest for Wasmq {
            async fn handler(data: String) -> Result<String, String> {
                let input: #input_type = serde_json::from_str(&data)
                    .map_err(|e| format!("Failed to deserialize input: {}", e))?;

                let result: Result<#output_type> = #fn_name(input).await;

                match result {
                    Ok(val) => serde_json::to_string(&val)
                        .map_err(|e| format!("Failed to serialize output: {}", e)),
                    Err(err) => Err(format!("Handler error: {}", err)),
                }
            }
        }

        bindings::export!(Wasmq with_types_in bindings);

        #(#fn_attrs)*
        #fn_vis #fn_sig {
            #fn_block
        }
    };

    TokenStream::from(expanded)
}
