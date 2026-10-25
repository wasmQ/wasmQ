use anyhow::{Context, Result};
use bytes::Bytes;
use serde_json::Value;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::p2::add_to_linker_async;
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::p2::add_only_http_to_linker_async;
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpCtxView, WasiHttpView, default_hooks};

const HANDLER_FUNC_FQN: &str = "handler";

pub struct ComponentRunStates {
    pub wasi_ctx: WasiCtx,
    pub wasi_http_ctx: WasiHttpCtx,
    pub resource_table: ResourceTable,
}

impl WasiView for ComponentRunStates {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.resource_table,
        }
    }
}

impl WasiHttpView for ComponentRunStates {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.wasi_http_ctx,
            table: &mut self.resource_table,
            hooks: default_hooks(),
        }
    }
}

#[derive(Clone)]
pub struct WasmtimeRuntime {}

impl WasmtimeRuntime {
    pub fn new() -> Self {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("Failed to install rustls crypto provider `aws_lc_rs`");

        Self {}
    }

    pub async fn execute(self, wasm_module: Bytes, input: Bytes) -> Result<Value> {
        let mut config = Config::new();
        config
            .wasm_component_model_async(true)
            .wasm_component_model(true)
            .wasm_component_model_async(true)
            .wasm_component_model_more_async_builtins(true);
        let engine = Engine::new(&config)?;
        let mut linker = Linker::new(&engine);

        add_to_linker_async(&mut linker)?;
        add_only_http_to_linker_async(&mut linker)?;

        let json_value =
            serde_json::from_slice::<Value>(&input).context("Failed to parse input JSON")?;
        let json = serde_json::to_string(&json_value).context("Failed to serialize input JSON")?;
        let wasi = WasiCtx::builder().build();
        let state = ComponentRunStates {
            wasi_ctx: wasi,
            resource_table: ResourceTable::new(),
            wasi_http_ctx: WasiHttpCtx::new(),
        };
        let mut store = Store::new(&engine, state);
        let component = Component::from_binary(&engine, &wasm_module)?;
        let instance = linker.instantiate_async(&mut store, &component).await?;
        let func = instance
            .get_typed_func::<(String,), (Result<String, String>,)>(&mut store, HANDLER_FUNC_FQN)?;
        let (result,) = func.call_async(&mut store, (json,)).await?;

        match result {
            Ok(success) => {
                serde_json::from_str(&success).context("Failed to parse successful output JSON")
            }
            Err(failure) => Err(anyhow::anyhow!("WASM module execution failed: {}", failure)),
        }
    }
}
