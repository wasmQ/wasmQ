## HTTP Task Example

This example demonstrates how to use the HTTP task in wasmq to perform a POST request
to a specified API endpoint.

### Usage

1. Build the task using the Justfile recipe as follows:

```bash
just build-task http
```

2. Run the task with the required arguments:

```bash
wasmq task run \
--source ./http.wasm \
--args '{
        "api_url": "https://httpbin.org/post",
        "data": {
            "sample_key": "sample_value"
        }
    }'
```
