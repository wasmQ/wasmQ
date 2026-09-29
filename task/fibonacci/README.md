## Fibonacci Task Example

This example demonstrates how to use the Fibonacci task in wasmq to calculate
the nth Fibonacci number.

### Usage

1. Build the task using the Justfile recipe as follows:

```bash
just build-task fibonacci
```

2. Run the task with the required arguments:

```bash
wasmq task run \
--source ./fibonacci.wasm \
--args '100'
```
