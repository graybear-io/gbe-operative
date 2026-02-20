# Operative Types

## Implemented

### ShellOperative
Runs `sh -c <command>`, captures stdout lines and exit code.
Params: `{ command }`.

### MockOperative
Returns configurable outcomes per task name. Testing only.

## Proposed Atomic Types

### HttpOperative
Structured HTTP calls without shelling out to curl.
Params: `{ url, method, headers, body }`.
Returns response body as output lines. Validates URLs, enforces timeouts.

### ScriptOperative
Runs a script file rather than an inline command.
Params: `{ script_path, args, interpreter }`.
Validates path exists before execution. Better for multi-line logic.

### AssertOperative
Runs a command and checks output against expectations.
Params: `{ command, expect_stdout, expect_exit_code }`.
Turns jobs into verifiable test suites.

### TemplateOperative
Renders a template file with variable substitution.
Params: `{ template_path, output_path, vars... }`.
Useful for config generation pipelines.

## Composition Architecture

### Core Principle
Composition is not a special operative type — it's recursive use of the
existing execution model. Oracle walks DAGs, driver dispatches tasks,
operatives execute them. A "molecule" is just a task whose operative
builds a sub-DAG and calls `run_job` again.

### MoleculeOperative
One operative type that handles all composite execution:
1. Resolves a molecule definition (inline, file ref, or registry)
2. Builds a JobDefinition from it
3. Wires a CompositeOperative with the right child operatives per step
4. Calls `run_job` with structured data flowing on edges

Different molecule "flavours" are just different operatives that vary
in how they describe the DAG — once the DAG is built, execution is
identical. You can always create a new operative for a new definition
format without touching the execution model.

### Structured Pipes (Unix philosophy, typed)
This is a structured implementation of Unix pipes. Instead of raw byte
streams, DAG edges carry typed payloads:

```
  step-a ──[json payload]──→ step-b ──[json payload]──→ step-c
```

Modeled after tools like n8n: each node receives structured input from
upstream edges and produces structured output for downstream edges.

### Edge Data Model
DAG edges currently carry only dependency signals ("B waits for A").
To enable composition, edges must also carry data:

```rust
/// Output from a completed task, carried on DAG edges.
enum EdgeData {
    /// Small structured data, inline on the edge.
    Inline(serde_json::Value),
    /// Large data stored externally, edge carries a reference.
    Ref { store: String, key: String },
}
```

**Inline** for small results (URLs, extracted fields, status codes).
**Ref** for large results (file contents, API response bodies) — consistent
with the existing pattern where references are passed around and actual
data lives in a store.

### How Data Flows

```yaml
molecule: "web-extract"
steps:
  - name: "render-url"
    operative: template
    params: { template: "https://api.example.com/{{org}}/usage" }
    # output: { "url": "https://api.example.com/acme/usage" }

  - name: "fetch"
    operative: http
    depends_on: ["render-url"]
    input_from: { url: "render-url.url" }
    params: { method: GET }
    # output: { "body_ref": "store://tmp/fetch-result.json", "status": 200 }

  - name: "extract"
    operative: shell
    depends_on: ["fetch"]
    input_from: { stdin_ref: "fetch.body_ref" }
    params: { command: "jq '.data[]'" }
```

`input_from` wires upstream outputs to downstream params using
dot-notation references: `"step-name.field"`. The driver resolves
these before dispatching each task.

### Execution Model

```
Job DAG (outer)
  task-a ──→ molecule-b ──→ task-c
                │
                └─ sub-DAG (inner, same driver loop)
                     render-url ──→ fetch ──→ extract
                        [EdgeData]    [EdgeData]
```

Same fail-fast semantics at both levels: inner step failure fails the
molecule, which fails the outer task.

### Open Questions
- **Edge data schema**: Should `EdgeData` be fully schemaless JSON or
  should molecules declare expected input/output types?
- **Where do molecules live?** Inline in job YAML, separate files
  referenced by name, or a registry?
- **Naming**: "molecule" aligns with Yegge's beads work. Alternatives:
  "pipeline", "recipe", "composite". Molecule fits the Forerunner theme
  least but carries the right semantic weight.
