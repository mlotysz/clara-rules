# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Clara Rules is a forward-chaining rules engine for Clojure and ClojureScript, implementing the Rete algorithm. Fork of [oracle-samples/clara-rules](https://github.com/oracle-samples/clara-rules). Documentation: http://www.clara-rules.org/

## Build & Test Commands

Build tool: **Clojure CLI** (deps.edn + tools.build)

```bash
clojure -T:build javac                      # Compile Java sources (required before tests)
clojure -T:build javac-main                  # Compile only production Java sources
clojure -M:test                              # Run standard tests (excludes generative/performance)
clojure -M:test-generative                   # Run property-based generative tests
clojure -M:test-performance                  # Run performance tests
clojure -M:cljs-test -m cljs-build simple    # Compile CLJS tests (whitespace optimization)
clojure -M:cljs-test -m cljs-build advanced  # Compile CLJS tests (advanced optimization)
node src/test/js/runner.js src/test/html/simple.html    # Run CLJS tests (after compile)
node src/test/js/runner.js src/test/html/advanced.html  # Run CLJS tests (after compile)
clojure -M:dev -m clj-kondo.main --lint src/main:src/test --fail-level error  # Lint
clojure -T:build jar                         # Build JAR
clojure -T:build install                     # Install to local .m2
clojure -T:build clean                       # Clean target/
```

ClojureScript tests require Node.js + Puppeteer (`npm install puppeteer`).

## Source Layout

```
src/main/clojure/clara/
  rules.cljc              # Public API: insert, retract, fire-rules, query, mk-session
  rules/
    engine.cljc           # Core Rete engine: beta network nodes, session impl, fire-rules
    compiler.clj          # Compiles defrule/defquery forms into Rete network (CLJ only)
    memory.cljc           # Working memory: IMemoryReader, ITransientMemory, IPersistentMemory
    dsl.clj               # DSL parsing for rule/query definitions
    accumulators.cljc     # Built-in accumulators (min, max, sum, count, etc.)
    listener.cljc         # Event listener protocol for tracing/debugging
    durability.clj        # Session serialization/deserialization (experimental)
    platform.cljc         # CLJ/CLJS platform abstractions
    schema.cljc           # Prismatic Schema definitions for internal structures
src/main/clojure/clara/
  tools/
    inspect.cljc          # Session inspection and debugging
    tracing.cljc          # Rule execution tracing
    fact_graph.cljc       # Fact dependency graph
src/main/java/clara/rules/  # Java interop interfaces (WorkingMemory, QueryResult, RuleLoader)
```

Tests: `src/test/clojure/` (CLJ), `src/test/clojurescript/` (CLJS), `src/test/common/` (shared .cljc)

## Architecture

### Rete Network

Rules compile into a two-phase network:

1. **Alpha network** — filters facts by type. Organized by `fact-type-fn` (defaults to Java class).
2. **Beta network** — joins across conditions. Node types in `engine.cljc`:
   - `RootJoinNode`, `HashJoinNode`, `ExpressionJoinNode` — fact joining
   - `NegationNode`, `NegationWithJoinFilterNode` — `:not` conditions
   - `AccumulateNode`, `AccumulateWithJoinFilterNode` — aggregation
   - `TestNode` — boolean test conditions
   - `ProductionNode` — rule RHS (right-hand side) execution
   - `QueryNode` — query result collection

### Session Model

Sessions are **immutable**. `insert`/`retract` return new sessions; `fire-rules` returns a session with rules executed. The compilation pipeline: DSL forms -> `compiler.clj` -> Rete node graph -> `LocalSession`.

### Truth Maintenance

- `insert!` (in rule RHS) — logical insertion; automatically retracted if the rule's conditions become false
- `insert-unconditional!` — persists regardless of rule truth
- Retractions cascade transitively through dependent logical insertions

### Key Protocols (engine.cljc)

- `ISession` — insert, retract, fire-rules, query
- `ILeftActivate` / `IRightActivate` — beta node activation
- `IMemoryReader` / `ITransientMemory` / `IPersistentMemory` — working memory access

### Cross-platform (.cljc)

Engine, memory, accumulators, and public API are `.cljc` files shared between CLJ and CLJS. The compiler (`compiler.clj`) and durability are CLJ-only. Platform-specific code uses reader conditionals (`#?(:clj ... :cljs ...)`). Macros for CLJS are in `clara.macros` (loaded via `:require-macros`).

### CLJS Compilation Note

The `defsession` macro in `.cljc` files evaluates rule/query vars on the Clojure side at compile time. The CLJS build script (`src/test/clojure/cljs_build.clj`) pre-loads CLJ test namespaces before CLJS compilation to ensure these vars resolve correctly.

## Conventions

- Variables in rule patterns are prefixed with `?` (e.g., `?temperature`, `?customer`)
- Schema validation via Prismatic Schema (`prismatic/schema`); tests use `schema.test/validate-schemas` fixture
- Test options fixture: `clara.tools.testing-utils/opts-fixture` wraps tests to run with multiple engine configurations
- Reflection warnings enabled in dev profile (`*warn-on-reflection* true`)
