# Splinter Development Guide

## Development Tools and Commands

When working with Rust code in this project:

- **Run tests**: `cargo nextest run --benches --all-targets` (preferred over `cargo test`)
- **Run doctests**: `cargo test --doc` (nextest doesn't support doctests yet)
- **Run specific test**: `cargo nextest run <test_name>`
- **Run lints**: `cargo clippy`
- **Format code**: `cargo fmt`

## Coding Conventions

Splinter is low-level systems software. Prioritize safety, performance, and clarity above all. Follow these principles based on [TigerStyle]:

[TigerStyle]: https://tigerstyle.dev/

### Safety

- **Control flow**: Use simple, explicit control structures. Avoid recursion. Keep functions under 70 lines. Centralize branching logic in parent functions.
- **Memory & types**: Use fixed-size types (e.g. u32, i64). Prefer to allocate memory at startup or make use of the stack. Avoid dynamically checked borrow rules (e.g. `RefCell`) and `dyn` usage.
- **Error handling**: Use assertions for invariants and argument checks. Treat warnings as errors.

### Performance

- **Early design**: Apply napkin math to estimate bottlenecks. Design for performance from the start.
- **Batching**: Batch I/O or expensive operations. Prioritize optimizing network > disk > memory > CPU.
- **Predictability**: Write predictable, branch-friendly code. Don't rely on compiler optimizations.

### Clarity

- **Naming**: Use clear variable names. Avoid abbreviations and single-letter variable names. Use specific types like ByteUnit and Duration rather than bare types for variables that have logical units.
- **Structure**: Keep functions simple. Group related code. Declare variables near usage.
- **Consistency**: Avoid aliases/dupes. Pass large values by reference. Maintain consistent indentation, comment style, and toolchain. Write idiomatic Rust code.
- **Off-by-One safety**: Treat indexes, counts, sizes as distinct. Be explicit in rounding and division.

### Documentation

- Document public APIs with Rustdoc comments.
- Keep the README and examples updated when behavior or usage changes.

## Development and Testing Instructions

- **Always use `cargo nextest run --benches --all-targets`** for running tests (not `cargo test`)
- To focus on one test, use `cargo nextest run <test_name>`
- Fix any test or type errors until the whole suite is green
- Add docs and doctests to any public functions on `Splinter` or `SplinterRef`, then verify them using `cargo test --doc`

## Additional Notes for AI Assistants

- This codebase prioritizes systems-level performance and safety
- When making changes, always run the full test suite with nextest
- Maintain the existing code style and architectural patterns
- Focus on explicit, readable code over clever optimizations
- Always validate changes with both unit tests and doctests
