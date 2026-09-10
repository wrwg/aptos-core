# Test organization and check ledger

Updated 2026-09-10 (denotation route, [`denotation.md`](denotation.md)). This is the ledger of the acceptance
fixtures under
[`leaner-e2e-tests/LeanerE2ETests/Check/`](../leaner-e2e-tests/LeanerE2ETests/Check/):
which pass exactly, which do not, and why. Chronology and the mappings of
the original v0 cases live in
[the history archive](test-organization-history.md), not here.

## Current result

**46 of 61 Check files pass exactly; 15 fail.** Every failure is a
construct the denotation does not yet carry, a fixture that still asserts
names of a retired route, or a fixture whose re-import round trip fails
outside verification; none is a diagnostics mismatch. A file passes
when its whole output matches the adjacent `.exp` at the driver's caps
(180k verification heartbeats per target); a file with one failing target
fails, however many targets it proves.

| Gate | Result | Note |
|---|---|---|
| `leaner-ir` build | PASS | `LeanerLang` and the `Denote` modules. |
| `DenotePerformance` gate | PASS (baseline regenerated 2026-09-10) | Loops 10–12% cheaper, scalars 8–22% dearer than the checkpoint; storage targets added (`record` 109M). |
| `leaner-ir` `lake test` | FAIL (38 of 215 roots, checkpoint result; not rerun) | 35 `LeanerLang.Tests.Native*` roots and `Performance` assert artifacts of the retired routes (their removal is D4); `Frontend` needs vectors (`replace`); `CompositionPerformance` is a resource-composition residual. Every `LeanerIR.Tests.*` root passes. |
| `leaner-move`, `leaner-rust` builds | PASS | |
| `leaner-e2e-tests` `lake build` | FAIL (unrelated) | The `mono-move-lean-link` Rust crate does not compile (E0061); the ledger was taken per file with `lake env lean` at the driver's caps. |

The remaining failure classes, by size:

| Class | Files | What is missing |
|---|---|---|
| Generics | 4 | Generic locals, calls, constructors, and fields are not carried. |
| Re-import round trips | 3 | `ControlForms`, `VectorBounds`, `Negative/Lowering` verify but their printed source does not re-import (`LEANER-BORROW-TYPE`), a printer issue outside verification; `VectorBounds` also has a `move` of a place at an unexpected type. |
| Recursion | 2 | A callee is verified before its callers; a recursive function needs the fixpoint characterization of its big-step meaning. |
| Returned and free-standing references | 2 | A mutable borrow outside a binding or call argument, and returned references (`References`, `Negative/ReturnedMutRefs`). |
| Retired-route assertions | 2 | `Verification/Typed` and `Verification/EnumRefs` assert artifacts of the retired route. |
| Fixture-local cap | 1 | `GlobalInv` verifies at the driver's cap but sets its own 50k cap, which `record` (two global borrows under invariants) exceeds. |
| Rust profile | 1 | The Rust profile's primitives have no denotation yet. |

## Per-file status

Each name is a `.lean` file under `Check/`. PASS means the entire fixture
matches its baseline; a PASS with no `verify` target is execution or
diagnostics coverage only, as noted.

### Language (19 files: 17 pass, 2 fail)

| File | Status | Remaining problem |
|---|---|---|
| `Language/Abilities` | PASS | No verification targets. |
| `Language/Addresses` | PASS | |
| `Language/Arithmetic` | PASS | |
| `Language/Attributes` | PASS | No verification targets. |
| `Language/ControlForms` | FAIL | Every target verifies; the printed source does not re-import. |
| `Language/EmptyModule` | PASS | No verification targets. |
| `Language/EnumPatterns` | PASS | Nested enums verify at seven goals' cost. |
| `Language/EnumPayloads` | PASS | |
| `Language/EnumRefs` | PASS | Execution only, no `verify`. |
| `Language/Enums` | PASS | |
| `Language/Generics` | FAIL | Every target has a generic local. |
| `Language/Integers` | PASS | |
| `Language/Literals` | PASS | |
| `Language/Loops` | PASS | |
| `Language/PositionalStructs` | PASS | |
| `Language/Signed` | PASS | |
| `Language/Tuples` | PASS | |
| `Language/VectorOperations` | PASS | |
| `Language/Vectors` | PASS | |

### Verification (30 files: 20 pass, 10 fail)

| File | Status | Remaining problem |
|---|---|---|
| `Verification/Aborts` | PASS | Includes the intended false-contract rejection. |
| `Verification/Account` | PASS | |
| `Verification/BorrowCertificates` | PASS | Certificate assertions, no `verify`. |
| `Verification/Callees` | FAIL | `drain` and `call_drain` are recursive; unspecified pure callees are inlined and pass. |
| `Verification/Calls` | FAIL | `recursive_choose` is recursive. |
| `Verification/Composition` | PASS | |
| `Verification/CorePrimitives` | PASS | |
| `Verification/Corpus` | PASS | |
| `Verification/CrossInv` | PASS | |
| `Verification/EnumRefs` | FAIL | The fixture builds enum twins with anonymous constructors of the retired route. |
| `Verification/Generics` | FAIL | Generic locals and calls. |
| `Verification/GenericScalarCalls` | FAIL | Generic calls. |
| `Verification/GenericStorage` | FAIL | Generic fields have no carrier; generic calls. |
| `Verification/GlobalBorrows` | PASS | |
| `Verification/GlobalInv` | FAIL | Verifies at the driver's cap; the fixture's own 50k cap is below `record`'s cost. |
| `Verification/Increment` | PASS | |
| `Verification/Invariants` | PASS | |
| `Verification/Loans` | PASS | |
| `Verification/LoopInvariants` | PASS | |
| `Verification/Loops` | PASS | |
| `Verification/LooseFrame` | PASS | Its hand proof no longer unfolds the raw contract by hand. |
| `Verification/Normalized` | PASS | |
| `Verification/Prophecies` | PASS | |
| `Verification/Read` | PASS | |
| `Verification/References` | FAIL | `reborrow` borrows outside a binding or call argument; returned references. |
| `Verification/ResourceComposition` | PASS | |
| `Verification/Rust` | FAIL | The Rust profile's `add` has no denotation. |
| `Verification/SpecLogicalArithmetic` | PASS | |
| `Verification/Storage` | PASS | |
| `Verification/Typed` | FAIL | Asserts `typedDenotation`/`Arguments` names of the retired route. |

### Negative and support (12 files: 9 pass, 3 fail)

A correct rejection does not make a file pass if its positive control fails.

| File | Status | Remaining problem |
|---|---|---|
| `Negative/BorrowGlobals` | PASS | |
| `Negative/Borrows` | PASS | |
| `Negative/IntrinsicUnsupported` | PASS | |
| `Negative/LoopInvariants` | PASS | Baseline names the unestablished invariant at entry and at an iteration. |
| `Negative/Lowering` | FAIL | Every target verifies; the printed source does not re-import. |
| `Negative/ReturnedMutRefs` | FAIL | The parameter-derived returned reference leaves a residual obligation. |
| `Negative/Specifications` | PASS | |
| `Negative/Surface` | PASS | |
| `Negative/Verification` | PASS | |
| `Negative/WrongIncrement` | PASS | |
| `PreparationRetry` | PASS | |
| `VectorBounds` | FAIL | `rhs_abort_before_bounds` moves a place at an unexpected type; the printed source does not re-import. |

### Missing v0 fixtures

| v0 file | Remaining work |
|---|---|
| `Verification/OrderedMap.lean` | Nine proof-carrying targets and their dependencies. |
| `Verification/Quicksort.lean` | Three proof-carrying targets and their dependencies. |
| `Verification/ReturnedMutRefs.lean` | The 66-target corpus; the hand `References` fixture does not replace it. |
| `Verification/SpecFunctions.lean` | Twelve targets. |
| `Verification/Summaries.lean` | Three targets. |
| `Negative/SpecFunctions.lean` | Diagnostic cases. |
| `Language/BorrowChecker.lean` | Borrow-checker cases. |

## Conventions and rules

A Check file is LeanerLang source with contracts, `verify` commands, and
execution or diagnostic assertions. The driver discovers every
`Check/**/*.lean`, runs each in its own Lean process at the driver's caps,
and compares all diagnostics to the adjacent `.exp` (absence of `.exp`
means empty output). Run it from `leaner-e2e-tests` with
`LEANER_E2E_SUITE=check lake test`; `UB=1` regenerates baselines and every
regenerated diff is reviewed.

- A baseline records intended behavior. A negative case's expected
  diagnostic names the construct or clause; an unsupported positive proof
  is never recorded as an expected failure.
- A file is promoted only by the driver at the unchanged caps; a passing
  pilot promotes nothing. Do not raise caps to count a port as done.
- Successful verifies pass the automatic native audit; use
  `#leaner_require_native` for partial ports and
  `#leaner_require_native_all` for completed fixtures.
- Assertion-style IR, Move, and Rust tests stay in their owning packages.
  The deprecated packages are reference material and are not run.
- Source verification is not a compiler-correctness theorem for emitted
  bytecode.

After each batch, update the date, the counts, the affected rows, and the
gate table in place. Record commits separately; a commit does not change a
status.
