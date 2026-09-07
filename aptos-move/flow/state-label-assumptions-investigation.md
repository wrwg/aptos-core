# Unresolved state-label verification issue

Discovered while strengthening the regression for the gas-schedule WP cycle
fix. This is a verification-instrumentation bug, not a reason to run the prover
inside WP. The guidance describes the intended WP correctness contract; this
finding means the implementation still has a correctness issue to repair.

## Reproducer

In `third_party/move/move-prover/tests/inference/guarded_state_labels.move`, add:

```move
// inference-reject-mutation: *borrow_global_mut<Config>(addr) = config; => *borrow_global_mut<Config>(addr) = Config { value: 0 };
```

Run with the real Boogie/Z3 executables:

```sh
cargo test -p move-prover --profile ci --test inference_testsuite guarded_state_labels
```

The original verifies, but the mutation also verifies, so the mutation check
fails. The current checked-in-style fixture only checks source emission and
ordinary verification; it does **not** establish mutation rejection.

## Cause

`emit_state_label_assumes` assumes a whole postcondition whenever it contains
an intermediate-state definition. `non_defining_residual` then skips the whole
condition if any label-defining behavioral/spec-function call occurs within it.
A clause containing `result_of<extract>` and the final `update<Config>` thus
assumes the final write instead of proving it. The same issue can mask errors
in other mixed clauses. Callee instrumentation also removes entire mixed
postconditions after emitting only their defining fragment.

## Experimental fix — not applied

`state-label-assumptions-investigation.patch` records an unfinished experiment:
extract guarded, scoped definitions; convert intermediate `result_of` to
full-arity `ensures_of`; assert residual final-state properties; keep full
callee postconditions on successful calls.

It rejects the mutation while proving the original. It also preserves the
tuple/`&mut` sourcifier regression after correctly constructing result slots.
However, broader tests are not green: `calculator` and `chained_opaque` exposed
behavioral-predicate issues, and four functional state-label fixtures produced
new failures (three postcondition failures and one timeout). Some fixtures
contain invalid claims previously assumed, but these results need individual
review; they must not be blindly accepted as new baselines. A final attempt to
anchor memory-free opaque calls is included in the patch; `chained_opaque`
still failed with it.

The experiment was removed from working Rust code. Do not apply or ship this
patch as a validated fix. The targeted WP cycle/ghost-memory fixes do not resolve
this separate verification bug.
