-- Copyright © Aptos Foundation
-- SPDX-License-Identifier: Apache-2.0

import LeanerIR.Proofs.Denote.Compile

/-!
# Agreement of the denotation with the big-step semantics

The one theorem of `designs/denotation.md`: for every function the compiler
accepts, the denotation of its term is exactly the typed view of its
big-step meaning.  It is stated over the compiler, so it holds for every
unit at once; a verified function transports its native theorem through it
to the authoritative semantics without any proof of its own.

**Status: assumed.**  By the user's decision of 2026-09-08 the induction
over the compiler's fuel is postponed and the statement is an explicit,
named axiom until then, so that `#print axioms` names it on every theorem
that depends on it.  Nothing else in the denotation is admitted.
-/

namespace LeanerIR.Proofs.Denote

open LeanerIR.Validation

/-- The typed view of a function's big-step meaning at the compiler's
argument and result codecs. -/
def typedMeaning (unit : ExecutableUnit) (handle : FunctionHandle) (f : Function)
    (args : HList f.params) : Comp f.result.carrier :=
  typedFunction (hlistCodec f.params) (resultCodec f.result) (functionSpec unit handle) args

/-- Exact agreement of a compiled function's denotation with the typed
view of its big-step meaning.  To be proved by induction on the fuel of
`compileExpr`, one case per term constructor, reusing the per-construct
agreement lemmas of `Proofs.Denotation` as the inductive cases. -/
axiom compileFunction_agrees (unit : ExecutableUnit) (handle : FunctionHandle)
    (f : Function) (compiled : compileFunction unit.unit handle = .ok f)
    (args : HList f.params) :
    Spec.Equiv (f.denote unit args) (typedMeaning unit handle f args)

/-- Equivalent computations have the same weakest precondition. -/
theorem wp_congr_equiv {σ ε α : Type} {a b : Spec σ ε α} (equiv : Spec.Equiv a b)
    (ensures : α → σ → Prop) (aborts : ε → Prop) (initial : σ) :
    wp a ensures aborts initial ↔ wp b ensures aborts initial := by
  simp only [wp]
  rw [(equiv.undefined initial)]
  constructor
  · rintro ⟨normal, failing, defined⟩
    exact ⟨fun r f step => normal r f ((equiv.ok _ _ _).mpr step),
      fun e step => failing e ((equiv.aborts _ _).mpr step), defined⟩
  · rintro ⟨normal, failing, defined⟩
    exact ⟨fun r f step => normal r f ((equiv.ok _ _ _).mp step),
      fun e step => failing e ((equiv.aborts _ _).mp step), defined⟩

/-- A call to a compiled callee reasons over the callee's denotation: an
unspecified callee is inlined by the agreement theorem, never summarized. -/
theorem wp_calleeMeaning_of_compiled (unit : ExecutableUnit) (handle : FunctionHandle)
    (f : Function) (compiled : compileFunction unit.unit handle = .ok f) (values : HList f.params)
    (ensures : f.result.carrier → RuntimeState → Prop) (aborts : Failure → Prop)
    (initial : RuntimeState) :
    wp (calleeMeaning unit handle f.params f.result values) ensures aborts initial ↔
      wp (f.denote unit values) ensures aborts initial :=
  (wp_congr_equiv (compileFunction_agrees unit handle f compiled values) ensures aborts initial).symm

/-- A contract established over the denotation holds of the typed
big-step meaning. -/
theorem satisfies_typedMeaning (unit : ExecutableUnit) (handle : FunctionHandle)
    (f : Function) (compiled : compileFunction unit.unit handle = .ok f)
    (contract : Contract RuntimeState Failure (HList f.params) f.result.carrier)
    (verified : Satisfies (f.denote unit) contract) :
    Satisfies (typedMeaning unit handle f) contract :=
  (satisfies_congr (compileFunction_agrees unit handle f compiled) contract).mp verified

/-- A contract established over the denotation holds of the function's
big-step meaning, at the runtime-row form of the contract.  This is the
public theorem a `verify` publishes. -/
theorem satisfiesFunction_of_denote (unit : ExecutableUnit) (handle : FunctionHandle)
    (f : Function) (compiled : compileFunction unit.unit handle = .ok f)
    (contract : Contract RuntimeState Failure (HList f.params) f.result.carrier)
    (verified : Satisfies (f.denote unit) contract) :
    SatisfiesFunction unit handle
      (contract.runtime (hlistCodec f.params) (resultCodec f.result)) :=
  satisfies_runtime (hlistCodec f.params) (resultCodec f.result) (functionSpec unit handle)
    contract (satisfies_typedMeaning unit handle f compiled contract verified)

end LeanerIR.Proofs.Denote
