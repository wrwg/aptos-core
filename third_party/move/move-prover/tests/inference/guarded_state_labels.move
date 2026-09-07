// Covers source emission and verification, not mutation strength. See
// aptos-move/flow/state-label-assumptions-investigation.md for a known surviving
// mutation caused by the existing state-label verification instrumentation.
module 0x42::guarded_state_labels {
    use std::signer;
    struct Pending has key { value: u64 }
    struct Config has key, drop { value: u64 }

    fun pending(addr: address): bool { exists<Pending>(addr) }
    spec pending {
        pragma opaque;
        aborts_if false;
        ensures result == exists<Pending>(addr);
    }

    fun extract(addr: address): Config acquires Pending {
        let Pending { value } = move_from<Pending>(addr);
        Config { value }
    }
    spec extract {
        pragma inference = none;
        aborts_if !exists<Pending>(addr);
        modifies Pending[addr];
        ensures result.value == old(global<Pending>(addr)).value;
        ensures !exists<Pending>(addr);
    }

    // The guard defines the read call's state and consumes extract's later
    // state. This is a forward chain, not a cyclic state definition.
    fun install(account: &signer) acquires Pending, Config {
        let addr = signer::address_of(account);
        if (pending(addr)) {
            let config = extract(addr);
            if (exists<Config>(addr)) {
                *borrow_global_mut<Config>(addr) = config;
            } else {
                move_to(account, config);
            }
        }
    }
}
