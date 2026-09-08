// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::tests::common;

/// Inference assumes supplied loop invariants while deriving its output. WP
/// must verify the written package before claiming success, or an invariant
/// which is false initially can make an invalid inferred contract look valid.
#[tokio::test]
async fn move_package_spec_infer_verifies_written_output() {
    let pkg = common::make_package("bad_invariant", &[(
        "bad_invariant",
        "module 0xCAFE::bad_invariant {
    fun count_to(n: u64): u64 {
        let i = 0;
        while (i < n) {
            i = i + 1;
        } spec {
            invariant [inferred] i == n;
        };
        i
    }
}",
    )]);
    let dir = pkg.path().to_str().unwrap();
    let client = common::make_client().await;
    let result = common::call_tool(
        &client,
        "move_package_wp",
        serde_json::json!({ "package_path": dir, "filter": "bad_invariant::count_to" }),
    )
    .await;
    let formatted = common::format_tool_result(&result);

    assert!(
        result.is_error == Some(true),
        "WP must reject inferred output which does not verify:\n{formatted}"
    );
    assert!(
        formatted.contains("post-inference verification failed"),
        "WP must identify its final verification step:\n{formatted}"
    );
    assert!(
        formatted.contains("base case of the loop invariant does not hold"),
        "WP must return the verifier diagnostic:\n{formatted}"
    );
}
