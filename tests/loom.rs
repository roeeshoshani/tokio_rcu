#![cfg(loom)]

#[test]
fn test_stuff() {
    loom::model(|| {})
}
