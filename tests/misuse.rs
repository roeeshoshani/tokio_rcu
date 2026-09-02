use tokio_rcu::Rcu;

#[test]
#[should_panic = "attempted to read an rcu protected pointer outside of an rcu-enabled tokio runtime"]
fn read_outside_of_runtime() {
    let x = Rcu::new(Box::new(String::from("some interesting piece of text")));
    let _ = unsafe { x.read() };
}

#[tokio::test]
#[should_panic = "attempted to read an rcu protected pointer outside of an rcu-enabled tokio runtime"]
async fn read_in_non_rcu_runtime() {
    let x = Rcu::new(Box::new(String::from("some interesting piece of text")));
    let _ = unsafe { x.read() };
}
