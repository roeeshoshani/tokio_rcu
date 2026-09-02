use std::sync::Arc;

use tokio_rcu::{Rcu, rcu_block_on};

#[derive(Debug, Clone)]
struct SharedState {
    users: Vec<String>,
}
impl SharedState {
    fn contains_user(&self, username: &str) -> bool {
        self.users.iter().find(|x| *x == username).is_some()
    }
}

fn main() {
    rcu_block_on(async move {
        let state = Arc::new(Rcu::new(Box::new(SharedState { users: Vec::new() })));

        // spawn a bunch of readers which constantly read the data.
        let readers: Vec<_> = (0..16)
            .map(|_| {
                tokio::spawn({
                    let state = state.clone();
                    async move {
                        loop {
                            let contains_desired_user =
                                state.with(|state| state.contains_user("Alice"));
                            if contains_desired_user {
                                break;
                            }

                            // we must yield to let other tasks run.
                            // in reality we should have been doing some important blocking I/O stuff here, but for the sake of
                            // this example, just yielding is enough.
                            tokio::task::yield_now().await;
                        }
                    }
                })
            })
            .collect();

        // allocate a new state and add some new data into it.
        let mut cur_state = state.read_clone();
        cur_state.users.push("Bob".into());

        // point all readers to the new state, and get back the old state
        let mut old_state = state.swap(Box::new(cur_state)).await;

        // note that we can re-use the old state allocation.
        // but, note that it doesn't contain any changes performed to the new state.
        // to be more specific, in this case, the old state doesn't contain the new Bob user.
        old_state.users.push("Alice".into());

        // point all readers back to the old allocation now that we updated it.
        state.swap(old_state).await;

        for task in readers {
            task.await.unwrap();
        }
    });
}
