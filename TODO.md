- does limiting the rcu_block_on future with !Sync really necessary? tokio does !Send only in tokio::spawn.

- tests for reading and writing the rcu pointer outside of the tokio runtime. this should somehow be disallowed to prevent unsafety.

- modify rcu guard so that you really can't hold it across await points. !Send and !Sync is not enough.
idea: have a thread local variable indicating the number of live rcu read guards. in the tokio worker runtime, if this value is non-zero, panic or avoid performing the quiescent state logic. maybe do it in debug only.

- make MAX_CONCURRENT_THREADS more dynamic, for example calc it by using `num_cpu_cores()` or something like that, and allocate dynamic buffers instead of using fixed sized ones.
this will help prevent wasted space on machines with a low number of cpu cores, and will help the code be more flexibly and run even on machines with thousands of cores (even though i'm not sure if there really are any such cores)


