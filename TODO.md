- in synchronize_rcu, don't make the current thread check itself. this is annoying since it means that a thread must always sleep at least once before completing the synchronize_rcu call.

- tests for reading and writing the rcu pointer outside of the tokio runtime. this should somehow be disallowed to prevent unsafety.

- modify rcu guard so that you really can't hold it across await points. !Send and !Sync is not enough.

- make MAX_CONCURRENT_THREADS more dynamic, for example calc it by using `num_cpu_cores()` or something like that, and allocate dynamic buffers instead of using fixed sized ones.
this will help prevent wasted space on machines with a low number of cpu cores, and will help the code be more flexibly and run even on machines with thousands of cores (even though i'm not sure if there really are any such cores)


- can we deadlock by holding an rcu reference and then swap that same pointer while still holding the reference? our thread will wait
for our thread to release the pointer. actually, this will probably not deadlock, just crash, since our thread will see itself going through quiescent states exactly due to sleeping. our thread will even wake itself up, which is a little weird, but ok.


