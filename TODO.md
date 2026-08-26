- currently, even if i set the epoch id to u8, i can't seem to overflow it. why? is there a bug in my code?

- provide a more raw interface in the `Rcu` wrapper, where for `new` and `swap` you provide a `Box<T>` instead of a `T`, and `swap` returns an undroppable guard which you must await and it then yields you a `Box<T>` representing the previous value. this will then allow you for example to implement double buffering using `Rcu` and using only 2 allocations that are continously being re-used.
  for implementing an undroppable type, here's what i found online:
  ```rust
  /// A type that cannot be dropped.
  pub struct Undroppable<T: ?Sized>(mem::ManuallyDrop<T>);
  
  impl<T> Undroppable<T> {
      // Makes `val` undroppable.
      //
      // If `val` has a  non-trivial destructor, attempting
      // to drop it will result in a compilation error.
      pub fn new_unchecked(val: T) -> Self {
          Self(mem::ManuallyDrop::new(val))
      }
  }
  
  impl<T:? Sized> Drop for Undroppable<T> {
      fn drop(&mut self) {
          const {
              assert!(!mem::needs_drop::<T>(), "This cannot be dropped.");
          }
      }
  }
  ```


- make MAX_CONCURRENT_THREADS more dynamic, for example calc it by using `num_cpu_cores()` or something like that, and allocate dynamic buffers instead of using fixed sized ones.
this will help prevent wasted space on machines with a low number of cpu cores, and will help the code be more flexibly and run even on machines with thousands of cores (even though i'm not sure if there really are any such cores)


- can we deadlock by holding an rcu reference and then swap that same pointer while still holding the reference? our thread will wait
for our thread to release the pointer. actually, this will probably not deadlock, just crash, since our thread will see itself going through quiescent states exactly due to sleeping. our thread will even wake itself up, which is a little weird, but ok.


