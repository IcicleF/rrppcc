rrppcc is an unsound RDMA RPC library that serves academic research purposes.

There are some performant and useful userspace RPC engines in C++ (e.g., [eRPC](https://github.com/erpc-io/eRPC)) with appealing features like zero-copy.
However, when some system researchers originally familiar with those RPC engines start to use Rust, they may find no comparable Rust alternatives.
Rust has memory safety, pervasive closures, and async/await.
C++ RPC engines does not have memory safety, often do not allow closures, and seldomly have support for C++20 coroutines.

This library offers native Rust userspace RPC that is partly inspired by eRPC.
Major features include:
- Fully userspace in the data plane
- Zero-copy
- Automatically use RDMA UD for small messages and RC for large messages

To use this library, you must have an available RDMA NIC installed on your computer.
Mellanox's ConnectX adaptor series are the best;
others should also work as long as you have `libibverbs` installed, but they are not tested.


## Unsoundness

rrppcc is **unsound**.
Undefined behavior may occur even if you only access safe interface.

Generally, you do not need to worry about the unsoundness, as normal use cases will not trigger UB.
However, you should read the *Unsoundness* sections in the documentation carefully.