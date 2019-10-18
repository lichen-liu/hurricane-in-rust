# Hurricane In Rust


## 1.0 Project Structure

```
hurricane-in-rust
│   README.md
│   ...
│
└─── Benchmarks
│   └─── click_log                        # Rust project for click log
│   
└─── hurricane                            # Main project folder
│   └─── src                              # Source
│       └─── app                          # Hurricane application master
│       └─── backend                      # Hurricane backend node
│       └─── common                       # Data serializer, bag, chunk ...
│       └─── communication                # Hurricane Network layer
│       └─── frontend                     # Hurricane frontend node
│       └─── util                         # Utilities
│       └─── lib.rs                       # Hurricane libraries
│       └─── macros.rs                    # Defined macros
│       └─── main.rs                      # Application entry point
│       └─── prelude.rs                   # Namespace header
│   └─── target                           # Output binary
│   └─── tests                            # Test configurations and data source
│       └─── data                         # Data source
│       └─── ...

```


## 2.0 Programming Style


### 2.1 Guideline

```
https://doc.rust-lang.org/1.9.0/style/
```


### 2.2 Special Notes

1. Syntax ```extern crate x;``` is no longer required when using external crates.


## 3.0 Commands


### 3.1 Utility

```
# Print number of lines of Rust code (Linux)
cd hurricane
wc -l src/*/*.rs
```
```
# Print number of lines of Rust code (Powershell)
cd hurricane
(type src/*/*.rs | Measure-Object -line).Lines
```

```
# Generate Hurricane project docs
# To include all information (public & private)
cargo rustdoc -- --document-private-items
# Then, the doc is under your target folder with html format
```


### 3.3 Cargo Bench
Rust has internal benchmark facility support, but it is an unstable feature, and can only be
run under the Nightly Rust.
```
http://seenaburns.com/benchmarking-rust-with-cargo-bench/
```

To install Nightly Rust
```
rustup install nightly
```

To Run Benchmark
1. Uncomment `#![feature(test)]` in `lib.rs`.
2. Uncomment the benchmark mod.
3. Run `rustup run nightly cargo bench`.

## 4.0 Notes


## 5.0 Design Specifications


### 5.1 Actor System Design

Use Actix (https://github.com/actix/actix) as the actor system for the project.

**Good examples:**

- https://simplabs.com/blog/2018/06/11/actix.html

- https://simplabs.com/blog/2018/06/27/actix-tcp-client.html


#### 5.1.1 Notes

I think actix is not as clear as akka, mainly because we cannot wrap all methods and data to the struct like Scala.
In Rust, we may have to put struct definitions and implementations in the same file.

To implement each actor, we need to complete 3 parts:

1. Actor definition
- In this part, we need to define the struct, implement the "constructor" and methods of the struct
- Then, we need to declare the struct as Actor with "impl Actor for your_struct"

2. Message definition
- Messages are specific to the system. We need to declare those message here.
- Basically, this is required by actix and you can use #[derive(Message)] for a shortcut

3. Message Handler
- In this part, we need to implement how we want to handle the messages in part 2.
- Some additional handlers are also added here, such as Error Handler or Stream Handler

See the Experiments/chatter as an example


#### 5.1.2 Actor Summary

List and summarize all the actors in the Rust implementation:  
- **Lifetime**: When will an actor be terminated? naturally, or unnaturally (TODO: respawn using supervisor).
- **Note 1**: Actors are more or less on the same level of hierarchy. By passing message, they are passing the "control flow" to the other. 
- **Note 2**: Relationship of Actors are more like a graph (interconnected) instead of a tree (hierarchical), though some are more "social".
- **Note 3**: Messages between TCP layer Actors are different than messages between local Actors.

| Name                  | Purpose                                                                     | Lifetime                    |
|-----------------------|-----------------------------------------------------------------------------|-----------------------------|
| HurricaneFrontend     | Computation side hosting data computation flow services. Driver.            | Application                 |
| AppMasterCore         | App Master actual logic                                                     | Node                        |
| AppMaster             | App Master TCP layer                                                        | TCP connection              | 
| TaskManagerCore       | Task Manager actual logic                                                   | Node                        |
| TaskManager           | Task Manager TCP layer with App Master                                      | TCP connection              |
| HurricaneWorkIO       | Hurricane Work TCP layer with Backend (Worker <=> IO)                       | TCP connection              |
| WorkExecutor          | Executor for a particular task (master worker)                              | Task                        |
| HurricaneGraphWork    | 1-thread Executor for single data pipeline of a task (slave worker)         | Data Pipeline               |
|                       |                                                                                                           |
| HurricaneBackend      | IO side hosting data bag storage services. Driver.                          | Application                 |
| HurricaneIO           | Hurricane Server that performs the actual disk IO.                          | TBD                         |


#### 5.1.3 Actor Spawning on Frontend

```
/// Note: * - Network Layer Actor
/// Note: HurricaneFrontend needs to actively check whether AppMaster and TaskManager are alive,
///       if not, HurricaneFrontend terminates the hosted compute (or frontend) node.
main -> HurricaneFrontend -> *AppMaster (Only on one node) -> AppMasterCore
                         `-> *TaskManager -> TaskManagerCore
                                              `-> WorkExecutor (Alive for one task) -> HurricaneGraphWork (Alive for one datapipe)
                                              `-> *HurricaneWorkIO
```
