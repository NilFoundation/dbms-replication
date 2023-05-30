# Replication Protocol SDK for [=nil; 'DROP DATABASE *](https://dbms.nil.foundation)
=nil; 'DROP DATABASE * is a scalable open-source multi-model database natively supporting graph, document and search. All supported data models & access patterns can be combined in queries allowing for maximal flexibility.

# Introduction

You may treat this document as an outline that can be filled with content gradually, and individual sections can be split into separate files as they grow larger.

# What is the replication SDK

The DBMS replication SDK allows you to use the DBMS as storage engine for a blockchain node.

Usually, when you write a node (for example, for the purpose of programmatically accessing events
or the state of the blockchain), you need to implement the validation logic for the events coming
from the specific blockchain, and apply them to the locally persisted state. In addition to the state,
you need to keep previous events persisted as well, in order to validate that the new events are correct.

Many node implementations will use their own choice of persistence in the form of a lightweight embedded
database for storing the events and state.

By doing so, the node implementation arguably create two problems:

- If they want to give programmatic access to state and events to external apps, they have to implement the complete
  data access API, which is a burden to do right (and it means reimplementing a good chunk of what regular databases
  do)
- Every node implementation would have a different API, and as such external apps interfacing with multiple different
  blockchain nodes would have to implement connectors to all their APIs

We've found that even though every blockchain is different and uses a varying set of primitives, the underlying
mechanisms of applying the events to state are the same. So, we provide a library that gives a set of abstractions
for the events and state that allow to unify them in the DBMS.


# DBMS write-ahead log and state

In order to understand what write-ahead logs are in traditional databases, let's consider a simple database with a single
table:

```
account_id, value
```

For this table, the database would have an on-disk representation, consisting of all rows and a (likely) a b-tree index
on the `account_id` column.

If you were to do an operation of adding an account (inserting a row), it can in theory be made atomic. For other, more
complex operations, like transferring money from one account to another, the operation can't easily be made atomic,
because it consists of two separate operations.

In order to execute such operations atomically, databases maintain a log of operations, and write transaction markers
to them. For example:

```
BEGIN TRANSACTION
UPDATE accounts SET value = value - 100 WHERE account_id = 1
UPDATE accounts SET value = value + 100 WHERE account_id = 2
END TRANSACTION
```

This operation log is written first to an append-only file, and only then it is applied to the state of the tables
in a separate file on disk. This is why this is called "write-ahead logs".

Usually, the structure of write-ahead logs is relatively straightforward: it is just a sequence of updates to the
table.

In addition to serving as a way to ensure transactions are atomic, the write-ahead logs are used for replicating
between databases in a leader-follower topology. They are a way to send incremental updates to the follower so
that its state stays up to date. It applies the write-ahead log to the state in exactly the same way that the leader does.

# Blockchain write-ahead log and state

**NOTE**: this needs clarification, as Ethereum events seem to correspond to smart contracts, and I'm not yet sure if the
regular transfers generate similar entries or not.

For whatever reasons, many blockchains don't use the term "write-ahead log" to describe a sequence of operations, and instead
use other terms (such as "events"). It doesn't matter too much, because events in the block are actually the same thing as
write-ahead log entries in the database.

Let's use Ethereum, and ERC20 standard for demonstration here. The block record usually contains `logs` entries.
One of such possible entries is the _Transfer_ event. It looks like this:

```
Transfer(from, to, value)
```

This transfer event, after passing required validation, will be applied to two wallets ("from" and "to"), and will
lead to the wallets changing value by "value". The new value of wallets will be their new state and will be used
to validate subsequent events.


# Translating log events

Now, it is hopefully clear that the way databases are replicated is not too different from the blockchains. In fact, you
can treat blockchain nodes as database replicas, that have stricter rules for validating entries of the write-ahead log
before applying them, than the regular databases do.

The way databases usually work for write requests:

```
Client --(query)--> Query Engine --> Write-Ahead Log --> State     (Leader)
                                          |
                                          |
                                          v
                                     Write-Ahead Log --> State     (Follower)
```


What if we could do this:


```
Blockchain --(events)-->   Node <--------------+          (Node "Leader")
                           |   ^               |
                           |   |               |
                           v   |               |
                         Write-Ahead Log --> State        (DBMS Follower)

```

In this scenario, the node implementation receives events from the blockchain, translates them
to the DBMS write-ahead log entries, and if needed, reads the previous entries or state from it.


# Key data structures

## Log entries

The primary data structure you'd be working with is called `prototype_log_entry`. This is a record that, when inserted
to the write-ahead log will result in changing the tables in a certain way. When writing a replication adapter, you'd be
translating the events from the specific blockchain to this structure. If you need to read the previous entries that you've
inserted to the write-ahead log, this is what you'd get back as well.

Here's the simplified definition of this structure:

```c++
        struct prototype_log_entry {
            struct insert_operation {
                std::unordered_map<std::string, std::string> map;
            };
            struct delete_operation {
                std::vector<std::string> keys;
            };
            struct compare_exchange_operation {
                std::string key;
                std::string oldValue;
                std::string newValue;
            };

            std::variant<delete_operation, insert_operation, compare_exchange_operation> op;
        }
```

There are currently 3 types of operations: `insert`, `delete` and `compare_exchange`.

In all 3 types of operations, it is mandatory to specify the key of the object. If you think about the state as being a regular table,
then a good example would be a table containing wallet data, where the key would be the wallet address.

The `insert` operation contains a map of (potentially multiple) objects to insert, as pairs of `key` and a velocypack-encoded object.
If an object with this key already exists in the state, it will be overwritten.

The `delete` operation contains a number of object keys for objects that should be deleted

And the `compare_exchange` object is a way to do an "optimistic" transaction. This operation will succeed only if the object that we are
trying to update is exactly equal to the `oldValue`.


## Replicated log

The key structure to interact with the write-ahead log is called `replicated_log_t`. This structure allows to call into the DBMS to insert the
write-ahead-log entries.

It is important to note here that DBMS has its own replication, so that state gets distributed across instances in multiple copies, for fault
tolerance reasons. And as such, the `replicated_log_t` is a write-ahead log that gets replicated.

This is the part of the interface you need to care about:

```c++
struct replicated_log_t {
    ...
    auto get_leader() const -> std::shared_ptr<log_leader>;
    auto get_follower() const -> std::shared_ptr<log_follower>;
    ...
};
```

Your replication adapter would interact with the leader, and insert log entries directly into it, and potentially read them back. So, just
call `get_leader()` if you have an instance of `replicated_log_t`.

The interface for interacting with the log_leader is:

```c++
struct ilog_leader : ilog_participant {
    virtual auto insert(log_payload payload, bool waitForSync) -> log_index = 0;

    struct DoNotTriggerAsyncReplication { };
    constexpr static auto doNotTriggerAsyncReplication = DoNotTriggerAsyncReplication {};
    virtual auto insert(log_payload payload, bool waitForSync, DoNotTriggerAsyncReplication) -> log_index = 0;
    virtual void triggerAsyncReplication() = 0;
    ...

    [[nodiscard]] auto getLogIterator(log_index firstIndex) const -> std::unique_ptr<LogIterator>;
};
```


The functions that you'd be interested in are:

- `insert(log_payload payload, bool waitForSync)`, which inserts a new write-ahead log entry, and returns an id of the inserted log entry
- `getLogIterator(log_index firstIndex)`, which allows you to find a write-ahead log entry by its id.

## Log payload
As describe above - we can use `insert(log_payload payload, bool waitForSync)` method to insert a new write-ahead log entry. But what is `log_payload`?
It's simple basic struct with buffer and some additional methods to work with it.

```c++
struct log_payload {
using BufferType = std::basic_string<std::uint8_t>;
    explicit log_payload(BufferType dummy);

    // Named constructors, have to make copies.
    [[nodiscard]] static auto create_from_slice(velocypack::Slice slice) -> log_payload;
    [[nodiscard]] static auto create_from_string(std::string_view string) -> log_payload;

    friend auto operator==(log_payload const &, log_payload const &) -> bool;

    [[nodiscard]] auto byteSize() const noexcept -> std::size_t;
    [[nodiscard]] auto slice() const noexcept -> velocypack::Slice;
    [[nodiscard]] auto copyBuffer() const -> velocypack::UInt8Buffer;
private:
    BufferType buffer;
};
```
So we have two options here: create from `std::string_view` or from `velocypack::Slice`.
The first one - only if you truly sure that you have a valid std::string for your log_entry, because in this string you also
need include such things as operation type, key, value and etc...
The second option is better, because you can use [velocypack](https://github.com/NilFoundation/dbms-velocypack) to serialize your data and
then use `create_from_slice` method to create log_payload from it.
Let's write full example of how to use it:

```c++
std::unordered_map<std::string, std::string> map {{"foo", "bar"}};
prototype_log_entry value_to_insert{prototype_log_entry::insert_operation {std::move(map)}};

entry_serializer<prototype_log_entry> serializer;
streams::serializer_tag_t<prototype::prototype_log_entry> tag;
serialization::Builder builder;
serializer.operator()(tag, value_to_insert, builder);

log_payload payload = log_payload::create_from_slice(builder.slice());
```
In this example we define simple insert operation with one key-value pair and set serializer for prototype_log_entry.
Then we create velocypack::Builder and serialize our value_to_insert into velocypack::Builder. After that we can use our `create_from_slice` method and get `log_payload` data for inserting in write-ahead-log.

It's necessary understand that before we can use velocypack to serialize our data, we need to define `inspect` functions - to show velocypack how we will serialize our data. For `prototye_log_entry`

```c++
template<class Inspector>
auto inspect(Inspector &f, prototype_log_entry::insert_operation &x) {
    return f.object(x).fields(f.field("map", x.map));
}
template<class Inspector>
auto inspect(Inspector &f, prototype_log_entry::delete_operation &x) {
    return f.object(x).fields(f.field("keys", x.keys));
}

template<class Inspector>
auto inspect(Inspector &f, prototype_log_entry::compare_exchange_operation &x) {
    return f.object(x).fields(f.field("key", x.key), f.field("oldValue", x.oldValue), f.field("newValue", x.newValue));
}

template<class Inspector>
auto inspect(Inspector &f, prototype_log_entry &x) {
      auto &b = f.builder();
      VPackObjectBuilder ob(&b);
      b.add(kType, VPackValue(x.getType()));
      b.add(VPackValue(kOp));
      return std::visit([&](auto &&op) { return f.apply(op); }, x.op);
    }
}
```

## State

The state is where the physical tables will be materialized. It is a result of sequentially applying write-ahead log entries.

```c++
    struct prototype_leader_state  {
        ...
        auto set(std::unordered_map<std::string, std::string> entries, prototype_state_methods::prototype_write_options)
            -> futures::Future<log_index>;

        auto compareExchange(std::string key, std::string oldValue, std::string newValue,
                             prototype_state_methods::prototype_write_options options)
            -> futures::Future<ResultT<log_index>>;

        auto remove(std::string key, prototype_state_methods::prototype_write_options) -> futures::Future<log_index>;
        auto remove(std::vector<std::string> keys, prototype_state_methods::prototype_write_options)
            -> futures::Future<log_index>;

        auto get(std::string key, log_index waitForApplied) -> futures::Future<ResultT<std::optional<std::string>>>;
        auto get(std::vector<std::string> keys, log_index waitForApplied)
            -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>>;
        ...
    }
```

As you can see, the state has high level functions for interacting with objects. When you're writing a replication adapter, most likely
you don't want to use the `set`, `remove` and `compareExchange` functions directly, since you'd be appending operations to the write-ahead
log instead. Interacting with write-ahead log gives you more control over the metadata than those high level primitives. The only method
you need from here is `get()`. It allows you to see the state that results from applying the write-ahead log entries.



# Practical example

To make writing integrations simpler, the replication SDK comes with a mock implementation of a state, that doesn't require full DBMS
machinery. It works by keeping all data in memory, in regular std::map. From integration perspective, it works the same way -- e.g.
it has a write-ahead log for writing entries, and can materialize them to the state. You can use this mock implementation to write
you replication adapter, and then just swap the mock with DBMS implementation when integrating with it.

```c++
// Creating a write-ahead log entry for the insert operation
std::unordered_map<std::string, std::string> map {{"foo", "bar"}};
prototype_log_entry value_to_insert{prototype_log_entry::insert_operation {std::move(map)}};

entry_serializer<prototype_log_entry> serializer;
streams::serializer_tag_t<prototype::prototype_log_entry> tag;
serialization::Builder builder;

// Serializing the write-ahead log entry to a binary format
serializer.operator()(tag, value_to_insert, builder);

log_payload payload = log_payload::create_from_slice(builder.slice());


// Creating replicated log object and setting up the state representation
auto leaderLog = makeReplicatedLog(logId);

auto leaderReplicatedState = std::dynamic_pointer_cast<replicated_state_t<prototype_state>>(
        feature->create_replicated_state("prototype-state", leaderLog));
leaderReplicatedState->start(std::make_unique<replicated_state_token>(state_generation {1}));

auto leaderState = leaderReplicatedState->getLeader();

// Inserting write-ahead log entry to the write-ahead log
auto index = leaderLog.insert(payload, false)

// Verifying that the inserted write-ahead log entry leads to changing state
auto result = leaderState->get("foo", log_index {index}).get();

// Reading back the write-ahead log entry from the write-ahead log
auto getLogIterator = leaderLog.getLogIterator(log_index {index});
assert(getLogIterator.logPayload(), payload);
```

# Writing a replication protocol adapter

As said above, when writing a replication protocol adapter for a blockchain, you would use our primitives
for representing events from the blockchain.
When receiving a new event, you need to verify it according to
the rules of the specific blockchain, and append it to the `leaderLog`.
Doing this allows you to not use any intermediate state and log representations.
This removes the need to reconcile the state, and makes the implementation fast.

In practice, the adapter you write will be loaded directly into the DBMS storage node as a shared library.
As a result, there will be minimal overhead when working with the data structures.
In fact, you will get almost direct access to the underlying LSM-tree-based state and log representations.

If you base your replication adapter on the existing node implementation, you will need to change the
place in the code where you persist the events and materialize the state.
It can be done by directly modifying the code of your node implementation.
The operations with the write-ahead log and the state are thread-safe,
so you can use your existing event loop and threading,
and only replace the parts that interact with the persistence layer.

# Architecture of the cluster, sharding, and fault tolerance

As soon as you work with the right abstractions, the rest should just work.
As a result, this has less influence on the architecture of the replication protocol adapter. However, for the sake of understanding, it
is still worth documenting here.
