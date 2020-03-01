# BPlusTreeMap

Thread safe concurrently Modifiable in memory B+ Tree

Modified from my original BTreeMap: https://github.com/huntj88/BTreeMap

Each node has its own ReadWriteLock and will release its ancestor nodes as soon as possible. 

During a write operation, if it has been determined that a node's structure will not change from the write operation, its safe to release the ancestor nodes write locks.

During a read operation its safe to release the nodes ancestors read locks as soon as a lock is acquired on a child node

[for more information about how the ReadWriteLock works](src/main/kotlin/me/jameshunt/bplustree/ReadWriteLock.kt)

### Usage
##### `fun put(key: Key, value: Value)`
---
##### `fun get(key: Key): Value?`
##### `fun getRange(start: Key, endInclusive: Key): List<Entry<Key, Value>>`
---
![generated code](img/usage.png)
