# BPlusTreeMap

## Thread safe in memory B+ Tree

Modified from my original BTreeMap: https://github.com/huntj88/BTreeMap

The BTree locks the tiniest portion of the data structure that is needed for the write operation, while the rest of the tree is available to do other operations.

Each node has its own ReadWriteLock.

During a write operation, if it has been determined that the tree's structure will not change from the write operation, its safe to release the ancestor nodes write locks.

During a read operation its safe to release the parent node read lock as soon as a lock is acquired on a child node.

There can only be one holder of each nodes write lock, while the number of read lock holders per node is only limited by a variable (set to 1000)

[for more information about how the ReadWriteLock works](src/main/kotlin/me/jameshunt/bplustree/ReadWriteLock.kt)


### Usage
##### `fun put(key: Key, value: Value)`
---
##### `fun get(key: Key): Value?`
##### `fun getRange(start: Key, endInclusive: Key): List<Entry<Key, Value>>`
---
![generated code](img/usage.png)
