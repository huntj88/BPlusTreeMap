package me.jameshunt.bplustree

class BPlusTreeMap<Key : Comparable<Key>, Value> {

    companion object {
        var loggingImpl: Logging? = null
    }

    private val readWriteLock = ReadWriteLock()
    private var rootNode: Node<Key, Value> = LeafNode(LeafNeighborAccess(), LeafNeighborAccess())

    fun get(key: Key): Value? {
        readWriteLock.lockRead()
        return rootNode.get(key, releaseAncestor = { readWriteLock.unlockRead() })
    }

    fun getRange(start: Key, endInclusive: Key): List<Entry<Key, Value>> {
        readWriteLock.lockRead()
        return rootNode.getRange(start, endInclusive, releaseAncestor = { readWriteLock.unlockRead() })
    }

    fun put(key: Key, value: Value) {
        log("locking write at root")
        readWriteLock.lockWrite()
        log("LOCKED write at root")

        when (val rootNode = rootNode) {
            is LeafNode -> rootNode.lockLeafWrite()
            is InternalNode -> rootNode.rwLock.lockWrite()
            else -> TODO()
        }

        val releaseAncestors = { readWriteLock.unlockWrite().also { log("unlocked root") } }
        val putResponse = rootNode.put(Entry(key, value), releaseAncestors = releaseAncestors)
        if (putResponse is PutResponse.NodeFull<Key, Value>) {
            rootNode = InternalNode<Key, Value>().also {
                it.keys[0] = putResponse.promoted
                it.children[0] = putResponse.left
                it.children[1] = putResponse.right
            }
            readWriteLock.unlockWrite().also { log("unlocked root") }
        }
    }

    data class Entry<Key : Comparable<Key>, Value>(
        override val key: Key,
        override val value: Value
    ) : Map.Entry<Key, Value>, Comparable<Entry<Key, Value>> {
        override fun compareTo(other: Entry<Key, Value>): Int {
            return key.compareTo(other.key)
        }
    }
}

fun Any.log(message: String) {
    BPlusTreeMap.loggingImpl?.log(this, message)
}

interface Logging {
    fun log(caller: Any, message: String)
}
