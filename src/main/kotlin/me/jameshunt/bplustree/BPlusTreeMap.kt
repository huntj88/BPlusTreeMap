package me.jameshunt.bplustree

class BPlusTreeMap<Key : Comparable<Key>, Value> {

    var rootNode: Node<Key, Value> = LeafNode()

    fun get(key: Key): Value? {
        return rootNode.get(key)
    }

    fun getRange(start: Key, endInclusive: Key): List<Entry<Key, Value>> {
        return rootNode.getRange(start, endInclusive)
    }

    fun put(key: Key, value: Value) {
        rootNode.writeLock.lock()
        val releaseRootNode = { rootNode.writeLock.release() }
        when (val putResponse = rootNode.put(Entry(key, value), releaseAncestors = releaseRootNode)) {
            is PutResponse.Success -> releaseRootNode()
            is PutResponse.NodeFull<Key, Value> -> {
                rootNode = InternalNode<Key, Value>().also {
                    it.keys[0] = putResponse.promoted
                    it.children[0] = putResponse.left
                    it.children[1] = putResponse.right
                }
            }
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
