package me.jameshunt.bplustree

import me.jameshunt.bplustree.BPlusTreeMap.Entry

interface Node<Key : Comparable<Key>, Value> {
    val rwLock: ReadWriteLock

    fun get(key: Key, releaseAncestor: () -> Unit): Value?
    fun getRange(start: Key, endInclusive: Key, releaseAncestor: () -> Unit): List<Entry<Key, Value>>

    fun put(entry: Entry<Key, Value>, releaseAncestors: () -> Unit): PutResponse<Key, Value>
}

// work around "cannot use reified type" if i did "Array<Key?>"
data class Box<T : Comparable<T>>(val key: T) : Comparable<Box<T>> {
    override fun compareTo(other: Box<T>): Int {
        return key.compareTo(other.key)
    }
}

sealed class PutResponse<out Key, out Value> {
    object Success : PutResponse<Nothing, Nothing>()
    data class NodeFull<Key : Comparable<Key>, Value>(
        val promoted: Box<Key>,
        val left: Node<Key, Value>,
        val right: Node<Key, Value>
    ) : PutResponse<Key, Value>()
}

const val numEntriesPerNode = 8
