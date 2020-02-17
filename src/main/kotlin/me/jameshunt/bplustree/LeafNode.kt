package me.jameshunt.bplustree

import me.jameshunt.bplustree.Tree.*

class LeafNode<Key : Comparable<Key>, Value> : Node<Key, Value> {
    private val entries: Array<Entry<Key, Value>?> = Array(numEntriesPerNode) { null }
    private var leftLink: LeafNode<Key, Value>? = null
    private var rightLink: LeafNode<Key, Value>? = null

    override fun get(key: Key): Value? {
        // TODO: could be binary search, already sorted
        entries.forEach {
            if (key == it?.key) {
                return it.value
            }
        }
        return null
    }

    override fun getRange(start: Key, endInclusive: Key): List<Entry<Key, Value>> {
        return mutableListOf<Entry<Key,Value>>().apply {
            getRangeAscending(collector = this, start = start, endInclusive = endInclusive)
        }
    }

    private fun getRangeAscending(collector: MutableList<Entry<Key, Value>>, start: Key, endInclusive: Key) {
        entries.forEach { entry ->
            entry?.let {
                when(entry.key in start..endInclusive) {
                    true -> collector.add(entry)
                    false -> if(collector.isEmpty()) Unit else return
                }
            }
        }

        rightLink?.getRangeAscending(collector, start, endInclusive)
    }

    override fun put(entry: Entry<Key, Value>): PutResponse<Key, Value> {
        return when (entries.last() == null) {
            true -> putInNodeWithEmptySpace(entry)
            false -> splitLeaf(entry)
        }
    }

    private fun splitLeaf(newEntry: Entry<Key, Value>): PutResponse.NodeFull<Key, Value> {
        entries.forEach { assert(it != null) }
        // TODO: optimize sort out
        val sorted = (entries + arrayOf(newEntry)).apply { sort() }

        val left = LeafNode<Key, Value>().also { node ->
            (0 until numEntriesPerNode / 2).forEach {
                node.entries[it] = sorted[it]
            }

            node.leftLink = leftLink
        }

        val right = LeafNode<Key, Value>().also { node ->
            (numEntriesPerNode / 2 until sorted.size).forEachIndexed { newNodeIndex, i ->
                node.entries[newNodeIndex] = sorted[i]
            }

            node.leftLink = left
            node.rightLink = rightLink
        }

        left.rightLink = right

        // reconnect existing nodes
        leftLink?.rightLink = left
        rightLink?.leftLink = right

        return PutResponse.NodeFull(Box(right.entries.first()!!.key), left, right)
    }

    private fun putInNodeWithEmptySpace(newEntry: Entry<Key, Value>): PutResponse.Success {
        fun getIndexOfSpotForPut(newEntry: Entry<Key, Value>): Int {
            assert(entries.last() == null)
            entries.forEachIndexed { index, keyValue ->
                when {
                    keyValue == null -> return index
                    keyValue.key == newEntry.key -> return index
                    keyValue.key > newEntry.key -> return index
                }
            }

            throw IllegalStateException("should never get here")
        }

        val index = getIndexOfSpotForPut(newEntry)
        val existingEntry = entries[index]
        return when {
            existingEntry == null -> {
                entries[index] = newEntry
                PutResponse.Success
            }
            existingEntry.key == newEntry.key -> {
                entries[index] = newEntry
                PutResponse.Success
            }
            existingEntry.key > newEntry.key -> {
                (entries.size - 1 downTo index + 1).forEach {
                    if (entries[it - 1] == null) return@forEach
                    entries[it] = entries[it - 1]
                }
                entries[index] = newEntry
                PutResponse.Success
            }
            else -> throw IllegalStateException("should never get here")
        }
    }
}
