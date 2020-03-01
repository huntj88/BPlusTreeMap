package me.jameshunt.bplustree

import me.jameshunt.bplustree.BPlusTreeMap.Entry

class LeafNode<Key : Comparable<Key>, Value> : Node<Key, Value> {
    override val rwLock: ReadWriteLock = ReadWriteLock()

    private val entries: Array<Entry<Key, Value>?> = Array(numEntriesPerNode) { null }
    private var leftLink: LeafNode<Key, Value>? = null
    private var rightLink: LeafNode<Key, Value>? = null

    override fun get(key: Key, releaseAncestor: () -> Unit): Value? {
        return rwLock.withReadLock {
            releaseAncestor()

            // TODO: could be binary search, already sorted
            entries.forEach {
                if (key == it?.key) {
                    return@withReadLock it.value
                }
            }
            return@withReadLock null
        }
    }

    override fun getRange(start: Key, endInclusive: Key, releaseAncestor: () -> Unit): List<Entry<Key, Value>> {
        return mutableListOf<Entry<Key, Value>>().apply {
            getRangeAscending(
                collector = this,
                start = start,
                endInclusive = endInclusive,
                releaseAncestor = releaseAncestor
            )
        }
    }

    private fun getRangeAscending(
        collector: MutableList<Entry<Key, Value>>,
        start: Key,
        endInclusive: Key,
        releaseAncestor: () -> Unit
    ) {
        var next = this as LeafNode<Key, Value>?
        next!!.rwLock.lockRead()
        releaseAncestor()

        while (next != null) {
            next.entries.forEach { entry ->
                entry?.let {
                    when (entry.key in start..endInclusive) {
                        true -> collector.add(entry)
                        false -> if (collector.isEmpty()) Unit else {
                            next!!.rwLock.unlockRead()
                            return
                        }
                    }
                }
            }
            val nextRightLink = next.rightLink?.also { it.rwLock.lockRead() }
            next.rwLock.unlockRead()
            next = nextRightLink
        }
    }

    override fun put(entry: Entry<Key, Value>, releaseAncestors: () -> Unit): PutResponse<Key, Value> {
        return when (entries.last() == null) {
            true -> {
                releaseAncestors()
                putInNodeWithEmptySpace(entry).also {
                    rwLock.unlockWrite()
                }
            }
            false -> {
                resolvePotentialWriteDeadlock()
                leftLink?.rwLock?.lockWrite()
                rightLink?.rwLock?.lockWrite()
                splitLeaf(entry).also {
                    leftLink?.rwLock?.unlockWrite()
                    rightLink?.rwLock?.unlockWrite()
                }
            }
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

    private fun resolvePotentialWriteDeadlock() {
        val leftNodeSplitting = leftLink
            ?.let { synchronized(it) { it.rwLock.isWriteLocked() && it.entries.last() != null} }
            ?: false

        val rightNodeSplitting = rightLink
            ?.let { synchronized(it) { it.rwLock.isWriteLocked() && it.entries.last() != null} }
            ?: false

        val rangeSelectAscendingLeftNodeReadLocked = leftLink?.rwLock?.isReadLocked() ?: false

        if(leftNodeSplitting || rightNodeSplitting || rangeSelectAscendingLeftNodeReadLocked) {
            // other thread is trying to do its own thing starting from a neighbor node. Let it do its thing first
            // other thread that already has pending lock on this node will get it, since order is fair,
            // this node then queues itself up to acquire the same write lock again
            rwLock.unlockWrite()
            rwLock.lockWrite()
        }
    }
}
