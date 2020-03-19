package me.jameshunt.bplustree

import me.jameshunt.bplustree.BPlusTreeMap.Entry

class LeafNode<Key : Comparable<Key>, Value>(
    val leftLink: LeafNeighborAccess,
    val rightLink: LeafNeighborAccess
) : Node<Key, Value> {

    init {
        leftLink.setRight(this)
        rightLink.setLeft(this)
    }

    override val rwLock: ReadWriteLock = ReadWriteLock()
    private val entries: Array<Entry<Key, Value>?> = Array(numEntriesPerNode) { null }

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
        next!!.leftLink.lock()
        next.rightLink.lock()
        next.rwLock.lockRead()
        releaseAncestor()

        while (next != null) {
            next.entries.forEach { entry ->
                entry?.let {
                    when (entry.key in start..endInclusive) {
                        true -> collector.add(entry)
                        false -> if (collector.isEmpty()) Unit else {
                            next!!.rwLock.unlockRead()

                            next!!.leftLink.access.unlock()
                            next!!.rightLink.access.unlock()
                            return
                        }
                    }
                }
            }

            val nextRightLink = next.rightLink.getRight()?.also {
                assert(it.leftLink.access.isLocked)
                it.rightLink.lock()
                it.rwLock.lockRead()
            }
            next.rwLock.unlockRead()
            next.leftLink.access.unlock()

            if(nextRightLink == null) {
                next.rightLink.access.unlock()
            }

            next = nextRightLink as LeafNode<Key, Value>?
        }
    }

    override fun put(entry: Entry<Key, Value>, releaseAncestors: () -> Unit): PutResponse<Key, Value> {
        return when (entries.last() == null) {
            true -> {
                releaseAncestors()
                putInNodeWithEmptySpace(entry).also {
                    leftLink.unlockLeftWrite()
                    rwLock.unlockWrite()
                    rightLink.unlockRightWrite()

                    leftLink.access.unlock()
                    rightLink.access.unlock()
                }
            }
            false -> {
                splitLeaf(entry).also {
                    leftLink.unlockLeftWrite()
                    rightLink.unlockRightWrite()

                    leftLink.access.unlock()
                    rightLink.access.unlock()
                }
            }
        }
    }

    fun lockLeafWrite() {

        log("attempting to lock left link")
        leftLink.lock()
        log("attempting to lock right link")
        rightLink.lock()
        log("LOCKED links")

        log("attempting to lock left")
        leftLink.lockLeftWrite()
        log("attempting to lock middle")
        this.rwLock.lockWrite()
        log("attempting to lock right")
        rightLink.lockRightWrite()
        log("locked write locks")
    }

    private fun splitLeaf(newEntry: Entry<Key, Value>): PutResponse.NodeFull<Key, Value> {
        entries.forEach { assert(it != null) }
        rwLock.finalize()

        // TODO: optimize sort out
        val sorted = (entries + arrayOf(newEntry)).apply { sort() }

        val newMiddleLink = LeafNeighborAccess()

        val left = LeafNode<Key, Value>(leftLink, newMiddleLink).also { node ->
            (0 until numEntriesPerNode / 2).forEach {
                node.entries[it] = sorted[it]
            }
        }

        val right = LeafNode<Key, Value>(newMiddleLink, rightLink).also { node ->
            (numEntriesPerNode / 2 until sorted.size).forEachIndexed { newNodeIndex, i ->
                node.entries[newNodeIndex] = sorted[i]
            }
        }

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
