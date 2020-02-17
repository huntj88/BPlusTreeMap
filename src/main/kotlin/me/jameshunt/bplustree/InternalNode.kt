package me.jameshunt.bplustree

import me.jameshunt.bplustree.Tree.*

class InternalNode<Key : Comparable<Key>, Value> : Node<Key, Value> {

    val keys: Array<Box<Key>?> = Array(numEntriesPerNode) { null }
    val children: Array<Node<Key, Value>?> = Array(numEntriesPerNode + 1) { null }

    override fun get(key: Key): Value? {
        val possibleLocationIndex = childIndexLocationOfKey(key)
        return children[possibleLocationIndex]?.get(key)
    }

    override fun getRange(start: Key, endInclusive: Key): List<Entry<Key, Value>> {
        val possibleLocationIndex = childIndexLocationOfKey(start)
        val node = children[possibleLocationIndex]!!
        return node.getRange(start, endInclusive)
    }

    override fun put(entry: Entry<Key, Value>): PutResponse<Key, Value> {
        val possibleLocationIndex = childIndexLocationOfKey(entry.key)
        val node = children[possibleLocationIndex]!!

        return when(val putResponse = node.put(entry)) {
            is PutResponse.Success -> putResponse
            is PutResponse.NodeFull<Key, Value> -> insertPromoted(putResponse)
        }
    }

    private fun insertPromoted(putResponse: PutResponse.NodeFull<Key, Value>): PutResponse<Key, Value> {
        fun isFull() = (keys.indexOfFirst { it == null } == -1)

        return when (isFull()) {
            true -> {
                val halfNumEntry = numEntriesPerNode / 2
                when {
                    putResponse.promoted < keys[0]!! -> {
                        val oldRightSide = InternalNode<Key, Value>().also { node ->
                            (halfNumEntry until numEntriesPerNode).forEachIndexed { index, oldPosition ->
                                node.keys[index] = keys[oldPosition]
                                node.children[index] = children[oldPosition]
                            }
                            node.children[halfNumEntry] = children[numEntriesPerNode]
                        }

                        val newLeftSide = InternalNode<Key, Value>().also { node ->
                            node.keys[0] = putResponse.promoted
                            (1 until halfNumEntry).forEach {
                                node.keys[it] = keys[it - 1]
                            }

                            node.children[0] = putResponse.left
                            node.children[1] = putResponse.right

                            (1..halfNumEntry).forEach {
                                node.children[it + 1] = children[it]
                            }
                        }

                        PutResponse.NodeFull(
                            promoted = keys[halfNumEntry - 1]!!,
                            left = newLeftSide,
                            right = oldRightSide
                        )
                    }
                    putResponse.promoted > keys[0]!! && putResponse.promoted < keys.last()!! -> {
                        val indexOfPromoted = keys.indexOfFirst { it!!.key > putResponse.promoted.key }

                        val leftNode = InternalNode<Key, Value>().also { node ->
                            (0 until indexOfPromoted).forEach {
                                node.keys[it] = keys[it]
                                node.children[it] = children[it]
                            }
                            node.children[indexOfPromoted] = putResponse.left
                        }

                        val rightNode = InternalNode<Key, Value>().also { node ->
                            node.children[0] = putResponse.right
                            (indexOfPromoted until numEntriesPerNode).forEachIndexed { index, indexOld ->
                                node.keys[index] = keys[indexOld]
                                node.children[index + 1] = children[indexOld + 1]
                            }
                        }

                        return PutResponse.NodeFull(putResponse.promoted, leftNode, rightNode)
                    }
                    putResponse.promoted > keys[keys.lastIndex]!! -> {
                        this.keys.forEach { assert(it != null) }

                        val oldLeftSide = InternalNode<Key, Value>().also { node ->
                            (0 until halfNumEntry).forEach {
                                node.keys[it] = keys[it]
                                node.children[it] = children[it]
                            }
                            node.children[halfNumEntry] = children[halfNumEntry]
                        }

                        val newRightSide = InternalNode<Key, Value>().also { node ->
                            val offset = halfNumEntry + 1
                            (offset until numEntriesPerNode).forEach {
                                node.keys[it - offset] = keys[it]
                            }
                            node.keys[halfNumEntry - 1] = putResponse.promoted

                            val grabFromIndex = node.children.size / 2 + 1
                            (0..halfNumEntry - 2).forEach {
                                node.children[it] = children[grabFromIndex + it]
                            }
                            node.children[halfNumEntry - 1] = putResponse.left
                            node.children[halfNumEntry] = putResponse.right
                        }

                        PutResponse.NodeFull(
                            promoted = keys[halfNumEntry]!!,
                            left = oldLeftSide,
                            right = newRightSide
                        )
                    }
                    else -> throw IllegalStateException("should not happen")
                }
            }
            false -> putInNodeWithEmptySpace(putResponse.promoted.key, putResponse.left, putResponse.right)
        }
    }

    private fun putInNodeWithEmptySpace(
        promoted: Key,
        left: Node<Key, Value>,
        right: Node<Key, Value>
    ): PutResponse.Success {

        fun getIndexOfSpotForPut(promoted: Key): Int {
            assert(keys.last() == null)
            keys.forEachIndexed { index, key ->
                when {
                    key == null -> return index
                    key.key == promoted -> return index
                    key.key > promoted -> return index
                }
            }

            throw IllegalStateException("should never get here")
        }

        val index = getIndexOfSpotForPut(promoted)
        val existingKey = keys[index]
        return when {
            existingKey == null -> {
                keys[index] = Box(promoted)
                children[index] = left
                children[index + 1] = right
                PutResponse.Success
            }
            existingKey.key > promoted -> {
                (keys.size - 1 downTo index + 1).forEach {
                    if (keys[it - 1] == null) return@forEach
                    keys[it] = keys[it - 1]
                    children[it + 1] = children[it]
                }
                keys[index] = Box(promoted)
                children[index] = left
                children[index + 1] = right
                PutResponse.Success
            }
            else -> throw IllegalStateException("should never get here")
        }
    }

    private fun childIndexLocationOfKey(key: Key): Int {
        // TODO: can be optimized with a binary search or something since they are already ordered
        keys.forEachIndexed { index, keyValue ->
            when {
                keyValue == null -> return index
                keyValue.key == key -> return index + 1 // entry containing key is always in right node of leaf split
                keyValue.key > key -> return index
            }
        }

        return children.lastIndex
    }
}
