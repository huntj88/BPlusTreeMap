package me.jameshunt.bplustree

import org.junit.Assert.assertEquals
import org.junit.BeforeClass
import org.junit.Test
import kotlin.random.Random

class BPlusTreeMapTest {

    @Test
    fun test() {
        val tree = BPlusTreeMap<Int, Int>()
        tree.put(1, 1)
        tree.put(2, 2)
        println(tree.get(1))
        println(tree.get(2))
        tree.put(3, 3)
        tree.put(4, 4)
        tree.put(5, 5)
        tree.put(6, 6)
        tree.put(7, 7)
        println(tree.get(6))
    }

    @Test
    fun moreSplittingOfNodes() {
        val tree = BPlusTreeMap<Int, Int>()
        tree.put(1, 1)
        tree.put(2, 2)
//        println(tree.get(1))
//        println(tree.get(2))
        tree.put(3, 3)
        tree.put(0, 0)
        tree.put(4, 4)
//        println(tree.get(3))
//        println(tree.get(0))
//        println(tree.get(4))

        tree.put(5, 5)
        tree.put(7, 7)
        tree.put(8, 8)
        tree.put(9, 9)
        tree.put(10, 10)
//        println(tree.get(5))
//
//
//        println(tree.get(1))
//        println(tree.get(2))
//        println(tree.get(3))
//        println(tree.get(0))
//        println(tree.get(4))
        (0..15).forEach {
            println(tree.get(it))
        }

        tree.getRange(1, 15).map { it.key }.let(::println)
    }

    @Test
    fun testGetRange() {
        val tree = BPlusTreeMap<Int, Int>()
        tree.put(1, 1)
        tree.put(2, 2)
        tree.put(3, 3)
        tree.put(4, 4)
        tree.put(5, 5)
        tree.put(6, 6)
        tree.put(7, 7)

        tree.getRange(1, 15).map { it.key }.also(::println).firstOrNull { it == 7 } ?: throw IllegalStateException()

        assertEquals(
            listOf(1, 2, 3),
            tree.getRange(1, 3).map { it.key }
        )

        assertEquals(
            listOf(1),
            tree.getRange(1, 1).map { it.key }
        )

        assertEquals(
            listOf(2, 3),
            tree.getRange(2, 3).map { it.key }
        )

        assertEquals(
            listOf(1, 2),
            tree.getRange(0, 2).map { it.key }
        )

        assertEquals(
            listOf(1, 2, 3, 4, 5, 6),
            tree.getRange(0, 6).map { it.key }
        )
    }

    @Test
    fun getAllTest() {
        val testData = (0 until 60000).toList()
        val random = Random(1)

        val tree = BPlusTreeMap<Int, Int>()

        testData
            .sortedBy { random.nextDouble() }
            .forEach { tree.put(it, it) }

        testData.forEach {
            tree.get(it) ?: throw IllegalStateException()
        }

        val fullRange = tree.getRange(start = 0, endInclusive = 59999)
        assertEquals(fullRange.size, 60000)
        assertEquals(2000, fullRange[2000].value)
    }

    companion object {
        lateinit var bTree: BPlusTreeMap<Int, Int>

        @BeforeClass
        @JvmStatic
        fun setup() {
            val random = Random(1)
            bTree = BPlusTreeMap<Int, Int>().apply {
                (0..1_000_000).sortedBy { random.nextInt() }.forEach { put(it, it * 2) }
            }
        }
    }

    @Test
    fun test1Million() {
        bTree.get(4343) ?: throw IllegalStateException()
        bTree.get(234233) ?: throw IllegalStateException()
        bTree.get(577432) ?: throw IllegalStateException()
        bTree.get(468743) ?: throw IllegalStateException()
        bTree.get(936743) ?: throw IllegalStateException()
        bTree.get(1_000_001)?.let { throw IllegalStateException("should be null") }

        val range = bTree.getRange(10000, 10500)
        println(range.take(200))
        if (501 != range.size) {
            throw IllegalStateException()
        }
    }
}
