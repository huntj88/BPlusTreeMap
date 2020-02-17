package me.jameshunt.bplustree

import org.junit.Assert.assertEquals
import org.junit.Test
import kotlin.random.Random

class TreeTest {

    @Test
    fun test() {
        val tree = Tree<Int, Int>()
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
        val tree = Tree<Int, Int>()
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
        val tree = Tree<Int, Int>()
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
        val range = (1..60000)
        val random = Random(1)

        Tree<Int, Int>().apply {
            val nums = range.sortedBy { random.nextDouble() }

            nums.forEach { put(it, it) }

            range.forEach {
                get(it) ?: throw IllegalStateException()
            }

            assertEquals(getRange(4000, 5000).size, 1001)

            println(this)
        }
    }
}
