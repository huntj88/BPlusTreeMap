package me.jameshunt.bplustree

import org.junit.Assert.assertEquals
import org.junit.BeforeClass
import org.junit.Test
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread
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

    @Test
    fun insert1Million() {
        val random = Random(1)
        BPlusTreeMap<Int, Int>().apply {
            (0..1_000_000).forEach {
                put(it, it * 2)
                get(it) ?: throw IllegalStateException()
            }
        }
    }

    @Test
    fun insert1MillionConcurrent() {
        val numThreads = 12
        val latch = CountDownLatch(numThreads)

        BPlusTreeMap<Int, Int>().apply {
            (0 until numThreads step 2).forEach {
                (it..2_000_000 step numThreads).insertInOtherThread(this, latch)
                (it + 1..2_000_000 step numThreads).reversed().insertInOtherThread(this, latch)
            }


        }
        latch.await()
    }

    @Test
    fun testConcurrentWrite() {

        val tree = BPlusTreeMap<Int, Int>()
        val latch = CountDownLatch(8)

        val maxNum = 1500000
        val one = (0..maxNum step 8).reversed()
        val two = (1..maxNum step 8)
        val three = (2..maxNum step 8).reversed()
        val four = (3..maxNum step 8)
        val five = (4..maxNum step 8).reversed()
        val six = (5..maxNum step 8)
        val seven = (6..maxNum step 8).reversed()
        val eight = (7..maxNum step 8)

        one.insertInOtherThread(tree, latch)
        two.insertInOtherThread(tree, latch)
        three.insertInOtherThread(tree, latch)
        four.insertInOtherThread(tree, latch)
        five.insertInOtherThread(tree, latch)
        six.insertInOtherThread(tree, latch)
        seven.insertInOtherThread(tree, latch)
        eight.insertInOtherThread(tree, latch)

        latch.await()

        assertEquals((0..500).toList(), tree.getRange(0, 500).map { it.value })
        (0..maxNum).forEach {
            tree.get(it) ?: throw IllegalStateException()
        }
    }

    @Test
    fun readWriteSameTime() {
        val numThreads = 2
        val latch = CountDownLatch(numThreads)

        BPlusTreeMap<Int, Int>().apply {
            (0 until 50000).forEach {
                this.put(it, it)
            }
            (50000..1000000).insertInOtherThread(this, latch)
            thread {
                println("started")
                (-100000..1000000).mapNotNull { this.get(it) }.size.let(::println)
                println("read finished")
                latch.countDown()
            }

        }
        latch.await()
    }

    fun Iterable<Int>.insertInOtherThread(tree: BPlusTreeMap<Int, Int>, latch: CountDownLatch) = thread {
        this.forEach {
//            println("inserting: $it, thread: ${Thread.currentThread()}")
            tree.put(it, it)
//            tree.get(it)
        }
        latch.countDown()
        println("done")
    }
}
