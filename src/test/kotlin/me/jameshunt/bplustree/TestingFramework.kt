package me.jameshunt.bplustree

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.junit.Assert
import org.junit.Test
import java.io.File
import java.io.IOException
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.Phaser
import kotlin.concurrent.thread


class MoreTests {
    @Test
    fun testConcurrentWrite() {
        val maxNum = 1500000

        configureTest(
            initTree = {
                BPlusTreeMap<Int, Int>()
            },
            runTest = {
                threadWithLogging {
                    (0..maxNum step 8).reversed().forEach { tree.put(it, it) }
                }
                threadWithLogging {
                    (1..maxNum step 8).forEach { tree.put(it, it) }
                }
                threadWithLogging {
                    (2..maxNum step 8).reversed().forEach { tree.put(it, it) }
                }
                threadWithLogging {
                    (3..maxNum step 8).forEach { tree.put(it, it) }
                }
                threadWithLogging {
                    (4..maxNum step 8).reversed().forEach { tree.put(it, it) }
                }
                threadWithLogging {
                    (5..maxNum step 8).forEach { tree.put(it, it) }
                }
                threadWithLogging {
                    (6..maxNum step 8).reversed().forEach { tree.put(it, it) }
                }
                threadWithLogging {
                    (7..maxNum step 8).forEach { tree.put(it, it) }
                }
            },
            finalAsserts = {
                Assert.assertEquals((0..500).toList(), tree.getRange(0, 500).map { it.value })
                (0..maxNum).forEach {
                    tree.get(it) ?: throw IllegalStateException()
                }
            }
        )
    }

    @Test
    fun testConcurrentRead() {
        configureTest(
            initTree = {
                BPlusTreeMap<Int, Int>().apply {
                    (0..1_000_000).forEach {
                        this.put(it, it)
                    }
                }
            },
            runTest = {
                threadWithLogging {
                    (0..1_000_000 step 4).forEach {
                        tree.get(it) ?: throw IllegalStateException()
                    }
                }
                threadWithLogging {
                    (1..1_000_000 step 4).forEach {
                        tree.get(it) ?: throw IllegalStateException()
                    }
                }
                threadWithLogging {
                    (2..1_000_000 step 4).forEach {
                        tree.get(it) ?: throw IllegalStateException()
                    }
                }
                threadWithLogging {
                    (3..1_000_000 step 4).forEach {
                        tree.get(it) ?: throw IllegalStateException()
                    }
                }

                threadWithLogging {
                    (0 until 10).forEach {
                       val range = tree.getRange(it * 100_000, (it + 1) * 100_000)
                        assert(range.size == 100001)
                    }
                }
            })
    }

    @Test
    fun testSyncRead() {
        configureTest(
            initTree = {
                BPlusTreeMap<Int, Int>().apply {
                    (0..1_000_000).forEach {
                        this.put(it, it)
                    }
                }
            },
            runTest = {
                (0..1_000_000).forEach {
                    tree.get(it) ?: throw IllegalStateException()
                }
                (0 until 10).forEach {
                    val range = tree.getRange(it * 100_000, (it + 1) * 100_000)
                    assert(range.size == 100001)
                }
            })
    }
}


fun <Key : Comparable<Key>, Value> configureTest(
    initTree: () -> BPlusTreeMap<Key, Value>,
    runTest: TestWrapper<Key, Value>.() -> Unit,
    finalAsserts: TestWrapper<Key, Value>.() -> Unit = {}
) {
//    BPlusTreeMap.loggingImpl = TestLogging()
    BPlusTreeMap.loggingImpl = null
    val phaser = Phaser(1)
    val tree = initTree()

    val wrapper = TestWrapper(phaser, tree)
    runTest(wrapper)

    // give time for at least some child threads to register themselves
    val registerTimeMilli = 80
    Thread.sleep(registerTimeMilli.toLong())
    phaser.arriveAndAwaitAdvance()

    wrapper.testEndTime = Instant.now()
    val elapsedTimeMilli = wrapper.testStartTime.until(wrapper.testEndTime, ChronoUnit.MILLIS) - registerTimeMilli
    println("Elapsed milliseconds: $elapsedTimeMilli")
    finalAsserts(wrapper)

    if (wrapper.exceptions.isNotEmpty()) {
        wrapper.writeLogsToFile()

        assert(false) {
            println("Errors occurred, check log files")
        }
    }
}

class TestWrapper<Key : Comparable<Key>, Value>(
    private val phaser: Phaser,
    val tree: BPlusTreeMap<Key, Value>
) {
    val exceptions: MutableList<ExceptionData> = mutableListOf()
    val testStartTime: Instant = Instant.now()
    lateinit var testEndTime: Instant

    private val instantModule = SimpleModule().apply {
        this.addSerializer(Instant::class.java, object : JsonSerializer<Instant>() {
            @Throws(IOException::class, JsonProcessingException::class)
            override fun serialize(
                instant: Instant,
                jsonGenerator: JsonGenerator,
                serializerProvider: SerializerProvider
            ) {
                jsonGenerator.writeString(DateTimeFormatter.ISO_INSTANT.format(instant))
            }
        })
    }

    val mapper: ObjectWriter = ObjectMapper()
        .registerModule(KotlinModule())
        .registerModule(instantModule)
        .writerWithDefaultPrettyPrinter()

    fun threadWithLogging(block: () -> Unit) {
        thread {
            phaser.register()
            try {
                block()
            } catch (e: Exception) {
                BPlusTreeMap.loggingImpl?.let { it as TestLogging }?.exceptionDetected = true
                synchronized(exceptions) {
                    exceptions.add(ExceptionData(exception = e))
                }
                throw e
            } finally {
                phaser.arriveAndDeregister()
            }
        }
    }

    data class ExceptionData(
        val time: Instant = Instant.now(),
        val thread: String = Thread.currentThread().toString(),
        val exception: Exception
    )
}

fun TestWrapper<*, *>.writeLogsToFile() {
    val logDirectory = File("src/test/logs").also { it.mkdir() }
    BPlusTreeMap
        .loggingImpl
        ?.let { it as TestLogging }
        ?.let { it.logsLeadingUpToException + it.logsAfterException }
        ?.let(this.mapper::writeValueAsString)
        ?.let { File(logDirectory, "$testStartTime-logs.json").writeText(it) }

    val exceptionFile = File(logDirectory, "$testStartTime-exceptions.txt")
    this.exceptions.forEach {
        exceptionFile.appendText("${it.time}\n")
        exceptionFile.appendText("Exception in thread \"${it.thread}\" ${it.exception}\n")
        it.exception.stackTrace.forEach { traceElement ->
            exceptionFile.appendText("\tat $traceElement\n")
        }
        exceptionFile.appendText("\n")
    }
}

class TestLogging : Logging {
    @Volatile
    var exceptionDetected = false
    val logsLeadingUpToException: Queue<LogData> = LimitedQueue<LogData>(2000)
    val logsAfterException = mutableListOf<LogData>()

    override fun log(caller: Any, message: String) {
        when (exceptionDetected) {
            true -> synchronized(logsAfterException) {
                if (logsAfterException.isEmpty()) {
                    logsAfterException.add(
                        LogData(
                            caller = this.toString(),
                            message = "SEPARATOR between logs before and after exception"
                        )
                    )
                }
                if (logsAfterException.size < 1000) {
                    logsAfterException.add(
                        LogData(
                            caller = caller.toString(),
                            message = message
                        )
                    )
                }
            }
            false -> synchronized(logsLeadingUpToException) {
                logsLeadingUpToException.add(
                    LogData(
                        caller = caller.toString(),
                        message = message
                    )
                )
            }
        }
    }

    data class LogData(
        val time: Instant = Instant.now(),
        val caller: String,
        val thread: String = Thread.currentThread().toString(),
        val message: String
    )
}

class LimitedQueue<E>(private val limit: Int) : LinkedList<E>() {
    override fun add(element: E): Boolean {
        super.add(element)
        while (size > limit) {
            super.remove()
        }
        return true
    }
}
