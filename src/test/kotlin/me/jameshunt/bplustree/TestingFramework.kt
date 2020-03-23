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
import java.util.*
import java.util.concurrent.Phaser
import kotlin.concurrent.thread


class MoreTests {
    @Test
    fun testConcurrentWrite() {
        val tree = BPlusTreeMap<Int, Int>()
        val maxNum = 1500000

        configureTest {
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
        }

        Assert.assertEquals((0..500).toList(), tree.getRange(0, 500).map { it.value })
        (0..maxNum).forEach {
            tree.get(it) ?: throw IllegalStateException()
        }
    }
}


fun configureTest(block: TestWrapper.() -> Unit) {
    BPlusTreeMap.loggingImpl = TestLogging()
    val phaser = Phaser(1)
    val wrapper = TestWrapper(phaser)
    block(wrapper)

    // give time for at least some child threads to register themselves
    Thread.sleep(80)
    phaser.arriveAndAwaitAdvance()

    if (wrapper.exceptions.isNotEmpty()) {
        wrapper.writeLogsToFile()

        assert(false) {
            println("Errors occurred, check log files")
        }
    }
}

fun TestWrapper.writeLogsToFile() {
    val timeNow = Instant.now()

    val logDirectory = File("src/test/logs").also { it.mkdir() }
    BPlusTreeMap
        .loggingImpl
        .let { it as TestLogging }
        .let { it.logsLeadingUpToException + it.logsAfterException }
        .let(this.mapper::writeValueAsString)
        .let { File(logDirectory, "$timeNow-logs.json").writeText(it) }

    val exceptionFile = File(logDirectory, "$timeNow-exceptions.txt")
    this.exceptions.forEach {
        exceptionFile.appendText("${it.time}\n")
        exceptionFile.appendText("Exception in thread \"${it.thread}\" ${it.exception}\n")
        it.exception.stackTrace.forEach { traceElement ->
            exceptionFile.appendText("\tat $traceElement\n")
        }
        exceptionFile.appendText("\n")
    }
}

class TestWrapper(private val phaser: Phaser) {
    val exceptions: MutableList<ExceptionData> = mutableListOf()

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
                BPlusTreeMap.loggingImpl.let { it as TestLogging }.exceptionDetected = true
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
