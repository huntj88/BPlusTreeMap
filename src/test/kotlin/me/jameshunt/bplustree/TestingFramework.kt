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

    val timeNow = Instant.now()

    if (wrapper.exceptions.isNotEmpty()) {
        val logDirectory = File("src/test/logs").also { it.mkdir() }
        BPlusTreeMap
            .loggingImpl
            .let { it as TestLogging }
            .logs
            .let(wrapper.mapper::writeValueAsString)
            .let { File(logDirectory, "$timeNow-logs.txt").writeText(it) }

        val exceptionFile = File(logDirectory, "$timeNow-exceptions.txt")
        wrapper.exceptions.forEach {
            exceptionFile.appendText("${it.time}\n")
            exceptionFile.appendText("Exception in thread \"${it.thread}\" ${it.exception}\n")
            it.exception.stackTrace.forEach { traceElement ->
                exceptionFile.appendText("\tat $traceElement\n")
            }
            exceptionFile.appendText("\n")
        }

        assert(false) {
            println("Errors occurred, check log files")
        }
    }
}

class TestWrapper(val phaser: Phaser) {
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
                synchronized(exceptions) {
                    exceptions.add(ExceptionData(
                        time = Instant.now(),
                        thread = Thread.currentThread().toString(),
                        exception = e
                    ))
                }
                throw e
            } finally {
                phaser.arriveAndDeregister()
            }
        }
    }

    data class ExceptionData(
        val time: Instant,
        val thread: String,
        val exception: Exception
    )
}

class TestLogging : Logging {
    val logs: Queue<LogData> = LimitedQueue<LogData>(200)

    override fun log(caller: Any, message: String) {
        synchronized(logs) {
            logs.add(LogData(
                time = Instant.now(),
                caller = caller.toString(),
                thread = Thread.currentThread().toString(),
                message = message
            ))
        }
    }

    data class LogData(
        val time: Instant,
        val caller: String,
        val thread: String,
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

class Blah {
    @Test
    fun blahss() {
        configureTest {
            threadWithLogging {
                println("wow0")
            }

            threadWithLogging {
                println("wow1")
            }

            threadWithLogging {
                println("wow2")
            }

            threadWithLogging {
                println("wow3")
            }

            threadWithLogging {
                println("wow4")
            }

            threadWithLogging {
                println("wow5")
                throw IllegalStateException()
            }

            threadWithLogging {
                println("wow6")
            }
        }
    }
}
