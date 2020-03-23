package me.jameshunt.bplustree

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.Test
import java.io.File
import java.io.IOException
import java.time.Instant
import java.time.format.DateTimeFormatter

object LogParser {

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

    private val mapper: ObjectMapper = ObjectMapper()
        .registerModule(KotlinModule())
        .registerModule(JavaTimeModule())
        .registerModule(instantModule)

    fun parse(logFileName: String): List<TestLogging.LogData> {
        return File("src/test/logs/$logFileName")
            .readBytes()
            .let { mapper.readValue(it) }
    }
}

class LogTester {

    @Test
    fun lastOfEachThread() {
        LogParser
            .parse("2020-03-23T04:12:37.959Z-logs.json")
            .groupBy { it.thread }
            .forEach { (thread, data) ->
                val last = data.last()
                print(
                    """
                        |${"\n"}
                        |time:    ${last.time}
                        |thread:  $thread, 
                        |caller:  ${last.caller}
                        |message: ${last.message}
                """.trimMargin()
                )
            }
    }
}
