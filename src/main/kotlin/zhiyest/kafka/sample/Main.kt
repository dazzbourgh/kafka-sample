package zhiyest.kafka.sample

import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration.ofMillis
import java.util.*
import kotlin.random.Random

fun main() {
    runBlocking {
        val jobs = listOf(
            launch {
                val props = Properties()
                props["bootstrap.servers"] = "localhost:9092"
                props["acks"] = "all"
                props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
                props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
                val producer: Producer<String, String> = KafkaProducer(props)
                val key = Random.nextInt()
                for (i in 0 until 10) {
                    producer.send(ProducerRecord("my-replicated-topic", key.toString(), i.toString()))
                    println("Producer: $key - $i")
                    delay(1000)
                }
                producer.close()
            },
            launch {
                val props = Properties()
                props.setProperty("bootstrap.servers", "localhost:9092")
                props.setProperty("group.id", "test")
                props.setProperty("enable.auto.commit", "true")
                props.setProperty("auto.commit.interval.ms", "1000")
                props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                val consumer = KafkaConsumer<String, String>(props)
                consumer.subscribe(listOf("my-replicated-topic"))
                var tries = 0
                while (tries < 3) {
                    val records = consumer.poll(ofMillis(100))
                    if (records.isEmpty) tries++
                    else for (record in records)
                        println("offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}")
                    delay(1000)
                }
                consumer.close()
            })
        jobs.forEach { it.join() }
    }
}
