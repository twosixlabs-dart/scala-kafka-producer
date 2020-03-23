package com.worldmodelers.kafka.producer.scala

import java.util.Properties

import better.files.Resource
import com.worldmodelers.kafka.messages.{ExampleProducerMessage, ExampleProducerMessageJsonFormat, ExampleProducerMessageSerde}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.scalatest.{FlatSpec, Matchers}

class ExampleProducerTestSuite extends FlatSpec with Matchers with EmbeddedKafka with ExampleProducerMessageJsonFormat {

    implicit val config = EmbeddedKafkaConfig( kafkaPort = 6308, zooKeeperPort = 2111 )

    val valueSerde : Serde[ ExampleProducerMessage ] = new ExampleProducerMessageSerde
    implicit val serializer = valueSerde.serializer()
    implicit val deserializer = valueSerde.deserializer()

    val props : Properties = {
        val p = new Properties()
        val pStream = Resource.getAsStream( "test.properties" )
        p.load( pStream )
        p
    }

    val inputTopic : String = props.getProperty( "topic.to" )

    "Example Kafka Producer" should "send a message" in {
        val exampleProducer : ExampleProducer = new ExampleProducer( inputTopic, props )

        withRunningKafka {
            exampleProducer.sendRandomMessage()
            val result : ExampleProducerMessage = consumeFirstMessageFrom( inputTopic )
            result.id.length shouldBe 36 // string length of uuids
            result.breadcrumbs should contain( "scala-kafka-producer" )
        }
    }

}
