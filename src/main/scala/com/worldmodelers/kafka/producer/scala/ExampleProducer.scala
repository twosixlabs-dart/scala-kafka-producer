package com.worldmodelers.kafka.producer.scala

import java.util.{Properties, UUID}

import com.worldmodelers.kafka.messages.ExampleProducerMessage
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


class ExampleProducer( val topic : String, val properties : Properties ) {

    implicit val executionContext : ExecutionContext = scala.concurrent.ExecutionContext.global

    private val LOG : Logger = LoggerFactory.getLogger( getClass )
    private val kafkaProps : Properties = getKafkaProperties()

    private val producer : KafkaProducer[ String, ExampleProducerMessage ] = new KafkaProducer[ String, ExampleProducerMessage ]( kafkaProps )


    def sendRandomMessage( ) : Unit = {
        val key = UUID.randomUUID().toString
        val value = ExampleProducerMessage( key, Seq( "scala-kafka-producer" ) )

        LOG.info( s"sending message : ${key}" )

        val message = new ProducerRecord[ String, ExampleProducerMessage ]( topic, key, value )

        //@formatter:off
        Future( producer.send( message ).get() ).onComplete {
                case Success( s ) => LOG.info( s"Message ${key} successfully sent" )
                case Failure( e ) => {
                    LOG.error( s"${e.getClass.getSimpleName} : ${e.getMessage} : ${e.getCause}" )
                    e.printStackTrace()
                }
        }
        //@formatter:on
    }

    private def getKafkaProperties( ) : Properties = {
        val kProps = properties
          .asScala
          .toList
          .filter( _._1.startsWith( "kafka." ) )
          .map( pair => {
              val key = pair._1.split( "kafka." )( 1 )
              (key, pair._2)
          } )

        val kafkaProps = new Properties()
        kProps.foreach( prop => kafkaProps.put( prop._1, prop._2 ) )
        kafkaProps
    }

}