import java.util.Properties

import better.files.Resource
import com.worldmodelers.kafka.producer.scala.ExampleProducer

object Main {

    def main( args : Array[ String ] ) : Unit = {
        val properties : Properties = {
            val p = new Properties()
            val pis = Resource.getAsStream( f"${args( 0 )}.properties" )
            p.load( pis )
            p
        }

        val topic = properties.getProperty( "topic.to" )

        val producer : ExampleProducer = new ExampleProducer( topic, properties )

        while ( true ) {
            producer.sendRandomMessage()
            Thread.sleep( 2000 )
        }

    }

}