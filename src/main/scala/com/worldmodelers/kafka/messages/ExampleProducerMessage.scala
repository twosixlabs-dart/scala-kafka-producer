package com.worldmodelers.kafka.messages

import java.util.UUID

import com.fasterxml.jackson.annotation.JsonProperty

case class ExampleProducerMessage( @JsonProperty( "id" )
                                   id : String = UUID.randomUUID().toString(),
                                   @JsonProperty( "breadcrumbs" )
                                   breadcrumbs : Seq[ String ] = Seq() )