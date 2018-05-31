/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 * Copyright (C) 2017-2018 Alexis Seigneurin.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.scala

import java.util.regex.Pattern

import org.scalatest.junit.JUnitSuite
import org.junit.Assert._
import org.junit._

import org.apache.kafka.streams.scala.kstream._

import ImplicitConversions._

import org.apache.kafka.streams.{StreamsBuilder => StreamsBuilderJ, _}
import org.apache.kafka.streams.kstream.{KTable => KTableJ, KStream => KStreamJ, KGroupedStream => KGroupedStreamJ, _}
import collection.JavaConverters._

/**
 * Test suite that verifies that the topology built by the Java and Scala APIs match.
 */ 
class TopologyTest extends JUnitSuite {

  val inputTopic = "input-topic"
  val userClicksTopic = "user-clicks-topic"
  val userRegionsTopic = "user-regions-topic"

  val pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

  @Test def shouldBuildIdenticalTopologyInJavaNScalaSimple() = {

    // build the Scala topology
    def getTopologyScala(): TopologyDescription = {

      import Serdes._
  
      val streamBuilder = new StreamsBuilder
      val textLines = streamBuilder.stream[String, String](inputTopic)
  
      val _: KStream[String, String] =
        textLines.flatMapValues(v => pattern.split(v.toLowerCase))
  
      streamBuilder.build().describe()
    }
  
    // build the Java topology
    def getTopologyJava(): TopologyDescription = {

      val streamBuilder = new StreamsBuilderJ
      val textLines = streamBuilder.stream[String, String](inputTopic)
  
      val _: KStreamJ[String, String] = textLines.flatMapValues {
        new ValueMapper[String, java.lang.Iterable[String]] {
          def apply(s: String): java.lang.Iterable[String] = pattern.split(s.toLowerCase).toIterable.asJava
        }
      }
      streamBuilder.build().describe()
    }

    // should match
    assertEquals(getTopologyScala(), getTopologyJava())
  }

  @Test def shouldBuildIdenticalTopologyInJavaNScalaAggregate() = {

    // build the Scala topology
    def getTopologyScala(): TopologyDescription = {

      import Serdes._
  
      val streamBuilder = new StreamsBuilder
      val textLines = streamBuilder.stream[String, String](inputTopic)
  
      val _: KTable[String, Long] =
        textLines.flatMapValues(v => pattern.split(v.toLowerCase))
          .groupBy((k, v) => v)
          .count()
  
      streamBuilder.build().describe()
    }

    // build the Java topology
    def getTopologyJava(): TopologyDescription = {

      val streamBuilder = new StreamsBuilderJ
      val textLines: KStreamJ[String, String] = streamBuilder.stream[String, String](inputTopic)
  
      val splits: KStreamJ[String, String] = textLines.flatMapValues {
        new ValueMapper[String, java.lang.Iterable[String]] {
          def apply(s: String): java.lang.Iterable[String] = pattern.split(s.toLowerCase).toIterable.asJava
        }
      }
  
      val grouped: KGroupedStreamJ[String, String] = splits.groupBy {
        new KeyValueMapper[String, String, String] {
          def apply(k: String, v: String): String = v
        }
      }
  
      val wordCounts: KTableJ[String, java.lang.Long] = grouped.count()
  
      streamBuilder.build().describe()
    }

    // should match
    assertEquals(getTopologyScala(), getTopologyJava())
  }

  @Test def shouldBuildIdenticalTopologyInJavaNScalaJoin() = {

    // build the Scala topology
    def getTopologyScala(): TopologyDescription = {
      import Serdes._
  
      val builder = new StreamsBuilder()
  
      val userClicksStream: KStream[String, Long] = builder.stream(userClicksTopic)
  
      val userRegionsTable: KTable[String, String] = builder.table(userRegionsTopic)
  
      val clicksPerRegion: KTable[String, Long] =
        userClicksStream
          .leftJoin(userRegionsTable)((clicks, region) => (if (region == null) "UNKNOWN" else region, clicks))
          .map((_, regionWithClicks) => regionWithClicks)
          .groupByKey
          .reduce(_ + _)

      builder.build().describe()
    }

    // build the Java topology
    def getTopologyJava(): TopologyDescription = {

      import java.lang.{Long => JLong}
  
      val builder: StreamsBuilderJ = new StreamsBuilderJ()
  
      val userClicksStream: KStreamJ[String, JLong] = 
        builder.stream[String, JLong](userClicksTopic, Consumed.`with`(Serdes.String, Serdes.JavaLong))
  
      val userRegionsTable: KTableJ[String, String] = 
        builder.table[String, String](userRegionsTopic, Consumed.`with`(Serdes.String, Serdes.String))
  
      // Join the stream against the table.
      val userClicksJoinRegion: KStreamJ[String, (String, JLong)] = userClicksStream
        .leftJoin(userRegionsTable, 
          new ValueJoiner[JLong, String, (String, JLong)] {
            def apply(clicks: JLong, region: String): (String, JLong) = 
              (if (region == null) "UNKNOWN" else region, clicks)
          }, 
          Joined.`with`[String, JLong, String](Serdes.String, Serdes.JavaLong, Serdes.String))
  
      // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
      val clicksByRegion : KStreamJ[String, JLong] = userClicksJoinRegion
        .map { 
          new KeyValueMapper[String, (String, JLong), KeyValue[String, JLong]] {
            def apply(k: String, regionWithClicks: (String, JLong)) = new KeyValue[String, JLong](regionWithClicks._1, regionWithClicks._2)
          }
        }
          
      // Compute the total per region by summing the individual click counts per region.
      val clicksPerRegion: KTableJ[String, JLong] = clicksByRegion
        .groupByKey(Serialized.`with`(Serdes.String, Serdes.JavaLong))
        .reduce {
          new Reducer[JLong] {
            def apply(v1: JLong, v2: JLong) = v1 + v2
          }
        }

      builder.build().describe()
    }

    // should match
    assertEquals(getTopologyScala(), getTopologyJava())
  }
}
