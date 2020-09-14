/*
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
package org.apache.kafka.streams.scala.kstream

import java.time.Duration

import org.apache.kafka.streams.kstream.internals.StreamJoinedInternal
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.state.Stores
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StreamJoinedTest extends FlatSpec with Matchers {

  "Create a StreamJoined" should "create a StreamJoined with Serdes" in {
    val streamJoined: StreamJoined[String, String, Long] = StreamJoined.`with`[String, String, Long]

    val streamJoinedInternal = new StreamJoinedInternal[String, String, Long](streamJoined)
    streamJoinedInternal.keySerde().getClass shouldBe Serdes.stringSerde.getClass
    streamJoinedInternal.valueSerde().getClass shouldBe Serdes.stringSerde.getClass
    streamJoinedInternal.otherValueSerde().getClass shouldBe Serdes.longSerde.getClass
  }

  "Create a StreamJoined" should "create a StreamJoined with Serdes and Store Suppliers" in {
    val storeSupplier = Stores.inMemoryWindowStore("myStore", Duration.ofMillis(500), Duration.ofMillis(250), false)

    val otherStoreSupplier =
      Stores.inMemoryWindowStore("otherStore", Duration.ofMillis(500), Duration.ofMillis(250), false)

    val streamJoined: StreamJoined[String, String, Long] =
      StreamJoined.`with`[String, String, Long](storeSupplier, otherStoreSupplier)

    val streamJoinedInternal = new StreamJoinedInternal[String, String, Long](streamJoined)
    streamJoinedInternal.keySerde().getClass shouldBe Serdes.stringSerde.getClass
    streamJoinedInternal.valueSerde().getClass shouldBe Serdes.stringSerde.getClass
    streamJoinedInternal.otherValueSerde().getClass shouldBe Serdes.longSerde.getClass
    streamJoinedInternal.otherStoreSupplier().equals(otherStoreSupplier)
    streamJoinedInternal.thisStoreSupplier().equals(storeSupplier)
  }

  "Create a StreamJoined" should "create a StreamJoined with Serdes and a State Store name" in {
    val streamJoined: StreamJoined[String, String, Long] = StreamJoined.as[String, String, Long]("myStoreName")

    val streamJoinedInternal = new StreamJoinedInternal[String, String, Long](streamJoined)
    streamJoinedInternal.keySerde().getClass shouldBe Serdes.stringSerde.getClass
    streamJoinedInternal.valueSerde().getClass shouldBe Serdes.stringSerde.getClass
    streamJoinedInternal.otherValueSerde().getClass shouldBe Serdes.longSerde.getClass
    streamJoinedInternal.storeName().equals("myStoreName")
  }

}
