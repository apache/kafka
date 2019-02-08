/*
 * Copyright (C) 2018 Joan Goyeau.
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
package org.apache.kafka.streams.scala.kstream

import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.scala.Serdes._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class JoinedTest extends FlatSpec with Matchers {

  "Create a Joined" should "create a Joined with Serdes" in {
    val joined: Joined[String, Long, Int] = Joined.`with`[String, Long, Int]

    joined.keySerde.getClass shouldBe Serdes.String.getClass
    joined.valueSerde.getClass shouldBe Serdes.Long.getClass
    joined.otherValueSerde.getClass shouldBe Serdes.Integer.getClass
  }

  "Create a Joined" should "create a Joined with Serdes and repartition topic name" in {
    val repartitionTopicName = "repartition-topic"
    val joined: Joined[String, Long, Int] = Joined.`with`(repartitionTopicName)

    joined.keySerde.getClass shouldBe Serdes.String.getClass
    joined.valueSerde.getClass shouldBe Serdes.Long.getClass
    joined.otherValueSerde.getClass shouldBe Serdes.Integer.getClass
    joined.name() shouldBe repartitionTopicName
  }
}
