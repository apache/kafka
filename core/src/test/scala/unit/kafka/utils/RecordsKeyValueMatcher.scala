/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unit.kafka.utils

import java.nio.ByteBuffer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{Record, SimpleRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serde}
import org.apache.kafka.common.utils.Utils
import org.hamcrest.{Description, TypeSafeDiagnosingMatcher}

/**
  * Heterogeneous matcher between alternative types of records:
  * [[ProducerRecord]], [[ConsumerRecord]] or [[Record]].
  *
  * It is conceptually incorrect to try to match records of different natures.
  * Only a committed [[Record]] is univoque, whereas a [[ProducerRecord]] or [[ConsumerRecord]] is
  * a physical representation of a record-to-be or viewed record.
  *
  * This matcher breaches that semantic so that testers can avoid performing manual comparisons on
  * targeted internal fields of these type of records. This implementation only compares key and
  * value of the records.
  *
  * @param expectedRecords The records expected.
  * @param topicPartition The topic-partition which the records belong to.
  * @param keySerde The [[Serde]] for the keys of the records.
  * @param valueSerde The [[Serde]] for the values of the records.
  * @tparam R1 The type of records used to formulate the expectations.
  * @tparam R2 The type of records compared against the expectations.
  * @tparam K The type of the record keys.
  * @tparam V The type of the record values.
  */
final class RecordsKeyValueMatcher[R1, R2, K, V](private val expectedRecords: Iterable[R1],
                                                 private val topicPartition: TopicPartition,
                                                 private val keySerde: Serde[K],
                                                 private val valueSerde: Serde[V])

  extends TypeSafeDiagnosingMatcher[Iterable[R2]] {

  override def describeTo(description: Description): Unit = {
    description.appendText(s"Records of $topicPartition: $expectedRecords")
  }

  override def matchesSafely(actualRecord: Iterable[R2], mismatchDescription: Description): Boolean = {
    if (expectedRecords.size != actualRecord.size) {
      mismatchDescription
        .appendText(s"Number of records differ. Expected: ")
        .appendValue(expectedRecords.size)
        .appendText(", Actual: ")
        .appendValue(actualRecord.size)
        .appendText(s"; Records: ${actualRecord.mkString("\n")}")
        .appendText("; ")
      return false
    }

    expectedRecords
      .zip(actualRecord)
      .map { case (expected, actual) => matches(expected, actual, mismatchDescription) }
      .reduce(_ && _)
  }

  private def matches(expected: R1, actual: R2, mismatchDescription: Description): Boolean = {
    val expectedRecord = convert(expected).getOrElse {
      mismatchDescription.appendText(s"Invalid expected record type: ${expected.getClass}")
      return false
    }

    val actualRecord = convert(actual).getOrElse {
      mismatchDescription.appendText(s"Invalid actual record type: ${expected.getClass}")
      return false
    }

    def compare(lhs: ByteBuffer, rhs: ByteBuffer, deserializer: Deserializer[_], desc: String): Boolean = {
      if ((lhs != null && !lhs.equals(rhs)) || (lhs == null && rhs != null)) {
        val deserializer = keySerde.deserializer()

        mismatchDescription.appendText(s"$desc mismatch. Expected: ")
          .appendValue(deserializer.deserialize(topicPartition.topic(), Utils.toNullableArray(lhs)))
          .appendText("; Actual: ")
          .appendValue(deserializer.deserialize(topicPartition.topic(), Utils.toNullableArray(rhs)))
          .appendText("; ")

        false
      } else true
    }

    val keq = compare(expectedRecord.key(), actualRecord.key(), keySerde.deserializer(), "Record key")
    val veq = compare(expectedRecord.value(), actualRecord.value(), valueSerde.deserializer(), "Record value")

    keq && veq
  }

  private def convert(recordCandidate: Any): Option[SimpleRecord] = {
    val keySerializer = keySerde.serializer()
    val valueSerializer = valueSerde.serializer()

    if (recordCandidate.isInstanceOf[ProducerRecord[K, V]]) {
      val record = recordCandidate.asInstanceOf[ProducerRecord[K, V]]
      Some(new SimpleRecord(record.timestamp(),
        Utils.wrapNullable(keySerializer.serialize(topicPartition.topic(), record.key())),
        Utils.wrapNullable(valueSerializer.serialize(topicPartition.topic(), record.value())),
        record.headers().toArray))

    } else if (recordCandidate.isInstanceOf[ConsumerRecord[K, V]]) {
      val record = recordCandidate.asInstanceOf[ConsumerRecord[K, V]]
      Some(new SimpleRecord(record.timestamp(),
        Utils.wrapNullable(keySerializer.serialize(topicPartition.topic(), record.key())),
        Utils.wrapNullable(valueSerializer.serialize(topicPartition.topic(), record.value())),
        record.headers().toArray))

    } else if (recordCandidate.isInstanceOf[Record]) {
      val record = recordCandidate.asInstanceOf[Record]
      Some(new SimpleRecord(record.timestamp(), record.key(), record.value(), record.headers()))

    } else {
      None
    }
  }
}

object RecordsKeyValueMatcher {

  /**
    * Provides a matcher which compares the key and value of a sequence of records with those of
    * the expectedRecords sequence, in order.
    *
    * @param expectedRecords The records expected.
    * @param topicPartition The topic-partition which the records belong to.
    * @param keySerde The [[Serde]] for the keys of the records.
    * @param valueSerde The [[Serde]] for the values of the records.
    * @tparam K The type of the record keys.
    * @tparam V The type of the record values.
    */
  def correspondTo[K, V](expectedRecords: Iterable[Any], topicPartition: TopicPartition)
                        (implicit keySerde: Serde[K], valueSerde: Serde[V]): RecordsKeyValueMatcher[Any, Any, K, V] = {
    new RecordsKeyValueMatcher[Any, Any, K, V](expectedRecords, topicPartition, keySerde, valueSerde)
  }

}


