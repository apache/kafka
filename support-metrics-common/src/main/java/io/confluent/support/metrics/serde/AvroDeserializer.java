/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.support.metrics.serde;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroDeserializer {

  /**
   * Deserializes the bytes as an array of Generic containers.
   *
   * <p>The bytes include a standard Avro header that contains a magic byte, the
   * record's Avro schema (and so on), followed by the byte representation of the record.
   *
   * <p>Implementation detail:  This method uses Avro's {@code DataFileWriter}.
   *
   * @return A Generic Container class
   * @schema Schema associated with this container
   */
  public GenericContainer[] deserialize(Schema schema, byte[] container) throws IOException {
    GenericContainer ret = null;
    List<GenericContainer> retList = new ArrayList<>();
    if (container != null) {
      DatumReader<GenericContainer> datumReader = new GenericDatumReader<>(schema);
      ByteArrayInputStream in = new ByteArrayInputStream(container);
      DataFileStream<GenericContainer> reader = new DataFileStream<GenericContainer>(
          in,
          datumReader
      );
      while (reader.hasNext()) {
        ret = reader.next(ret);
        retList.add(ret);
      }
      return retList.toArray(new GenericContainer[retList.size()]);
    } else {
      return null;
    }
  }

  /**
   * Deserializes the bytes as an array of Generic containers. Assumes schema is
   * embedded with bytes.
   *
   * <p>The bytes include a standard Avro header that contains a magic byte, the
   * record's Avro schema (and so on), followed by the byte representation of the record.
   *
   * <p>Implementation detail:  This method uses Avro's {@code DataFileWriter}.
   *
   * @return A Generic Container class
   */
  public GenericContainer[] deserialize(byte[] container) throws IOException {
    GenericContainer ret = null;
    List<GenericContainer> retList = new ArrayList<>();
    if (container != null) {
      DatumReader<GenericContainer> datumReader = new GenericDatumReader<>();
      ByteArrayInputStream in = new ByteArrayInputStream(container);
      DataFileStream<GenericContainer> reader = new DataFileStream<GenericContainer>(
          in,
          datumReader
      );
      while (reader.hasNext()) {
        ret = reader.next(ret);
        retList.add(ret);
      }
      return retList.toArray(new GenericContainer[retList.size()]);
    } else {
      return null;
    }
  }

  /**
   * Deserializes the bytes of AVRO records as specific containers of a particular class
   */
  @SuppressWarnings("unchecked")
  public <T> T[] deserialize(Class<T> clazz, byte[] container) throws IOException {
    T ret = null;
    List<T> retList = new ArrayList<T>();
    if (container != null) {
      DatumReader<T> datumReader = new SpecificDatumReader<T>(clazz);
      ByteArrayInputStream in = new ByteArrayInputStream(container);
      DataFileStream<T> reader = new DataFileStream<T>(in, datumReader);
      while (reader.hasNext()) {
        ret = reader.next(ret);
        retList.add(ret);
      }
      return retList.toArray((T[]) java.lang.reflect.Array.newInstance(clazz, retList.size()));
    } else {
      return null;
    }
  }

}
