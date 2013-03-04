/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.bridge.pig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import kafka.bridge.hadoop.KafkaOutputFormat;
import kafka.bridge.hadoop.KafkaRecordWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.piggybank.storage.avro.PigAvroDatumWriter;
import org.apache.pig.piggybank.storage.avro.PigSchema2Avro;

public class AvroKafkaStorage extends StoreFunc
{
  protected KafkaRecordWriter<Object, byte[]> writer;
  protected org.apache.avro.Schema avroSchema;
  protected PigAvroDatumWriter datumWriter;
  protected Encoder encoder;
  protected ByteArrayOutputStream os;

  public AvroKafkaStorage(String schema)
  {
    this.avroSchema = org.apache.avro.Schema.parse(schema);
  }

  @Override
  public OutputFormat getOutputFormat() throws IOException
  {
    return new KafkaOutputFormat();
  }

  @Override
  public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException
  {
    return location;
  }

  @Override
  public void setStoreLocation(String uri, Job job) throws IOException
  {
    KafkaOutputFormat.setOutputPath(job, new Path(uri));
  }

  @Override
  @SuppressWarnings("unchecked")
  public void prepareToWrite(RecordWriter writer) throws IOException
  {
    if (this.avroSchema == null)
      throw new IllegalStateException("avroSchema shouldn't be null");

    this.writer = (KafkaRecordWriter) writer;
    this.datumWriter = new PigAvroDatumWriter(this.avroSchema);
    this.os = new ByteArrayOutputStream();
    this.encoder = new BinaryEncoder(this.os);
  }

  @Override
  public void cleanupOnFailure(String location, Job job) throws IOException
  {
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature)
  {
  }

  @Override
  public void checkSchema(ResourceSchema schema) throws IOException
  {
    this.avroSchema = PigSchema2Avro.validateAndConvert(avroSchema, schema);
  }

  protected void writeEnvelope(OutputStream os, Encoder enc) throws IOException
  {
  }

  @Override
  public void putNext(Tuple tuple) throws IOException
  {
    os.reset();
    writeEnvelope(os, this.encoder);
    datumWriter.write(tuple, this.encoder);
    this.encoder.flush();

    try {
      this.writer.write(null, this.os.toByteArray());
    }
    catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
