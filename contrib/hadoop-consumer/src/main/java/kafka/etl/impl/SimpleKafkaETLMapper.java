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
package kafka.etl.impl;

import kafka.etl.KafkaETLKey;
import kafka.etl.KafkaETLUtils;
import kafka.message.Message;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Simple implementation of KafkaETLMapper. It assumes that 
 * input data are text timestamp (long).
 */
@SuppressWarnings("deprecation")
public class SimpleKafkaETLMapper implements
Mapper<KafkaETLKey, BytesWritable, LongWritable, Text> {

    protected long _count = 0;
    
	protected Text getData(Message message) throws IOException {
		ByteBuffer buf = message.payload();
		if(buf == null)
		  return new Text();
		
		byte[] array = new byte[buf.limit()];
		buf.get(array);
		
		Text text = new Text( new String(array, "UTF8"));
		return text;
	}


    @Override
    public void map(KafkaETLKey key, BytesWritable val,
            OutputCollector<LongWritable, Text> collector,
            Reporter reporter) throws IOException {
        
         
        byte[] bytes = KafkaETLUtils.getBytes(val);
        
        //check the checksum of message
        Message message = new Message(ByteBuffer.wrap(bytes));
        long checksum = key.getChecksum();
        if (checksum != message.checksum()) 
            throw new IOException ("Invalid message checksum " 
                                            + message.checksum() + ". Expected " + key + ".");
        Text data = getData (message);
        _count ++;
           
        collector.collect(new LongWritable (_count), data);

    }


    @Override
    public void configure(JobConf arg0) {
        // TODO Auto-generated method stub
        
    }


    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        
    }

}
