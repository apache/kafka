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


import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import kafka.etl.KafkaETLKey;
import kafka.etl.KafkaETLRequest;
import kafka.etl.Props;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.producer.KeyedMessage;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

import static org.apache.kafka.common.utils.Utils.formatAddress;

/**
 * Use this class to produce test events to Kafka server. Each event contains a
 * random timestamp in text format.
 */
@SuppressWarnings("deprecation")
public class DataGenerator {

	protected final static Random RANDOM = new Random(
			System.currentTimeMillis());

	protected Props _props;
	protected Producer _producer = null;
	protected URI _uri = null;
	protected String _topic;
	protected int _count;
	protected String _offsetsDir;
	protected final int TCP_BUFFER_SIZE = 300 * 1000;
	protected final int CONNECT_TIMEOUT = 20000; // ms
	protected final int RECONNECT_INTERVAL = Integer.MAX_VALUE; // ms

	public DataGenerator(String id, Props props) throws Exception {
		_props = props;
		_topic = props.getProperty("kafka.etl.topic");
		System.out.println("topics=" + _topic);
		_count = props.getInt("event.count");

		_offsetsDir = _props.getProperty("input");
		
		// initialize kafka producer to generate count events
		String serverUri = _props.getProperty("kafka.server.uri");
		_uri = new URI (serverUri);
		
		System.out.println("server uri:" + _uri.toString());
        Properties producerProps = new Properties();
        producerProps.put("metadata.broker.list", formatAddress(_uri.getHost(), _uri.getPort()));
        producerProps.put("send.buffer.bytes", String.valueOf(TCP_BUFFER_SIZE));
        producerProps.put("connect.timeout.ms", String.valueOf(CONNECT_TIMEOUT));
        producerProps.put("reconnect.interval", String.valueOf(RECONNECT_INTERVAL));
        
		_producer = new Producer(new ProducerConfig(producerProps));
			
	}

	public void run() throws Exception {

		List<KeyedMessage> list = new ArrayList<KeyedMessage>();
		for (int i = 0; i < _count; i++) {
			Long timestamp = RANDOM.nextLong();
			if (timestamp < 0) timestamp = -timestamp;
			byte[] bytes = timestamp.toString().getBytes("UTF8");
            list.add(new KeyedMessage<Integer, byte[]>(_topic, null, bytes));
		}
		// send events
		System.out.println(" send " + list.size() + " " + _topic + " count events to " + _uri);
		_producer.send(list);

		// close the producer
		_producer.close();
		
		// generate offset files
		generateOffsets();
	}

    protected void generateOffsets() throws Exception {
        JobConf conf = new JobConf();
        conf.set("hadoop.job.ugi", _props.getProperty("hadoop.job.ugi"));
        conf.setCompressMapOutput(false);
        Path outPath = new Path(_offsetsDir + Path.SEPARATOR + "1.dat");
        FileSystem fs = outPath.getFileSystem(conf);
        if (fs.exists(outPath)) fs.delete(outPath);
        
        KafkaETLRequest request =
            new KafkaETLRequest(_topic, "tcp://" + formatAddress(_uri.getHost(), _uri.getPort()), 0);

        System.out.println("Dump " + request.toString() + " to " + outPath.toUri().toString());
        byte[] bytes = request.toString().getBytes("UTF-8");
        KafkaETLKey dummyKey = new KafkaETLKey();
        SequenceFile.setCompressionType(conf, SequenceFile.CompressionType.NONE);
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outPath, 
                                        KafkaETLKey.class, BytesWritable.class);
        writer.append(dummyKey, new BytesWritable(bytes));
        writer.close();
    }
    
	public static void main(String[] args) throws Exception {

		if (args.length < 1)
			throw new Exception("Usage: - config_file");

		Props props = new Props(args[0]);
		DataGenerator job = new DataGenerator("DataGenerator", props);
		job.run();
	}

}
