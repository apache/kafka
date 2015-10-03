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

package org.apache.kafka.log4jappender;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A log4j appender that produces log messages to Kafka
 */
public class KafkaLog4jAppender extends AppenderSkeleton {

    private static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    private static final String COMPRESSION_TYPE_CONFIG = "compression.type";
    private static final String ACKS_CONFIG = "acks";
    private static final String RETRIES_CONFIG = "retries";
    private static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
    private static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";

    private String brokerList = null;
    private String topic = null;
    private String compressionType = null;

    private int retries = 0;
    private int requiredNumAcks = Integer.MAX_VALUE;
    private boolean syncSend = false;
    private Producer<byte[], byte[]> producer = null;

    public Producer<byte[], byte[]> getProducer() {
        return producer;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public int getRequiredNumAcks() {
        return requiredNumAcks;
    }

    public void setRequiredNumAcks(int requiredNumAcks) {
        this.requiredNumAcks = requiredNumAcks;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean getSyncSend() {
        return syncSend;
    }

    public void setSyncSend(boolean syncSend) {
        this.syncSend = syncSend;
    }

    @Override
    public void activateOptions() {
        // check for config parameter validity
        Properties props = new Properties();
        if (brokerList != null)
            props.put(BOOTSTRAP_SERVERS_CONFIG, brokerList);
        if (props.isEmpty())
            throw new ConfigException("The bootstrap servers property should be specified");
        if (topic == null)
            throw new ConfigException("Topic must be specified by the Kafka log4j appender");
        if (compressionType != null)
            props.put(COMPRESSION_TYPE_CONFIG, compressionType);
        if (requiredNumAcks != Integer.MAX_VALUE)
            props.put(ACKS_CONFIG, Integer.toString(requiredNumAcks));
        if (retries > 0)
            props.put(RETRIES_CONFIG, retries);

        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        this.producer = getKafkaProducer(props);
        LogLog.debug("Kafka producer connected to " + brokerList);
        LogLog.debug("Logging for topic: " + topic);
    }

    protected Producer<byte[], byte[]> getKafkaProducer(Properties props) {
        return new KafkaProducer<byte[], byte[]>(props);
    }

    @Override
    protected void append(LoggingEvent event) {
        String message = subAppend(event);
        LogLog.debug("[" + new Date(event.getTimeStamp()) + "]" + message);
        Future<RecordMetadata> response = producer.send(new ProducerRecord<byte[], byte[]>(topic, message.getBytes()));
        if (syncSend) {
            try {
                response.get();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            } catch (ExecutionException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private String subAppend(LoggingEvent event) {
        return (this.layout == null) ? event.getRenderedMessage() : this.layout.format(event);
    }

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;
            producer.close();
        }
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }
}
