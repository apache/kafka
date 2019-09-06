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
package org.apache.kafka.streams.perf;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;


/**
 * A basic DSL and data generation that emulates the behavior of the Yahoo Benchmark
 * https://yahooeng.tumblr.com/post/135321837876/benchmarking-streaming-computation-engines-at
 * Thanks to Michael Armbrust for providing the initial code for this benchmark in his blog:
 * https://databricks.com/blog/2017/06/06/simple-super-fast-streaming-engine-apache-spark.html
 */
public class YahooBenchmark {
    private final SimpleBenchmark parent;
    private final String campaignsTopic;
    private final String eventsTopic;

    static class ProjectedEvent {
        /* attributes need to be public for serializer to work */
        /* main attributes */
        String eventType;
        String adID;

        /* other attributes */
        long eventTime;
        /* not used
        public String userID = UUID.randomUUID().toString();
        public String pageID = UUID.randomUUID().toString();
        public String addType = "banner78";
        public String ipAddress = "1.2.3.4";
         */
    }

    static class CampaignAd {
        /* attributes need to be public for serializer to work */
        String adID;
        String campaignID;
    }

    @SuppressWarnings("WeakerAccess")
    public YahooBenchmark(final SimpleBenchmark parent, final String campaignsTopic, final String eventsTopic) {
        this.parent = parent;
        this.campaignsTopic = campaignsTopic;
        this.eventsTopic = eventsTopic;
    }

    // just for Yahoo benchmark
    private boolean maybeSetupPhaseCampaigns(final String topic,
                                             final String clientId,
                                             final boolean skipIfAllTests,
                                             final int numCampaigns,
                                             final int adsPerCampaign,
                                             final List<String> ads) {
        parent.resetStats();
        // initialize topics
        System.out.println("Initializing topic " + topic);

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, parent.props.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int c = 0; c < numCampaigns; c++) {
                final String campaignID = UUID.randomUUID().toString();
                for (int a = 0; a < adsPerCampaign; a++) {
                    final String adId = UUID.randomUUID().toString();
                    final String concat = adId + ":" + campaignID;
                    producer.send(new ProducerRecord<>(topic, adId, concat));
                    ads.add(adId);
                    parent.processedRecords++;
                    parent.processedBytes += concat.length() + adId.length();
                }
            }
        }
        return true;
    }

    // just for Yahoo benchmark
    private void maybeSetupPhaseEvents(final String topic,
                                       final String clientId,
                                       final int numRecords,
                                       final List<String> ads) {
        parent.resetStats();
        final String[] eventTypes = new String[]{"view", "click", "purchase"};
        final Random rand = new Random(System.currentTimeMillis());
        System.out.println("Initializing topic " + topic);

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, parent.props.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        final long startTime = System.currentTimeMillis();

        try (final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            final ProjectedEvent event = new ProjectedEvent();
            final Map<String, Object> serdeProps = new HashMap<>();
            final Serializer<ProjectedEvent> projectedEventSerializer = new JsonPOJOSerializer<>();
            serdeProps.put("JsonPOJOClass", ProjectedEvent.class);
            projectedEventSerializer.configure(serdeProps, false);

            for (int i = 0; i < numRecords; i++) {
                event.eventType = eventTypes[rand.nextInt(eventTypes.length - 1)];
                event.adID = ads.get(rand.nextInt(ads.size() - 1));
                event.eventTime = System.currentTimeMillis();
                final byte[] value = projectedEventSerializer.serialize(topic, event);
                producer.send(new ProducerRecord<>(topic, event.adID, value));
                parent.processedRecords++;
                parent.processedBytes += value.length + event.adID.length();
            }
        }

        final long endTime = System.currentTimeMillis();

        parent.printResults("Producer Performance [records/latency/rec-sec/MB-sec write]: ", endTime - startTime);
    }


    public void run() {
        final int numCampaigns = 100;
        final int adsPerCampaign = 10;

        final List<String> ads = new ArrayList<>(numCampaigns * adsPerCampaign);
        maybeSetupPhaseCampaigns(campaignsTopic, "simple-benchmark-produce-campaigns", false, numCampaigns, adsPerCampaign, ads);
        maybeSetupPhaseEvents(eventsTopic, "simple-benchmark-produce-events", parent.numRecords, ads);

        final CountDownLatch latch = new CountDownLatch(1);
        parent.setStreamProperties("simple-benchmark-yahoo" + new Random().nextInt());

        final KafkaStreams streams = createYahooBenchmarkStreams(parent.props, campaignsTopic, eventsTopic, latch, parent.numRecords);
        parent.runGenericBenchmark(streams, "Streams Yahoo Performance [records/latency/rec-sec/MB-sec counted]: ", latch);

    }
    // Note: these are also in the streams example package, eventually use 1 file
    private class JsonPOJOSerializer<T> implements Serializer<T> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        /**
         * Default constructor needed by Kafka
         */
        @SuppressWarnings("WeakerAccess")
        public JsonPOJOSerializer() {}

        @Override
        public byte[] serialize(final String topic, final T data) {
            if (data == null) {
                return null;
            }

            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (final Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }
    }

    // Note: these are also in the streams example package, eventuall use 1 file
    private class JsonPOJODeserializer<T> implements Deserializer<T> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        private Class<T> tClass;

        /**
         * Default constructor needed by Kafka
         */
        @SuppressWarnings("WeakerAccess")
        public JsonPOJODeserializer() {}

        @SuppressWarnings("unchecked")
        @Override
        public void configure(final Map<String, ?> props, final boolean isKey) {
            tClass = (Class<T>) props.get("JsonPOJOClass");
        }

        @Override
        public T deserialize(final String topic, final byte[] bytes) {
            if (bytes == null) {
                return null;
            }

            final T data;
            try {
                data = objectMapper.readValue(bytes, tClass);
            } catch (final Exception e) {
                throw new SerializationException(e);
            }

            return data;
        }
    }

    private KafkaStreams createYahooBenchmarkStreams(final Properties streamConfig, final String campaignsTopic, final String eventsTopic,
                                                     final CountDownLatch latch, final int numRecords) {
        final Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<ProjectedEvent> projectedEventSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", ProjectedEvent.class);
        projectedEventSerializer.configure(serdeProps, false);
        final Deserializer<ProjectedEvent> projectedEventDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", ProjectedEvent.class);
        projectedEventDeserializer.configure(serdeProps, false);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, ProjectedEvent> kEvents = builder.stream(eventsTopic,
                                                                       Consumed.with(Serdes.String(),
                                                                                     Serdes.serdeFrom(projectedEventSerializer, projectedEventDeserializer)));
        final KTable<String, String> kCampaigns = builder.table(campaignsTopic, Consumed.with(Serdes.String(), Serdes.String()));

        final KStream<String, ProjectedEvent> filteredEvents = kEvents
            // use peek to quick when last element is processed
            .peek((key, value) -> {
                parent.processedRecords++;
                if (parent.processedRecords % 1000000 == 0) {
                    System.out.println("Processed " + parent.processedRecords);
                }
                if (parent.processedRecords >= numRecords) {
                    latch.countDown();
                }
            })
            // only keep "view" events
            .filter((key, value) -> value.eventType.equals("view"))
            // select just a few of the columns
            .mapValues(value -> {
                final ProjectedEvent event = new ProjectedEvent();
                event.adID = value.adID;
                event.eventTime = value.eventTime;
                event.eventType = value.eventType;
                return event;
            });

        // deserialize the add ID and campaign ID from the stored value in Kafka
        final KTable<String, CampaignAd> deserCampaigns = kCampaigns.mapValues(value -> {
            final String[] parts = value.split(":");
            final CampaignAd cAdd = new CampaignAd();
            cAdd.adID = parts[0];
            cAdd.campaignID = parts[1];
            return cAdd;
        });

        // join the events with the campaigns
        final KStream<String, String> joined = filteredEvents.join(
            deserCampaigns,
            (value1, value2) -> value2.campaignID,
            Joined.with(Serdes.String(), Serdes.serdeFrom(projectedEventSerializer, projectedEventDeserializer), null)
        );

        // key by campaign rather than by ad as original
        final KStream<String, String> keyedByCampaign = joined
            .selectKey((key, value) -> value);

        // calculate windowed counts
        keyedByCampaign
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.of(Duration.ofMillis(10 * 1000)))
            .count(Materialized.as("time-windows"));

        return new KafkaStreams(builder.build(), streamConfig);
    }
}
