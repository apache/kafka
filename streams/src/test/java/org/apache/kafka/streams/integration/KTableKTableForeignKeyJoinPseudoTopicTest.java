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
package org.apache.kafka.streams.integration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.utils.UniqueTopicSerdeScope;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class KTableKTableForeignKeyJoinPseudoTopicTest {

    private static final String LEFT_TABLE = "left_table";
    private static final String RIGHT_TABLE = "right_table";
    private static final String OUTPUT = "output-topic";
    private static final String REJOIN_OUTPUT = "rejoin-output-topic";
    private final Properties streamsConfig = mkProperties(mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-ktable-joinOnForeignKey"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "asdf:0000"),
        mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
    ));


    private static Collection<Object[]> buildParameters(final List<?>... argOptions) {
        List<Object[]> result = new LinkedList<>();
        result.add(new Object[0]);

        for (final List<?> argOption : argOptions) {
            result = times(result, argOption);
        }

        return result;
    }

    private static List<Object[]> times(final List<Object[]> left, final List<?> right) {
        final List<Object[]> result = new LinkedList<>();
        for (final Object[] args : left) {
            for (final Object rightElem : right) {
                final Object[] resArgs = new Object[args.length + 1];
                System.arraycopy(args, 0, resArgs, 0, args.length);
                resArgs[args.length] = rightElem;
                result.add(resArgs);
            }
        }
        return result;
    }


    @Test
    public void shouldUseExpectedTopicsWithSerde() {
        final UniqueTopicSerdeScope serdeScope = new UniqueTopicSerdeScope();
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> left = builder.table(
            LEFT_TABLE,
            Consumed.with(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true),
                          serdeScope.decorateSerde(Serdes.String(), streamsConfig, false))
        );
        final KTable<String, String> right = builder.table(
            RIGHT_TABLE,
            Consumed.with(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true),
                          serdeScope.decorateSerde(Serdes.String(), streamsConfig, false))
        );

        left.join(
            right,
            value -> value.split("\\|")[1],
            (value1, value2) -> "(" + value1 + "," + value2 + ")",
            Materialized.with(null, serdeScope.decorateSerde(Serdes.String(), streamsConfig, false)
            ))
            .toStream()
            .to(OUTPUT);


        final Topology topology = builder.build(streamsConfig);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> leftInput = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> rightInput = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            leftInput.pipeInput("lhs1", "lhsValue1|rhs1");
            rightInput.pipeInput("rhs1", "rhsValue1");
        }
        // verifying primarily that no extra pseudo-topics were used, but it's nice to also verify the rest of the
        // topics our serdes serialize data for
        assertThat(serdeScope.registeredTopics(), is(mkSet(
            // expected pseudo-topics
            "KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000006-topic-fk--key",
            "KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000006-topic-pk--key",
            "KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000006-topic-vh--value",
            // internal topics
            "ktable-ktable-joinOnForeignKey-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000006-topic--key",
            "ktable-ktable-joinOnForeignKey-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-0000000014-topic--key",
            "ktable-ktable-joinOnForeignKey-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-0000000014-topic--value",
            "ktable-ktable-joinOnForeignKey-left_table-STATE-STORE-0000000000-changelog--key",
            "ktable-ktable-joinOnForeignKey-left_table-STATE-STORE-0000000000-changelog--value",
            "ktable-ktable-joinOnForeignKey-right_table-STATE-STORE-0000000003-changelog--key",
            "ktable-ktable-joinOnForeignKey-right_table-STATE-STORE-0000000003-changelog--value",
            // output topics
            "output-topic--key",
            "output-topic--value"
        )));
    }

}
