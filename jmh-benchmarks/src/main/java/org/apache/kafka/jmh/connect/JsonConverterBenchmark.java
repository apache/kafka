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
package org.apache.kafka.jmh.connect;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class JsonConverterBenchmark {
    private static final String TOPIC = "topic";

    private JsonConverter converter;

    @Param({"true", "false"})
    private boolean blackbirdModule;

    @State(Scope.Benchmark)
    public static class Data {

        final org.apache.kafka.connect.data.Schema envelopeSchema = buildEnvelopeSchema();

        final Struct envelopeStruct = new Struct(envelopeSchema)
                .put("before", buildValueStruct())
                .put("after", buildValueStruct())
                .put("source", buildSourceStruct())
                .put("op", "u")
                .put("ts_ms", 1638362438000L)
                .put("transaction", buildTransactionStruct());


        public String structJson = "{\n" +
                "  \"schema\": {\n" +
                "  \"type\": \"struct\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"type\": \"struct\",\n" +
                "      \"fields\": [\n" +
                "        {\n" +
                "          \"type\": \"int32\",\n" +
                "          \"optional\": false,\n" +
                "          \"field\": \"id\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"aircraft\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"airline\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"int32\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"passengers\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"airport\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"flight\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"metar\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"double\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"flight_distance\"\n" +
                "        }\n" +
                "      ],\n" +
                "      \"optional\": true,\n" +
                "      \"name\": \"dbserver1.public.aviation.Value\",\n" +
                "      \"field\": \"before\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\": \"struct\",\n" +
                "      \"fields\": [\n" +
                "        {\n" +
                "          \"type\": \"int32\",\n" +
                "          \"optional\": false,\n" +
                "          \"field\": \"id\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"aircraft\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"airline\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"int32\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"passengers\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"airport\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"flight\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"metar\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"double\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"flight_distance\"\n" +
                "        }\n" +
                "      ],\n" +
                "      \"optional\": true,\n" +
                "      \"name\": \"dbserver1.public.aviation.Value\",\n" +
                "      \"field\": \"after\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\": \"struct\",\n" +
                "      \"fields\": [\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": false,\n" +
                "          \"field\": \"version\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": false,\n" +
                "          \"field\": \"connector\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": false,\n" +
                "          \"field\": \"name\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"int64\",\n" +
                "          \"optional\": false,\n" +
                "          \"field\": \"ts_ms\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": true,\n" +
                "          \"name\": \"io.debezium.data.Enum\",\n" +
                "          \"version\": 1,\n" +
                "          \"parameters\": {\n" +
                "            \"allowed\": \"true,last,false,incremental\"\n" +
                "          },\n" +
                "          \"default\": \"false\",\n" +
                "          \"field\": \"snapshot\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": false,\n" +
                "          \"field\": \"db\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"sequence\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": false,\n" +
                "          \"field\": \"schema\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": false,\n" +
                "          \"field\": \"table\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"int64\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"txId\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"int64\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"lsn\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"int64\",\n" +
                "          \"optional\": true,\n" +
                "          \"field\": \"xmin\"\n" +
                "        }\n" +
                "      ],\n" +
                "      \"optional\": false,\n" +
                "      \"name\": \"io.debezium.connector.postgresql.Source\",\n" +
                "      \"field\": \"source\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\": \"string\",\n" +
                "      \"optional\": false,\n" +
                "      \"field\": \"op\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\": \"int64\",\n" +
                "      \"optional\": true,\n" +
                "      \"field\": \"ts_ms\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\": \"struct\",\n" +
                "      \"fields\": [\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"optional\": false,\n" +
                "          \"field\": \"id\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"int64\",\n" +
                "          \"optional\": false,\n" +
                "          \"field\": \"total_order\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"int64\",\n" +
                "          \"optional\": false,\n" +
                "          \"field\": \"data_collection_order\"\n" +
                "        }\n" +
                "      ],\n" +
                "      \"optional\": true,\n" +
                "      \"name\": \"event.block\",\n" +
                "      \"version\": 1,\n" +
                "      \"field\": \"transaction\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"optional\": false,\n" +
                "  \"name\": \"dbserver1.public.aviation.Envelope\",\n" +
                "  \"version\": 1\n" +
                "},\n" +
                "  \"payload\": {\n" +
                "    \"before\": null,\n" +
                "    \"after\": {\n" +
                "      \"id\": 941445,\n" +
                "      \"aircraft\": \"Mi-8\",\n" +
                "      \"airline\": \"LOT Polish Airlines\",\n" +
                "      \"passengers\": 232,\n" +
                "      \"airport\": \"ZBAR\",\n" +
                "      \"flight\": \"MH9445\",\n" +
                "      \"metar\": \"METAR: GOOY 251100Z 24008KT 9999 BKN011 27/22 Q1014\",\n" +
                "      \"flight_distance\": 1697.4732487340466\n" +
                "    },\n" +
                "    \"source\": {\n" +
                "      \"version\": \"2.5.0-SNAPSHOT\",\n" +
                "      \"connector\": \"postgresql\",\n" +
                "      \"name\": \"dbserver1\",\n" +
                "      \"ts_ms\": 1702288693179,\n" +
                "      \"snapshot\": \"last_in_data_collection\",\n" +
                "      \"db\": \"postgres\",\n" +
                "      \"sequence\": \"[null,\\\"2195663032\\\"]\",\n" +
                "      \"schema\": \"public\",\n" +
                "      \"table\": \"aviation\",\n" +
                "      \"txId\": 30881,\n" +
                "      \"lsn\": 2195663032,\n" +
                "      \"xmin\": null\n" +
                "    },\n" +
                "    \"op\": \"r\",\n" +
                "    \"ts_ms\": 1702288722694,\n" +
                "    \"transaction\": null\n" +
                "  }\n" +
                "}";

        private static org.apache.kafka.connect.data.Schema buildEnvelopeSchema() {

            return SchemaBuilder.struct()
                    .name("dbserver1.public.aviation.Envelope")
                    .version(1)
                    .field("before", buildValueSchema())
                    .field("after", buildValueSchema())
                    .field("source", buildSourceSchema())
                    .field("op", SchemaBuilder.STRING_SCHEMA)
                    .field("ts_ms", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
                    .field("transaction", buildTransactionSchema())
                    .build();
        }
        private static org.apache.kafka.connect.data.Schema buildValueSchema() {

            return SchemaBuilder.struct()
                    .name("dbserver1.public.aviation.Value")
                    .version(1)
                    .field("id", SchemaBuilder.INT32_SCHEMA)
                    .field("aircraft", SchemaBuilder.STRING_SCHEMA)
                    .field("airline", SchemaBuilder.STRING_SCHEMA)
                    .field("passengers", SchemaBuilder.INT32_SCHEMA)
                    .field("airport", SchemaBuilder.STRING_SCHEMA)
                    .field("flight", SchemaBuilder.STRING_SCHEMA)
                    .field("metar", SchemaBuilder.STRING_SCHEMA)
                    .field("flight_distance", SchemaBuilder.FLOAT64_SCHEMA)
                    .build();
        }

        private static Struct buildValueStruct() {

            Struct valueStruct = new Struct(buildValueSchema());

            valueStruct.put("id", 941445);
            valueStruct.put("aircraft", "Mi-8");
            valueStruct.put("airline", "LOT Polish Airlines");
            valueStruct.put("passengers", 232);
            valueStruct.put("airport", "ZBAR");
            valueStruct.put("flight", "MH9445");
            valueStruct.put("metar", "METAR: GOOY 251100Z 24008KT 9999 BKN011 27/22 Q1014");
            valueStruct.put("flight_distance", 1697.4732487340466);

            return valueStruct;
        }

        private static org.apache.kafka.connect.data.Schema buildSourceSchema() {

            return SchemaBuilder.struct()
                    .name("io.debezium.connector.postgresql.Source")
                    .version(1)
                    .field("version", SchemaBuilder.STRING_SCHEMA)
                    .field("connector", SchemaBuilder.STRING_SCHEMA)
                    .field("name", SchemaBuilder.STRING_SCHEMA)
                    .field("ts_ms", SchemaBuilder.INT64_SCHEMA)
                    .field("snapshot", SchemaBuilder.STRING_SCHEMA)
                    .field("db", SchemaBuilder.STRING_SCHEMA)
                    .field("sequence", SchemaBuilder.STRING_SCHEMA)
                    .field("schema", SchemaBuilder.STRING_SCHEMA)
                    .field("table", SchemaBuilder.STRING_SCHEMA)
                    .field("txId", SchemaBuilder.INT64_SCHEMA)
                    .field("lsn", SchemaBuilder.INT64_SCHEMA)
                    .field("xmin", SchemaBuilder.INT64_SCHEMA)
                    .build();
        }

        private static Struct buildSourceStruct() {

            Struct sourceStruct = new Struct(buildSourceSchema());

            sourceStruct.put("version", "2.5.0-SNAPSHOT");
            sourceStruct.put("connector", "postgresql");
            sourceStruct.put("name", "dbserver1");
            sourceStruct.put("ts_ms", 1702288693179L);
            sourceStruct.put("snapshot", "last_in_data_collection");
            sourceStruct.put("db", "postgres");
            sourceStruct.put("sequence", "[null,\"2195663032\"]");
            sourceStruct.put("schema", "public");
            sourceStruct.put("table", "aviation");
            sourceStruct.put("txId", 30881L);
            sourceStruct.put("lsn", 2195663032L);
            sourceStruct.put("xmin", 30881L);

            return sourceStruct;
        }

        private static org.apache.kafka.connect.data.Schema buildTransactionSchema() {

            return SchemaBuilder.struct()
                    .name("event.block")
                    .version(1)
                    .field("id", SchemaBuilder.STRING_SCHEMA)
                    .field("total_order", SchemaBuilder.INT64_SCHEMA)
                    .field("data_collection_order", SchemaBuilder.INT64_SCHEMA)
                    .build();
        }

        private static Struct buildTransactionStruct() {

            Struct transactionStruct = new Struct(buildTransactionSchema());


            transactionStruct.put("id", "transaction_id");
            transactionStruct.put("total_order", 1000L);
            transactionStruct.put("data_collection_order", 10000L);

            return transactionStruct;
        }
    }


    @Setup(Level.Trial)
    public void setup(BenchmarkParams params)  {

        converter = new JsonConverter(Boolean.parseBoolean(params.getParam("blackbirdModule")));
        converter.configure(Collections.emptyMap(), false);
    }

    @Benchmark
    public void deserialize(Blackhole blackhole, Data data) {

        blackhole.consume(converter.toConnectData(TOPIC, data.structJson.getBytes(Charset.defaultCharset())));
    }

    @Benchmark
    public void serialize(Blackhole blackhole, Data data) {

        blackhole.consume(converter.fromConnectData(TOPIC, data.envelopeSchema, data.envelopeStruct));
    }
}
