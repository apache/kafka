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

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Values;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * This benchmark tests the performance of the {@link Values} data handling class.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ValuesBenchmark {

    private static final Schema MAP_INT_STRING_SCHEMA = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build();
    private static final Schema FLAT_STRUCT_SCHEMA = SchemaBuilder.struct()
            .field("field", Schema.INT32_SCHEMA)
            .build();
    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct()
            .field("first", Schema.INT32_SCHEMA)
            .field("second", Schema.STRING_SCHEMA)
            .field("array", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
            .field("map", MAP_INT_STRING_SCHEMA)
            .field("nested", FLAT_STRUCT_SCHEMA)
            .build();
    private static final SchemaAndValue[] TEST_VALUES = {
        SchemaAndValue.NULL,
        new SchemaAndValue(Schema.OPTIONAL_BOOLEAN_SCHEMA, null),
        new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true),
        new SchemaAndValue(Schema.BOOLEAN_SCHEMA, false),
        new SchemaAndValue(Schema.OPTIONAL_INT8_SCHEMA, null),
        new SchemaAndValue(Schema.INT8_SCHEMA, (byte) 0),
        new SchemaAndValue(Schema.INT8_SCHEMA, Byte.MAX_VALUE),
        new SchemaAndValue(Schema.OPTIONAL_INT16_SCHEMA, null),
        new SchemaAndValue(Schema.INT16_SCHEMA, (short) 0),
        new SchemaAndValue(Schema.INT16_SCHEMA, Short.MAX_VALUE),
        new SchemaAndValue(Schema.OPTIONAL_INT32_SCHEMA, null),
        new SchemaAndValue(Schema.INT32_SCHEMA, 0),
        new SchemaAndValue(Schema.INT32_SCHEMA, Integer.MAX_VALUE),
        new SchemaAndValue(Schema.OPTIONAL_INT64_SCHEMA, null),
        new SchemaAndValue(Schema.INT64_SCHEMA, (long) 0),
        new SchemaAndValue(Schema.INT64_SCHEMA, Long.MAX_VALUE),
        new SchemaAndValue(Schema.OPTIONAL_FLOAT32_SCHEMA, null),
        new SchemaAndValue(Schema.FLOAT32_SCHEMA, (float) 0),
        new SchemaAndValue(Schema.FLOAT32_SCHEMA, 0.1f),
        new SchemaAndValue(Schema.FLOAT64_SCHEMA, 1.1f),
        new SchemaAndValue(Schema.FLOAT32_SCHEMA, Float.MAX_VALUE),
        new SchemaAndValue(Schema.OPTIONAL_FLOAT64_SCHEMA, null),
        new SchemaAndValue(Schema.FLOAT64_SCHEMA, (double) 0),
        new SchemaAndValue(Schema.FLOAT64_SCHEMA, (double) 0.1f),
        new SchemaAndValue(Schema.FLOAT64_SCHEMA, (double) 1.1f),
        new SchemaAndValue(Schema.FLOAT64_SCHEMA, Double.MAX_VALUE),
        new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, null),
        new SchemaAndValue(Date.SCHEMA, "2019-08-23"),
        new SchemaAndValue(Time.SCHEMA, "14:34:54.346Z"),
        new SchemaAndValue(Timestamp.SCHEMA, "2019-08-23T14:34:54.346Z"),
        new SchemaAndValue(Schema.STRING_SCHEMA, ""),
        new SchemaAndValue(Schema.STRING_SCHEMA, "a-random-string"),
        new SchemaAndValue(Schema.STRING_SCHEMA, "[]"),
        new SchemaAndValue(Schema.STRING_SCHEMA, "[1, 2, 3]"),
        new SchemaAndValue(Schema.STRING_SCHEMA, "{}"),
        new SchemaAndValue(Schema.STRING_SCHEMA, "{\"1\": 2, \"3\": 4}"),
        new SchemaAndValue(SchemaBuilder.array(Schema.INT16_SCHEMA), new short[]{1, 2, 3}),
        new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA),
                Map.of("key", true)),
        new SchemaAndValue(STRUCT_SCHEMA, new Struct(STRUCT_SCHEMA)
                .put("first", 1)
                .put("second", "foo")
                .put("array", List.of(1, 2, 3))
                .put("map", Map.of(1, "value"))
                .put("nested", new Struct(FLAT_STRUCT_SCHEMA).put("field", 12))),
    };

    private SchemaAndValue[] convertToBooleanCases;
    private SchemaAndValue[] convertToByteCases;
    private SchemaAndValue[] convertToDateCases;
    private SchemaAndValue[] convertToDecimalCases;
    private SchemaAndValue[] convertToDoubleCases;
    private SchemaAndValue[] convertToFloatCases;
    private SchemaAndValue[] convertToShortCases;
    private SchemaAndValue[] convertToListCases;
    private SchemaAndValue[] convertToMapCases;
    private SchemaAndValue[] convertToLongCases;
    private SchemaAndValue[] convertToIntegerCases;
    private SchemaAndValue[] convertToStructCases;
    private SchemaAndValue[] convertToTimeCases;
    private SchemaAndValue[] convertToTimestampCases;
    private SchemaAndValue[] convertToStringCases;
    private String[] parseStringCases;

    private SchemaAndValue[] successfulCases(BiFunction<Schema, Object, Object> fn) {
        List<SchemaAndValue> successful = new ArrayList<>();
        for (SchemaAndValue testCase : TEST_VALUES) {
            try {
                fn.apply(testCase.schema(), testCase.value());
                successful.add(testCase);
            } catch (Throwable ignored) {
            }
        }
        return successful.toArray(new SchemaAndValue[]{});
    }

    private String[] casesToString(Function<String, Object> fn) {
        List<String> successful = new ArrayList<>();
        for (SchemaAndValue testCase : TEST_VALUES) {
            String v = String.valueOf(testCase.value());
            try {
                fn.apply(v);
                successful.add(v);
            } catch (Throwable ignored) {
            }
        }
        return successful.toArray(new String[]{});
    }

    @Setup
    public void setup() {
        convertToBooleanCases = successfulCases(Values::convertToBoolean);
        convertToByteCases = successfulCases(Values::convertToByte);
        convertToDateCases = successfulCases(Values::convertToDate);
        convertToDecimalCases = successfulCases((schema, object) -> Values.convertToDecimal(schema, object, 1));
        convertToDoubleCases = successfulCases(Values::convertToDouble);
        convertToFloatCases = successfulCases(Values::convertToFloat);
        convertToShortCases = successfulCases(Values::convertToShort);
        convertToListCases = successfulCases(Values::convertToList);
        convertToMapCases = successfulCases(Values::convertToMap);
        convertToLongCases = successfulCases(Values::convertToLong);
        convertToIntegerCases = successfulCases(Values::convertToInteger);
        convertToStructCases = successfulCases(Values::convertToStruct);
        convertToTimeCases = successfulCases(Values::convertToTime);
        convertToTimestampCases = successfulCases(Values::convertToTimestamp);
        convertToStringCases = successfulCases(Values::convertToString);
        parseStringCases = casesToString(Values::parseString);
    }

    @Benchmark
    public void testConvertToBoolean(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToBooleanCases) {
            blackhole.consume(Values.convertToBoolean(testCase.schema(), testCase.value()));
        }
    }

    @Benchmark
    public void testConvertToByte(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToByteCases) {
            blackhole.consume(Values.convertToByte(testCase.schema(), testCase.value()));
        }
    }

    @Benchmark
    public void testConvertToDate(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToDateCases) {
            blackhole.consume(Values.convertToDate(testCase.schema(), testCase.value()));
        }
    }

    @Benchmark
    public void testConvertToDecimal(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToDecimalCases) {
            blackhole.consume(Values.convertToDecimal(testCase.schema(), testCase.value(), 1));
        }
    }

    @Benchmark
    public void testConvertToDouble(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToDoubleCases) {
            blackhole.consume(Values.convertToDouble(testCase.schema(), testCase.value()));
        }
    }

    @Benchmark
    public void testConvertToFloat(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToFloatCases) {
            blackhole.consume(Values.convertToFloat(testCase.schema(), testCase.value()));
        }
    }

    @Benchmark
    public void testConvertToShort(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToShortCases) {
            blackhole.consume(Values.convertToShort(testCase.schema(), testCase.value()));
        }
    }

    @Benchmark
    public void testConvertToList(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToListCases) {
            blackhole.consume(Values.convertToList(testCase.schema(), testCase.value()));
        }
    }

    @Benchmark
    public void testConvertToMap(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToMapCases) {
            blackhole.consume(Values.convertToMap(testCase.schema(), testCase.value()));
        }
    }

    @Benchmark
    public void testConvertToLong(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToLongCases) {
            blackhole.consume(Values.convertToLong(testCase.schema(), testCase.value()));
        }
    }

    @Benchmark
    public void testConvertToInteger(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToIntegerCases) {
            blackhole.consume(Values.convertToInteger(testCase.schema(), testCase.value()));
        }
    }

    @Benchmark
    public void testConvertToStruct(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToStructCases) {
            blackhole.consume(Values.convertToStruct(testCase.schema(), testCase.value()));
        }
    }

    @Benchmark
    public void testConvertToTime(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToTimeCases) {
            blackhole.consume(Values.convertToTime(testCase.schema(), testCase.value()));
        }
    }

    @Benchmark
    public void testConvertToTimestamp(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToTimestampCases) {
            blackhole.consume(Values.convertToTimestamp(testCase.schema(), testCase.value()));
        }
    }

    @Benchmark
    public void testConvertToString(Blackhole blackhole) {
        for (SchemaAndValue testCase : convertToStringCases) {
            blackhole.consume(Values.convertToString(testCase.schema(), testCase.value()));
        }
    }

    @Benchmark
    public void testInferSchema(Blackhole blackhole) {
        for (SchemaAndValue testCase : TEST_VALUES) {
            blackhole.consume(Values.inferSchema(testCase.value()));
        }
    }

    @Benchmark
    public void testParseString(Blackhole blackhole) {
        for (String testCase : parseStringCases) {
            blackhole.consume(Values.parseString(testCase));
        }
    }
}
