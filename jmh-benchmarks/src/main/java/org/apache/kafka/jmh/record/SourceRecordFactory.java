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
package org.apache.kafka.jmh.record;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * A simple factory that can be used to generate synthetic Debezium events.
 */
public class SourceRecordFactory {

    enum EventType {
        INSERT,
        UPDATE,
        DELETE
    }

    private final int maximumRecordFields;
    private final List<StructFieldProducer> fieldProducers = new ArrayList<>();

    public SourceRecordFactory(int maximumRecordFields) {
        this.maximumRecordFields = maximumRecordFields;
        this.fieldProducers.addAll(List.of(
            new NameStructFieldProducer(),
            new EmailStructFieldProducer(),
            new SetenceStructFieldProducer(),
            new VariableScaleDecimalStructFieldProducer()
        ));
    }

    public SourceRecord createSourceRecord(BaseRecordBatchBenchmark.DbzEvent dbzEvent) {
        return switch (dbzEvent) {
            case INSERT -> createInsertSourceRecord();
            case UPDATE -> createUpdateSourceRecord();
            case DELETE -> createDeleteSourceRecord();
            case RANDOM -> {
                final EventType eventType = EventType.values()[ThreadLocalRandom.current().nextInt(0, 3)];
                yield switch (eventType) {
                        case INSERT -> createInsertSourceRecord();
                        case UPDATE -> createUpdateSourceRecord();
                        case DELETE -> createDeleteSourceRecord();
                    };
            }
        };
    }

    public SourceRecord createInsertSourceRecord() {
        final SchemaAndValue key = createBasicKey();
        final SchemaAndValue source = createSourceInfoBlock();

        final List<StructFieldProducer> fieldProducers = createRandomRecordFields();
        final Map<String, SchemaAndValue> afterFields = createRecord(fieldProducers);

        final SchemaBuilder recordSchemaBuilder = SchemaBuilder.struct();
        afterFields.forEach((k, v) -> recordSchemaBuilder.field(k, v.schema()));
        final Schema recordSchema = recordSchemaBuilder.build();

        final Schema envelope = createEnvelopeSchema(source.schema(), recordSchema);

        final Struct after = new Struct(recordSchema);
        afterFields.forEach((k, v) -> after.put(k, v.value()));

        final Struct payload = new Struct(envelope);
        payload.put("before", null);
        payload.put("after", after);
        payload.put("source", source.value());
        payload.put("op", "c");
        payload.put("ts_ms", Instant.now().toEpochMilli());

        return new SourceRecord(
            Map.of(),
            Map.of(),
            "s1.table",
            key.schema(),
            key.value(),
            envelope,
            payload);
    }

    public SourceRecord createUpdateSourceRecord() {
        final SchemaAndValue key = createBasicKey();
        final SchemaAndValue source = createSourceInfoBlock();

        final List<StructFieldProducer> fieldProducers = createRandomRecordFields();
        final Map<String, SchemaAndValue> beforeFields = createRecord(fieldProducers);
        final Map<String, SchemaAndValue> afterFields = createRecord(fieldProducers);

        final SchemaBuilder recordSchemaBuilder = SchemaBuilder.struct();
        afterFields.forEach((k, v) -> recordSchemaBuilder.field(k, v.schema()));
        final Schema recordSchema = recordSchemaBuilder.build();

        final Schema envelope = createEnvelopeSchema(source.schema(), recordSchema);

        final Struct before = new Struct(recordSchema);
        beforeFields.forEach((k, v) -> before.put(k, v.value()));

        final Struct after = new Struct(recordSchema);
        afterFields.forEach((k, v) -> after.put(k, v.value()));

        final Struct payload = new Struct(envelope);
        payload.put("before", before);
        payload.put("after", after);
        payload.put("source", source.value());
        payload.put("op", "u");
        payload.put("ts_ms", Instant.now().toEpochMilli());

        return new SourceRecord(
            Map.of(),
            Map.of(),
            "s1.table",
            key.schema(),
            key.value(),
            envelope,
            payload);
    }

    public SourceRecord createDeleteSourceRecord() {
        final SchemaAndValue key = createBasicKey();
        final SchemaAndValue source = createSourceInfoBlock();

        final List<StructFieldProducer> fieldProducers = createRandomRecordFields();
        final Map<String, SchemaAndValue> beforeFields = createRecord(fieldProducers);

        final SchemaBuilder recordSchemaBuilder = SchemaBuilder.struct();
        beforeFields.forEach((k, v) -> recordSchemaBuilder.field(k, v.schema()));
        final Schema recordSchema = recordSchemaBuilder.build();

        final Schema envelope = createEnvelopeSchema(source.schema(), recordSchema);

        final Struct before = new Struct(recordSchema);
        beforeFields.forEach((k, v) -> before.put(k, v.value()));

        final Struct payload = new Struct(envelope);
        payload.put("before", before);
        payload.put("after", null);
        payload.put("source", source.value());
        payload.put("op", "c");
        payload.put("ts_ms", Instant.now().toEpochMilli());

        return new SourceRecord(
            Map.of(),
            Map.of(),
            "s1.table",
            key.schema(),
            key.value(),
            envelope,
            payload);
    }

    private Schema createEnvelopeSchema(Schema sourceSchema, Schema recordSchema) {
        return SchemaBuilder.struct()
            .name("s1.table.Envelope")
            .version(1)
            .field("before", recordSchema)
            .field("after", recordSchema)
            .field("source", sourceSchema)
            .field("op", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    }

    private SchemaAndValue createSourceInfoBlock() {
        final Schema schema = SchemaBuilder.struct()
            .name("io.debezium.connector.test.Source")
            .version(1)
            .field("version", Schema.STRING_SCHEMA)
            .field("connector", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .field("schema", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .field("filename", Schema.OPTIONAL_STRING_SCHEMA)
            .field("position", Schema.OPTIONAL_INT64_SCHEMA)
            .build();

        final Struct source = new Struct(schema);
        source.put("version", "1.2.3.Final");
        source.put("connector", "test");
        source.put("name", "dbz-deployment-prod");
        source.put("ts_ms", Instant.now().toEpochMilli());
        source.put("schema", "s1");
        source.put("table", "table1");
        source.put("filename", "transaction-log.001");
        source.put("position", 10000000000L);

        return new SchemaAndValue(schema, source);
    }

    private SchemaAndValue createBasicKey() {
        final Schema schema = SchemaBuilder.struct()
            .name("s1.table.Key")
            .version(1)
            .field("id", Schema.INT32_SCHEMA)
            .build();

        final Struct value = new Struct(schema);
        value.put("id", 1);

        return new SchemaAndValue(schema, value);
    }

    private Map<String, SchemaAndValue> createRecord(List<StructFieldProducer> fieldProducers) {
        final Map<String, SchemaAndValue> fields = new LinkedHashMap<>();
        fields.put("id", new SchemaAndValue(Schema.INT32_SCHEMA, 1));

        for (int i = 0; i < fieldProducers.size(); i++) {
            final StructFieldProducer fieldProducer = fieldProducers.get(i);
            final Schema fieldSchema = fieldProducer.fieldSchema();
            final String fieldName = fieldProducer.fieldName(i);
            final Object fieldValue = fieldProducer.fieldValue(false);
            fields.put(fieldName, new SchemaAndValue(fieldSchema, fieldValue));
        }

        return fields;
    }

    private List<StructFieldProducer> createRandomRecordFields() {
        int fieldCount = ThreadLocalRandom.current().nextInt(1, 10);

        List<StructFieldProducer> fields = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            fields.add(fieldProducers.get(ThreadLocalRandom.current().nextInt(1, fieldProducers.size())));
        }

        return fields;
    }

    interface StructFieldProducer {
        default boolean isPermittedInKey() {
            return true;
        }

        String fieldName(int fieldIndex);
        Schema fieldSchema();
        Object fieldValue(boolean key);
    }

    static class NameStructFieldProducer implements StructFieldProducer {
        private static final String[] FIRST_NAMES = {"Ava", "Noah", "Mia", "Liam", "Zoe", "Ethan", "Ivy", "Mason", "Nora", "Leo"};
        private static final String[] LAST_NAMES = {"Smith", "Johnson", "Brown", "Jones", "Miller", "Davis", "Garcia", "Rodriguez", "Martinez"};

        @Override
        public String fieldName(int fieldIndex) {
            return "name_" + fieldIndex;
        }

        @Override
        public Schema fieldSchema() {
            return Schema.STRING_SCHEMA;
        }

        @Override
        public Object fieldValue(boolean key) {
            final String firstName = FIRST_NAMES[ThreadLocalRandom.current().nextInt(FIRST_NAMES.length)];
            final String lastName = LAST_NAMES[ThreadLocalRandom.current().nextInt(LAST_NAMES.length)];
            return firstName + " " + lastName;
        }
    }

    static class EmailStructFieldProducer implements StructFieldProducer {
        private static final String[] DOMAINS = {"example.com", "mail.test", "corp.local", "acme.io"};

        @Override
        public String fieldName(int fieldIndex) {
            return "email_" + fieldIndex;
        }

        @Override
        public Schema fieldSchema() {
            return Schema.OPTIONAL_STRING_SCHEMA;
        }

        @Override
        public Object fieldValue(boolean key) {
            if (ThreadLocalRandom.current().nextInt(2) == 0) {
                return "user@" + DOMAINS[ThreadLocalRandom.current().nextInt(DOMAINS.length)];
            }
            return null;
        }
    }

    static class SetenceStructFieldProducer implements StructFieldProducer {
        private static final String[] WORDS = {
            "quick", "brown", "fox", "jumps", "over", "lazy", "dog", "robust", "scalable", "stream", "event", "payload"
        };

        @Override
        public boolean isPermittedInKey() {
            return false;
        }

        @Override
        public String fieldName(int fieldIndex) {
            return "sentence_" + fieldIndex;
        }

        @Override
        public Schema fieldSchema() {
            return Schema.OPTIONAL_STRING_SCHEMA;
        }

        @Override
        public Object fieldValue(boolean key) {
            final int wordCount = ThreadLocalRandom.current().nextInt(4, 10);

            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < wordCount; i++) {
                if (i > 0) {
                    sb.append(' ');
                }
                sb.append(WORDS[ThreadLocalRandom.current().nextInt(WORDS.length)]);
            }
            sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));
            sb.append('.');
            return sb.toString();
        }
    }

    static class VariableScaleDecimalStructFieldProducer implements StructFieldProducer {

        private static final Schema VSD_SCHEMA = SchemaBuilder.struct()
            .name("io.debezium.data.VariableScaleDecimal")
            .version(1)
            .doc("Variable scaled decimal")
            .field("scale", Schema.INT32_SCHEMA)
            .field("value", Schema.BYTES_SCHEMA)
            .build();


        @Override
        public String fieldName(int fieldIndex) {
            return "vsd_" + fieldIndex;
        }

        @Override
        public Schema fieldSchema() {
            return VSD_SCHEMA;
        }

        @Override
        public Object fieldValue(boolean key) {
            final int scale = ThreadLocalRandom.current().nextInt(0, 7);
            final int maxDigits = ThreadLocalRandom.current().nextInt(1, 39); // Oracle supports to 38 digits

            final StringBuilder sb = new StringBuilder();
            sb.append(ThreadLocalRandom.current().nextInt(1, 10)); // first digit should be between 1-9
            for (int i = 1; i < maxDigits; i++) {
                sb.append(ThreadLocalRandom.current().nextInt(0, 10)); // next digits 0-9
            }

            final BigDecimal decimal = new BigDecimal(new BigInteger(sb.toString()), scale);

            final Struct result = new Struct(fieldSchema());
            result.put("value", decimal.unscaledValue().toByteArray());
            result.put("scale", decimal.scale());
            return result;
        }
    }

}