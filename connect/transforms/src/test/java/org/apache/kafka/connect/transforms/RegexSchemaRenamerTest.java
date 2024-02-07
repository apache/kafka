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
package org.apache.kafka.connect.transforms;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;



public class RegexSchemaRenamerTest {
    RegexSchemaRenamer<SourceRecord> rr = new RegexSchemaRenamer<>();


    @Test
    public void testApplyWithStruct() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(RegexSchemaRenamer.REGEX_CONFIG, ".*\\.([^.]*)\\.(Value|Key)");
        config.put(RegexSchemaRenamer.REPLACEMENT_CONFIG, "schema.$1.$2");
        rr.configure(config);
        
        
        SourceRecord cr = structRecord();
        SourceRecord newSchemaRecord = rr.apply(cr);
        
        assertEquals(newSchemaRecord.keySchema().name(), "schema.TABLE.Key");
        assertEquals(newSchemaRecord.valueSchema().name(), "schema.TABLE.Value");
    }
    
    @Test
    public void testApplyWithNullKeySchema() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(RegexSchemaRenamer.REGEX_CONFIG, ".*\\.([^.]*)\\.(Value|Key)");
        config.put(RegexSchemaRenamer.REPLACEMENT_CONFIG, "schema.$1.$2");
        rr.configure(config);
        
        
        SourceRecord cr = nullSchemaKeyRecord();
        SourceRecord newSchemaRecord = rr.apply(cr);
        
        assertNull(newSchemaRecord.keySchema());
        assertEquals(newSchemaRecord.valueSchema().name(), "schema.TABLE.Value");
    }
    
    @Test
    public void testApplyWithSimpleKeySchema() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(RegexSchemaRenamer.REGEX_CONFIG, ".*\\.([^.]*)\\.(Value|Key)");
        config.put(RegexSchemaRenamer.REPLACEMENT_CONFIG, "schema.$1.$2");
        rr.configure(config);
        
        
        SourceRecord cr = simpleScheamKeyRecord();
        SourceRecord newSchemaRecord = rr.apply(cr);
        
        assertEquals(newSchemaRecord.keySchema(), Schema.INT64_SCHEMA);
        assertEquals(newSchemaRecord.valueSchema().name(), "schema.TABLE.Value");
    }

    private SourceRecord nullSchemaKeyRecord() {
        Schema keySchema = null;
        Long key = Long.valueOf(0);
        Schema valueSchema = SchemaBuilder.struct().name("database.com.schema.TABLE.Value").field("name", SchemaBuilder.OPTIONAL_STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema);
        value.put("name", "name value");
        
        Map<String, Integer> partition = new HashMap<>();
        Map<String, Integer> offset = new HashMap<>();
        
        return new SourceRecord(partition, offset, "my.topic", 0, keySchema, key, valueSchema, value, Long.valueOf(123456L));
    }

    private SourceRecord simpleScheamKeyRecord() {
        Schema keySchema = SchemaBuilder.INT64_SCHEMA;
        Long key = Long.valueOf(0);
        Schema valueSchema = SchemaBuilder.struct().name("database.com.schema.TABLE.Value").field("name", SchemaBuilder.OPTIONAL_STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema);
        value.put("name", "name value");
        
        Map<String, Integer> partition = new HashMap<>();
        Map<String, Integer> offset = new HashMap<>();
        
        return new SourceRecord(partition, offset, "my.topic", 0, keySchema, key, valueSchema, value, Long.valueOf(123456L));
    }
    
    private SourceRecord structRecord() {
        Schema keySchema = SchemaBuilder.struct().name("database.com.schema.TABLE.Key").field("id", SchemaBuilder.INT64_SCHEMA).build();
        Struct key = new Struct(keySchema);
        key.put("id", Long.valueOf(0));
        Schema valueSchema = SchemaBuilder.struct().name("database.com.schema.TABLE.Value").field("name", SchemaBuilder.OPTIONAL_STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema);
        value.put("name", "name value");
        
        Map<String, Integer> partition = new HashMap<>();
        Map<String, Integer> offset = new HashMap<>();
        
        return new SourceRecord(partition, offset, "my.topic", 0, keySchema, key, valueSchema, value, Long.valueOf(123456L));
    }
}
