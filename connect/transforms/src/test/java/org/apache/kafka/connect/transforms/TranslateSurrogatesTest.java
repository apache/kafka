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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TranslatesSurrogatesTest {
	private final TranslateSurrogates<SourceRecord> xformKey = new TranslateSurrogates.Key<>();
	private final TranslateSurrogates<SourceRecord> xformValue = new TranslateSurrogates.Value<>();

	@AfterEach
	public void teardown() {
		xformKey.close();
		xformValue.close();
	}	
	@Test
	public void testConfigModeEmpty() {
		assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(TranslateSurrogates.MODE_CONFIG, "")));
	}

	@Test
	public void testConfigModeInvalid() {
		assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(TranslateSurrogates.MODE_CONFIG, "abc")));
	}

	@Test
	public void testConfigReplacementInvalid() {
		assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(TranslateSurrogates.REPLACEMENT_CONFIG, "\uD84D\uDE3A")));
	}

	@Test	
	public void testConfigDefaults() {
		xformKey.configure(Collections.emptyMap());
		assertEquals("%3F",xformKey.translationFunc.apply(63)); //codepoint 63 is '?'
	}

	@Test	
	public void testConfigJavaEncodeMode() {
		xformKey.configure(Collections.singletonMap(TranslateSurrogates.MODE_CONFIG, "java-encode"));
		assertEquals("\\U003F",xformKey.translationFunc.apply(63)); //codepoint 63 is '?'
	}

	@Test	
	public void testConfigReplaceModeDefault() {
		xformKey.configure(Collections.singletonMap(TranslateSurrogates.MODE_CONFIG, "replace"));
		assertEquals(TranslateSurrogates.UNICODE_REPLACEMENT_STRING, xformKey.translationFunc.apply(63)); //codepoint 63 is '?'
	}

	@Test	
	public void testConfigReplaceModeString() {
		Map<String, String> config = new HashMap<>();
		config.put(TranslateSurrogates.MODE_CONFIG, "replace");
		config.put(TranslateSurrogates.REPLACEMENT_CONFIG, "somestr");
		xformKey.configure(config);
		assertEquals("somestr", xformKey.translationFunc.apply(63)); //codepoint 63 is '?'
	}

	@Test	
	public void testSurrogateDefaultMode() {
		xformKey.configure(Collections.emptyMap());
		assertEquals("%F0%A3%98%BA",xformKey.translationFunc.apply(144954)); //codepoint 144954 is '𣘺' a surrogate pair
	}

	@Test	
	public void testSurrogateJavaEncodeMode() {
		xformKey.configure(Collections.singletonMap(TranslateSurrogates.MODE_CONFIG, "java-encode"));
		assertEquals("\\UD84D\\UDE3A",xformKey.translationFunc.apply(144954)); //codepoint 144954 is '𣘺' a surrogate pair
	}

	@Test	
	public void testSurrogateReplacementMode() {
		Map<String, String> config = new HashMap<>();
		config.put(TranslateSurrogates.MODE_CONFIG, "replace");
		config.put(TranslateSurrogates.REPLACEMENT_CONFIG, "somestr");
		xformKey.configure(config);
		assertEquals("somestr", xformKey.translationFunc.apply(144954)); //codepoint 144954 is '𣘺' a surrogate pair
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSchemalessNoFields() {
		xformValue.configure(Collections.emptyMap());
		SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
				null, "abc𣘺"));

		assertEquals("abc%F0%A3%98%BA",transformed.value());

		Map<String, Object> value = new HashMap<>();
		value.put("field1", 123);
		value.put("field2", "abc");
		value.put("field3", "abc𣘺");
		value.put("field4", "abc𣘺abc");
		transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
				null, value));

		Map<String, Object> transformedValue = ((Map<String,Object>)transformed.value());

		assertEquals(123,transformedValue.get("field1"));
		assertEquals("abc",transformedValue.get("field2"));
		assertEquals("abc%F0%A3%98%BA",transformedValue.get("field3"));
		assertEquals("abc%F0%A3%98%BAabc",transformedValue.get("field4"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSchemalessFields() {
		xformValue.configure(Collections.singletonMap(TranslateSurrogates.FIELDS_CONFIG, "field3"));

		Map<String, Object> value = new HashMap<>();
		value.put("field1", 123);
		value.put("field2", "abc");
		value.put("field3", "abc𣘺");
		value.put("field4", "abc𣘺abc");
		SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
				null, value));

		Map<String, Object> transformedValue = ((Map<String,Object>)transformed.value());

		assertEquals(123,transformedValue.get("field1"));
		assertEquals("abc",transformedValue.get("field2"));
		assertEquals("abc%F0%A3%98%BA",transformedValue.get("field3"));
		assertEquals("abc𣘺abc",transformedValue.get("field4"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testArray() {
		xformValue.configure(Collections.emptyMap());

		List<String> stringList = new ArrayList<>();
		stringList.add("abc");
		stringList.add("abc𣘺");
		stringList.add("abc𣘺abc");

		SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
				null, stringList));
		
		List<Object> transformedList = (List<Object>)transformed.value();

		assertEquals("abc",transformedList.get(0));
		assertEquals("abc%F0%A3%98%BA",transformedList.get(1));
		assertEquals("abc%F0%A3%98%BAabc",transformedList.get(2));
	}
	
	@Test
	public void testUnsupportedDataTypeException() {
		xformValue.configure(Collections.emptyMap());
		assertThrows(DataException.class, () -> {
			@SuppressWarnings("unused")
			SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
					null, new Date()));
		});
	}
	
	public void testSchema() {
		xformValue.configure(Collections.emptyMap());
		
		SchemaBuilder nestedBuilder = SchemaBuilder.struct();
		nestedBuilder.field("nestedstringfield", Schema.OPTIONAL_STRING_SCHEMA);
		Schema nestedSchema = nestedBuilder.build();
		
		SchemaBuilder builder = SchemaBuilder.struct();
		builder.field("stringfield", Schema.STRING_SCHEMA);
		builder.field("bytesfield", Schema.BYTES_SCHEMA);
		builder.field("nestedstruct", nestedSchema);
		Schema valueSchema = builder.build();
		
		Struct nestedValue = new Struct(nestedSchema);
		nestedValue.put("nestedstringfield", "abc𣘺abc");
		
		Struct recordValue = new Struct(valueSchema);
		recordValue.put("stringfield", "abc𣘺");
		recordValue.put("bytesfield", "abc𣘺abc".getBytes(StandardCharsets.UTF_8));
		recordValue.put("nestedStruct", nestedValue);
		 
		SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
				valueSchema, recordValue));
		
		Struct transformedValue = (Struct) transformed.value();

		assertEquals("abc𣘺abc".getBytes(StandardCharsets.UTF_8),transformedValue.get("bytesfield"));
		assertEquals("abc%F0%A3%98%BA",transformedValue.get("stringfield"));
		assertEquals("abc%F0%A3%98%BAabc",((Struct)transformedValue.get("nestedstruct")).get("nestedstringfield"));
		
		xformValue.configure(Collections.singletonMap(TranslateSurrogates.FIELDS_CONFIG, "nestedstringfield"));
		
		transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
				valueSchema, recordValue));
		transformedValue = (Struct) transformed.value();
		assertEquals("abc𣘺abc".getBytes(StandardCharsets.UTF_8),transformedValue.get("bytesfield"));
		assertEquals("abc𣘺",transformedValue.get("stringfield"));
		assertEquals("abc%F0%A3%98%BAabc",((Struct)transformedValue.get("nestedstruct")).get("nestedstringfield"));		
	}

}
