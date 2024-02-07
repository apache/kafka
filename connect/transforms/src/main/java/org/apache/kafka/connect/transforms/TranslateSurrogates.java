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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class TranslateSurrogates<R extends ConnectRecord<R>> implements Transformation<R> {

	public static final String OVERVIEW_DOC =
			"Translate UTF16 surrogate pairs in the message to their corresponding UTF8 URL encoding, "
					+ "or to the default replacement character U+FFFD, "
					+ "or a configured replacement string, "
					+ "or with Java notated UTF16."
					+ "<p/>For string fields only, optionally specified at any arbitrary depth, in structs, arrays, or maps."
					+ "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
					+ "or value (<code>" + Value.class.getName() + "</code>).";

	public static final String FIELDS_CONFIG = "fields";
	public static final String MODE_CONFIG = "mode";
	public static final String REPLACEMENT_CONFIG = "replacement";

	public static final String MODE_URLENCODE = "url-encode";
	public static final String MODE_JAVAENCODE = "java-encode";
	public static final String MODE_REPLACE = "replace";

	public static final String UNICODE_REPLACEMENT_STRING = "\uFFFD";  

	public static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(MODE_CONFIG, ConfigDef.Type.STRING, MODE_URLENCODE, new ConfigDef.Validator() {
				@Override
				public void ensureValid(String name, Object value) {
					String input = (String) value;
					if (! input.equalsIgnoreCase(MODE_URLENCODE) && ! input.equalsIgnoreCase(MODE_JAVAENCODE) && ! input.equalsIgnoreCase(MODE_REPLACE))
						throw new ConfigException("Translation mode for the transform must be one of url-encode (default), java-encode, or replace");
				}
			}, ConfigDef.Importance.HIGH, "Translation mode for the transform. This must be one of url-encode (default), "
							+ "java-encode, or replace")
			.define(FIELDS_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(),
					ConfigDef.Importance.MEDIUM, "Names of fields to translate surrogates in. If not specified, entire "
							+ "message is parsed for strings to translate")
			.define(REPLACEMENT_CONFIG, ConfigDef.Type.STRING, UNICODE_REPLACEMENT_STRING, new ConfigDef.Validator() {
				@Override
				public void ensureValid(String name, Object value) {
					String input = (String) value;
					for (int index=0; index<input.length(); index++) {
						if (! Character.isBmpCodePoint(input.codePointAt(index)))
							throw new ConfigException("Replacement string itself cannot have surrogates");
					}
				}
			}, ConfigDef.Importance.LOW, "Custom replacement string, that will be applied to surrogates found in all"
					+ " 'fields' values (non-empty string values only). Works only with 'replace' mode.");

	public static final String PURPOSE = "translate surrogates";
	public static final String SPEC_CONFIG = "spec";

	Function<Integer, String> translationFunc = codePoint -> {
		char[] chars = Character.toChars(codePoint);
		try {
			return URLEncoder.encode(new String(chars), StandardCharsets.UTF_8.toString());
		} catch (UnsupportedEncodingException e) {
			throw new ConfigException("Invalid charset used for URL encoding");
		}
	};

	private Set<String> translateFields;
	private String replacement;
	private String mode;

	@Override
	public void configure(Map<String, ?> props) {
		final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
		mode = config.getString(MODE_CONFIG);
		translateFields = new HashSet<>(config.getList(FIELDS_CONFIG));
		replacement = config.getString(REPLACEMENT_CONFIG);

		if (mode.contentEquals(MODE_JAVAENCODE)) {
			translationFunc = codePoint -> {
				char[] chars = Character.toChars(codePoint);
				StringBuilder replacementSb = new StringBuilder();
				for (int i=0; i<chars.length; i++) {
					replacementSb.append(String.format("\\U%04X", (int)chars[i]));
				}
				return replacementSb.toString();
			};
		} else if (mode.contentEquals(MODE_REPLACE)) {
			translationFunc = codePoint -> replacement;
		}
	}

	@Override
	public R apply(R record) {
		return newRecord(record, translated(operatingValue(record)));
	}

	private Object translated(Object input) {

		Schema.Type inferredType = ConnectSchema.schemaType(input.getClass());

		if (inferredType == null) {
			throw new DataException("TranslateSurrogates transformation was passed a value of type " + input.getClass()
			+ " which is not supported by Connect's data API");
		}

		switch (inferredType) {
		case ARRAY:
			@SuppressWarnings("unchecked") final List<Object> listValue = (List<Object>)input;
			final List<Object> updatedArrayValue = new ArrayList<Object>();
			for (Object item: listValue) {
				updatedArrayValue.add(translated(item));
			}
			return updatedArrayValue;
		case STRUCT:
			final Struct structValue = requireStruct(input, PURPOSE);
			final Struct updatedStructValue = new Struct(structValue.schema());
			for (Field field : structValue.schema().fields()) {
				final Object origFieldValue = structValue.get(field);
				if (translateFields.isEmpty()) {
					updatedStructValue.put(field, translated(origFieldValue));
				} else {
					updatedStructValue.put(field, translateFields.contains(field.name()) ? translated(origFieldValue) : origFieldValue);            	
				}
			}
			return updatedStructValue;    
		case MAP:
			final Map<String, Object> mapValue = requireMap(input, PURPOSE);
			final HashMap<String, Object> updatedMapValue = new HashMap<>(mapValue);
			if (translateFields.isEmpty()) {
				translateFields = mapValue.keySet();
			}
			for (String field : translateFields) {
				updatedMapValue.put(field, translated(mapValue.get(field)));
			}
			return updatedMapValue;
		case STRING:
			return (Object) translateString((String) input);
		default:
			return input;
		}
	}

	private String translateString(String input) {
		StringBuilder replacedSb = new StringBuilder();

		for (int index=0; index<input.length() ; index++) {
			int codePoint = input.codePointAt(index);
			if (! Character.isBmpCodePoint(codePoint)) {
				replacedSb.append(translationFunc.apply(codePoint));
				index++;
			} else {
				replacedSb.appendCodePoint(codePoint);
			}
		}
		return replacedSb.toString();
	}

	@Override
	public void close() {
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	protected abstract Schema operatingSchema(R record);

	protected abstract Object operatingValue(R record);

	protected abstract R newRecord(R record, Object updatedValue);

	public static class Key<R extends ConnectRecord<R>> extends TranslateSurrogates<R> {
		@Override
		protected Schema operatingSchema(R record) {
			return record.keySchema();
		}

		@Override
		protected Object operatingValue(R record) {
			return record.key();
		}

		@Override
		protected R newRecord(R record, Object updatedValue) {
			return record.newRecord(record.topic(), record.kafkaPartition(), record.valueSchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
		}

	}

	public static class Value<R extends ConnectRecord<R>> extends TranslateSurrogates<R> {
		@Override
		protected Schema operatingSchema(R record) {
			return record.valueSchema();
		}

		@Override
		protected Object operatingValue(R record) {
			return record.value();
		}

		@Override
		protected R newRecord(R record, Object updatedValue) {
			return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
		}
	}
}
