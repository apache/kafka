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

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.Hex;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Hash<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(Hash.class);
    private static final String FIELD_NAME_CONFIG = "field.name";
    private static final String FIELD_NAME_DOC =
        "The name of the field which value should be hashed. If null or empty, "
            + "the entire key or value is used (and assumed to be a string). By default is null."
            + "A period can be used to denote a nested path.";
    //todo support nested paths
    private static final String SKIP_MISSING_OR_NULL_CONFIG = "skip.missing.or.null";
    private static final String SKIP_MISSING_OR_NULL_DOC =
        "In case the value to be hashed is null or missing, "
            + "should a record be silently passed without transformation.";
    private static final String FUNCTION_CONFIG = "function";
    private static final String FUNCTION_DOC =
        "The name of the hash function to use. The supported values are: md5, sha1, sha256.";
    private static final String HASH_SALT_CONFIG = "salt";
    private static final String HASH_SALT_DOC =
        "Optional value to use for salt when hashing the given field.";
    private static final String CHARSET_CONFIG = "charset";
    private static final String CHARSET_DOC =
        "Character set to use when encoding";

    public static final ConfigDef CONFIG_DEF = new ConfigDef().define(
        FIELD_NAME_CONFIG,
        ConfigDef.Type.STRING,
        null,
        ConfigDef.Importance.HIGH,
        FIELD_NAME_DOC)
        .define(
            SKIP_MISSING_OR_NULL_CONFIG,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.LOW,
            SKIP_MISSING_OR_NULL_DOC)
        .define(
            FUNCTION_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.ValidString.in(
                HashFunction.MD5.toString(),
                HashFunction.SHA1.toString(),
                HashFunction.SHA256.toString()),
            ConfigDef.Importance.HIGH,
            FUNCTION_DOC)
        .define(
            HASH_SALT_CONFIG,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.LOW,
            HASH_SALT_DOC)
        .define(
            CHARSET_CONFIG,
            ConfigDef.Type.STRING,
            Charset.defaultCharset().toString(),
            ConfigDef.Importance.LOW,
            CHARSET_DOC);
    private MessageDigest messageDigest;
    private Optional<String> fieldName;
    private boolean skipMissingOrNull;
    private Optional<String> hashSalt;
    private Charset charset;

    @Override
    public void configure(final Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        final HashFunction hashFunction = HashFunction.fromString(config.getString(FUNCTION_CONFIG));
        final String rawFieldName = config.getString(FIELD_NAME_CONFIG);
        final String rawHashSalt = config.getString(HASH_SALT_CONFIG);

        if (null == rawFieldName || "".equals(rawFieldName)) {
            fieldName = Optional.empty();
        } else {
            fieldName = Optional.of(rawFieldName);
        }

        if (null == rawHashSalt || "".equals(rawHashSalt)) {
            hashSalt = Optional.empty();
        } else {
            hashSalt = Optional.of(rawHashSalt);
        }

        skipMissingOrNull = config.getBoolean(SKIP_MISSING_OR_NULL_CONFIG);
        charset = Charset.forName(config.getString(CHARSET_CONFIG));

        try {
            switch (hashFunction) {
                case MD5:
                    messageDigest = MessageDigest.getInstance("MD5");
                    break;
                case SHA1:
                    messageDigest = MessageDigest.getInstance("SHA1");
                    break;
                case SHA256:
                    messageDigest = MessageDigest.getInstance("SHA-256");
                    break;
                default:
                    throw new ConnectException("Unknown hash function " + hashFunction);
            }
        } catch (final NoSuchAlgorithmException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public R apply(final R record) {
        final SchemaAndValue schemaAndValue = getSchemaAndValue(record);
        if (schemaAndValue.schema() == null) {
            throw new DataException(dataPlace() + " schema can't be null: " + record);
        }

        final Optional<Object> newValue;
        if (fieldName.isPresent()) {
            newValue = getNewValueForNamedField(
                record.toString(), schemaAndValue.schema(), schemaAndValue.value(), fieldName.get());
        } else {
            newValue = getNewValueWithoutFieldName(
                record.toString(), schemaAndValue.schema(), schemaAndValue.value());
        }

        if (newValue.isPresent()) {
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                newValue.get(),
                record.timestamp(),
                record.headers()
            );
        } else {
            return record;
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected abstract String dataPlace();

    protected abstract SchemaAndValue getSchemaAndValue(final R record);

    private Optional<Object> getNewValueForNamedField(final String recordStr,
                                                      final Schema schema,
                                                      final Object value,
                                                      final String fieldName) {
        //todo maybe support maps?
        if (Schema.Type.STRUCT != schema.type()) {
            throw new DataException(dataPlace() + " schema type must be STRUCT if field name is specified: "
                + recordStr);
        }

        if (value == null) {
            throw new DataException(dataPlace() + " can't be null if field name is specified: " + recordStr);
        }

//        Struct updatedRecord;
//
//        final List<List<String>> fieldNames =
//            Arrays.asList(fieldName.split(",")).stream().map(s -> Arrays.asList(s.split("."))).collect(
//                Collectors.toList());

        final Field field = schema.field(fieldName);
        if (field == null) {
            if (skipMissingOrNull) {
                if (log.isDebugEnabled()) {
                    log.debug(fieldName + " in " + dataPlace() + " schema is missing, skipping transformation");
                }
                return Optional.empty();
            } else {
                throw new DataException(fieldName + " in " + dataPlace() + " schema can't be missing: " + recordStr);
            }
        }

        if (field.schema().type() != Schema.Type.STRING) {
            throw new DataException(fieldName + " schema type in " + dataPlace()
                + " must be " + Schema.Type.STRING
                + ": " + recordStr);
        }

        final Struct struct = (Struct) value;
        final String stringValue = struct.getString(fieldName);
        if (stringValue == null) {
            if (skipMissingOrNull) {
                if (log.isDebugEnabled()) {
                    log.debug(fieldName + " in " + dataPlace() + " is null, skipping transformation");
                }
                return Optional.empty();
            } else {
                throw new DataException(fieldName + " in " + dataPlace() + " can't be null: " + recordStr);
            }
        } else {
            final String updatedValue = hashString(stringValue);
            final Struct updatedRecord = struct.put(fieldName, updatedValue);
            return Optional.ofNullable(updatedRecord);
        }
    }

    private Optional<Object> getNewValueWithoutFieldName(final String recordStr,
                                                         final Schema schema,
                                                         final Object value) {
        if (schema.type() != Schema.Type.STRING) {
            throw new DataException(dataPlace() + " schema type must be "
                + Schema.Type.STRING
                + " if field name is not specified: "
                + recordStr);
        }

        if (value == null) {
            if (skipMissingOrNull) {
                if (log.isDebugEnabled()) {
                    log.debug(dataPlace() + " is null, skipping transformation");
                }
                return Optional.empty();
            } else {
                throw new DataException(dataPlace() + " can't be null: " + recordStr);
            }
        }

        return Optional.of(hashString(value.toString()));
    }

    private String hashString(final String string) {
        messageDigest.reset();
        this.hashSalt.ifPresent(s -> messageDigest.update(s.getBytes(this.charset)));

        final byte[] digest = messageDigest.digest(string.getBytes(this.charset));
        return Hex.encode(digest);
    }

    public enum HashFunction {
        MD5 {
            public String toString() {
                return "md5";
            }
        },
        SHA1 {
            public String toString() {
                return "sha1";
            }
        },
        SHA256 {
            public String toString() {
                return "sha256";
            }
        };

        public static HashFunction fromString(final String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }
    }

    public static class Key<R extends ConnectRecord<R>> extends Hash<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.keySchema(), record.key());
        }

        @Override
        protected String dataPlace() {
            return "key";
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends Hash<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.valueSchema(), record.value());
        }

        @Override
        protected String dataPlace() {
            return "value";
        }
    }
}
