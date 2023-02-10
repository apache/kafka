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
package org.apache.kafka.connect.runtime;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

/**
 * This is a simple Converter implementation that uses "encoding" header to encode/decode strings via provided charset name
 */
public class SampleConverterWithHeaders implements Converter {
    private static final String HEADER_ENCODING = "encoding";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        String encoding = extractEncoding(headers);

        try {
            return new SchemaAndValue(Schema.STRING_SCHEMA, new String(value, encoding));
        } catch (UnsupportedEncodingException e) {
            throw new DataException("Unsupported encoding: " + encoding, e);
        }
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        String encoding = extractEncoding(headers);

        try {
            return ((String) value).getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            throw new DataException("Unsupported encoding: " + encoding, e);
        }
    }

    private String extractEncoding(Headers headers) {
        Header header = headers.lastHeader(HEADER_ENCODING);
        if (header == null) {
            throw new DataException("Header '" + HEADER_ENCODING + "' is required!");
        }

        return new String(header.value());
    }


    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        throw new DataException("Headers are required for this converter!");
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        throw new DataException("Headers are required for this converter!");
    }
}
