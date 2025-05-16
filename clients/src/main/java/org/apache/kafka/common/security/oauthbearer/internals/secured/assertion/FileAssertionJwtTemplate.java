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
package org.apache.kafka.common.security.oauthbearer.internals.secured.assertion;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.oauthbearer.internals.secured.CachedFile;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.common.security.oauthbearer.internals.secured.CachedFile.RefreshPolicy.lastModifiedPolicy;

/**
 * {@code FileAssertionJwtTemplate} is used by the user to specify a JSON file on disk that contains static values
 * that can be loaded and used to construct the assertion. The file structure is a JSON containing optionally a
 * header and/or payload top-level attribute.
 *
 * <p/>
 *
 * Here is a minimally viable JSON structure:
 *
 * <pre>
 * {
 * }
 * </pre>
 *
 * OK, at that point it doesn't make sense for the user to build that file.
 *
 * <p/>
 *
 * Here is another, slightly less minimal JSON template:
 *
 * <pre>
 * {
 *    "header": {
 *     "foo": 1
 *   },
 *    "payload": {
 *     "bar": 2
 *   }
 * }
 * </pre>
 *
 * This provides a single header value and a single payload claim.
 *
 * <p/>
 *
 * A more realistic example template looks like so:
 *
 * <pre>
 * {
 *   "header": {
 *     "kid": "f829d41b06f14f9e",
 *     "some-random-header": 123456
 *   },
 *   "payload": {
 *     "sub": "some-service-account",
 *     "aud": "my_audience",
 *     "iss": "https://example.com",
 *     "useSomeResource": false,
 *     "allowedAnimals": [
 *       "cat",
 *       "dog",
 *       "hamster"
 *     ]
 *   }
 * }
 * </pre>
 *
 * The AssertionCreator would accept the AssertionJwtTemplate and augment the template header and/or payload
 * with dynamic values. For example, the above header would be augmented with the {@code alg} (algorithm) and
 * {@code typ} (type) values per the OAuth RFC:
 *
 * <pre>
 * {
 *   "kid": "f829d41b06f14f9e",
 *   "some-random-header": 123456,
 *   "alg": "RS256",
 *   "typ": "JWT"
 * }
 * </pre>
 *
 * And the payload would also be augmented to add the {@code iat} (issued at) and {@code exp} (expiration) timestamps:
 *
 * <pre>
 * {
 *   "iat": 1741121401,
 *   "exp": 1741125001,
 *   "sub": "some-service-account",
 *   "aud": "my_audience",
 *   "iss": "https://example.com",
 *   "useSomeResource": false,
 *   "allowedAnimals": [
 *     "cat",
 *     "dog",
 *     "hamster"
 *   ]
 * }
 * </pre>
 */
public class FileAssertionJwtTemplate implements AssertionJwtTemplate {

    @SuppressWarnings("unchecked")
    private static final CachedFile.Transformer<CachedJwtTemplate> JSON_TRANSFORMER = (file, json) -> {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = (Map<String, Object>) mapper.readValue(json, Map.class);

            Map<String, Object> header = (Map<String, Object>) map.computeIfAbsent("header", k -> Map.of());
            Map<String, Object> payload = (Map<String, Object>) map.computeIfAbsent("payload", k -> Map.of());

            return new CachedJwtTemplate(header, payload);
        } catch (Exception e) {
            throw new KafkaException("An error occurred parsing the OAuth assertion template file from " + file.getPath(), e);
        }
    };

    private final CachedFile<CachedJwtTemplate> jsonFile;

    public FileAssertionJwtTemplate(File jsonFile) {
        this.jsonFile = new CachedFile<>(jsonFile, JSON_TRANSFORMER, lastModifiedPolicy());
    }

    @Override
    public Map<String, Object> header() {
        return jsonFile.transformed().header;
    }

    @Override
    public Map<String, Object> payload() {
        return jsonFile.transformed().payload;
    }

    /**
     * Internally, the cached file is represented by the two maps for the header and payload.
     */
    private static class CachedJwtTemplate {

        private final Map<String, Object> header;

        private final Map<String, Object> payload;

        private CachedJwtTemplate(Map<String, Object> header, Map<String, Object> payload) {
            this.header = Collections.unmodifiableMap(header);
            this.payload = Collections.unmodifiableMap(payload);
        }
    }
}
