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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * {@code AssertionJwtTemplate} is used to provide values for use by {@link AssertionCreator}.
 * The JWT header and/or payload used in the assertion likely requires headers and claims. Not all identity
 * providers require the same set of headers and claims; some may require a given header or claim while
 * other identity providers may prohibit it. In order to provide the most flexibility, the header
 * values and claims that are to be included in the JWT can be added via a template.
 *
 * <p/>
 *
 * Both the {@link #header()} and {@link #payload()} APIs return a map of Objects. This because the
 * <a href="https://www.json.org/">JSON specification<a> allow values to be one of the following "types":
 *
 * <ul>
 *   <li>object</li>
 *   <li>array</li>
 *   <li>string</li>
 *   <li>number</li>
 *   <li><code>true</code></li>
 *   <li><code>false</code></li>
 *   <li><code>null</code></li>
 * </ul>
 *
 * However, because the maps must be converted into JSON, it's important that any nested types use standard
 * Java type equivalents (Map, List, String, Integer, Double, and Boolean) so that the JSON library will
 * know how to serialize the entire object graph.
 */
public interface AssertionJwtTemplate extends Closeable {

    /**
     * Returns a map containing zero or more header values.
     *
     * @return Values to include in the JWT header
     */
    Map<String, Object> header();

    /**
     * Returns a map containing zero or more JWT payload claim values.
     *
     * @return Values to include in the JWT payload
     */
    Map<String, Object> payload();

    /**
     * Closes any resources used by this implementation. The default implementation of
     * this method is a no op, for convenience to implementors.
     */
    @Override
    default void close() throws IOException {
        // Do nothing...
    }
}
