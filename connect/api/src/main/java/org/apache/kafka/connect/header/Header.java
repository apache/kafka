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
package org.apache.kafka.connect.header;

import org.apache.kafka.connect.data.Schema;

/**
 * A {@link Header} is a key-value pair, and multiple headers can be included with the key, value, and timestamp in each Kafka message.
 * If the value contains schema information, then the header will have a non-null {@link #schema() schema}.
 * <p>
 * This is an immutable interface.
 */
public interface Header {

    /**
     * The header's key, which is not necessarily unique within the set of headers on a Kafka message.
     *
     * @return the header's key; never null
     */
    String key();

    /**
     * Return the {@link Schema} associated with this header, if there is one. Not all headers will have schemas.
     *
     * @return the header's schema, or null if no schema is associated with this header
     */
    Schema schema();

    /**
     * Get the header's value as deserialized by Connect's header converter.
     *
     * @return the deserialized object representation of the header's value; may be null
     */
    Object value();

    /**
     * Return a new {@link Header} object that has the same key but with the supplied value.
     *
     * @param schema the schema for the new value; may be null
     * @param value  the new value
     * @return the new {@link Header}; never null
     */
    Header with(Schema schema, Object value);

    /**
     * Return a new {@link Header} object that has the same schema and value but with the supplied key.
     *
     * @param key the key for the new header; may not be null
     * @return the new {@link Header}; never null
     */
    Header rename(String key);
}
