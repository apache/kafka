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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.util.ConnectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * A deterministic hash of a connector configuration. This can be used to detect changes
 * in connector configurations across worker lifetimes, which is sometimes necessary when
 * connectors are reconfigured in a way that affects their tasks' runtime behavior but does
 * not affect their tasks' configurations (for example, changing the key converter class).
 *
 * @see <a href="https://issues.apache.org/jira/browse/KAFKA-9228">KAFKA-9228</a>
 */
public class ConfigHash {

    private static final Logger log = LoggerFactory.getLogger(ConfigHash.class);

    public static final ConfigHash NO_HASH = new ConfigHash(null);
    public static final String CONNECTOR_CONFIG_HASH_HEADER = "X-Connect-Connector-Config-Hash";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Integer hash;

    // Visible for testing
    ConfigHash(Integer hash) {
        this.hash = hash;
    }

    /**
     * Read and parse a hash from the headers of a REST request.
     *
     * @param connector the name of the connector; only used for error logging
     *                  purposes and may be null
     * @param headers the headers from which to read and parse the hash;
     *                may be null
     *
     * @return the parsed hash; never null, but may be {@link #NO_HASH} if
     * no hash header is present
     *
     * @throws ConnectException if the expected header is present for the hash,
     * but it cannot be parsed as a 32-bit signed integer
     */
    public static ConfigHash fromHeaders(String connector, HttpHeaders headers) {
        if (headers == null)
            return NO_HASH;

        String header = headers.getHeaderString(CONNECTOR_CONFIG_HASH_HEADER);
        if (header == null)
            return NO_HASH;

        int hash;
        try {
            hash = Integer.parseInt(header);
        } catch (NumberFormatException e) {
            if (connector == null)
                connector = "<unknown>";

            if (log.isTraceEnabled()) {
                log.error("Invalid connector config hash header for connector {}", connector);
                log.trace("Invalid connector config hash header for connector {}: '{}'", connector, header);
            } else {
                log.error(
                        "Invalid connector config hash header for connector {}. "
                                + "Please enable TRACE logging to see the invalid value",
                        connector
                );
            }
            throw new ConnectException("Invalid hash header; expected a 32-bit signed integer");
        }
        return new ConfigHash(hash);
    }

    /**
     * Generate a deterministic hash from the config. For configurations
     * with identical key-value pairs, this hash will always be the same, and
     * {@link #shouldUpdateTasks(ConfigHash, ConfigHash)} will return {@code false}
     * for any two such configurations. Note that, for security reasons, those
     * {@link ConfigHash} instances will still not {@link #equals(Object) equal}
     * each other.
     *
     * @param config the configuration to hash; may be null
     *
     * @return the resulting hash; may be {@link #NO_HASH} if the configuration
     * was null
     *
     * @throws ConnectException if the configuration cannot be serialized to JSON
     * for the purposes of hashing
     */
    public static ConfigHash fromConfig(Map<String, String> config) {
        if (config == null)
            return NO_HASH;

        Map<String, String> toHash = new TreeMap<>(config);

        byte[] serialized;
        try {
            serialized = OBJECT_MAPPER.writeValueAsBytes(toHash);
        } catch (IOException e) {
            throw new ConnectException(
                    "Unable to serialize connector config contents for hashing",
                    e
            );
        }

        int hash = Utils.murmur2(serialized);
        return new ConfigHash(hash);
    }

    /**
     * Read and parse the hash from the headers of a REST request.
     *
     * @param map the map to read the hash field from; may be null
     * @param field the field to read and parse; may be null
     *
     * @return the parsed hash; never null, but may be {@link #NO_HASH} if
     * the map is null or the field is not present in the map
     *
     * @throws ConnectException if the expected field is present for the hash,
     * but it cannot be parsed as a 32-bit signed integer
     */
    public static ConfigHash fromMap(Map<String, ?> map, String field) {
        if (map == null)
            return NO_HASH;

        Object rawHash = map.get(field);
        if (rawHash == null)
            return NO_HASH;

        int hash = ConnectUtils.intValue(rawHash);
        return new ConfigHash(hash);
    }

    /**
     * Determine whether tasks should be restarted based on a previously-stored
     * hash, and the hash for a connector config that was used to generate new task configs
     *
     * @param previous the previously-stored config hash for the connector
     * @param current the hash of the connector config which led to newly-generated
     *                task configs
     *
     * @return whether a restart of the connector's tasks should be forced, possibly to
     * pick up runtime-controlled configuration changes that would otherwise be dropped
     *
     * @see <a href="https://issues.apache.org/jira/browse/KAFKA-9228">KAFKA-9228</a>
     */
    public static boolean shouldUpdateTasks(ConfigHash previous, ConfigHash current) {
        if (previous == null || current == null)
            return false;

        return previous.exists() && !previous.matches(current);
    }

    /**
     * Insert this hash (if it {@link #exists() exists}) into a {@link Struct} with the desired field name.
     *
     * @param struct the struct to add the hash to; may be null, in which case
     *               this method becomes a no-op
     * @param field the name of the field to add the hash under; may be null,
     *              in which case this method becomes a no-op
     */
    public void addToStruct(Struct struct, String field) {
        if (hash == null || struct == null || field == null)
            return;
        struct.put(field, hash);
    }

    /**
     * Add this hash (if it {@link #exists() exists}) to a map of HTTP headers
     *
     * @param headers the headers map to add this hash to; may be null, in which case
     *                this method becomes a no-op
     */
    public void addToHeaders(Map<String, String> headers) {
        if (headers == null || !exists())
            return;

        headers.put(CONNECTOR_CONFIG_HASH_HEADER, Integer.toString(hash));
    }

    /**
     * @return whether a hash for this config was found
     */
    public boolean exists() {
        return hash != null;
    }

    @Override
    public String toString() {
        // DO NOT OVERRIDE THIS METHOD; config hashes should not be logged
        return "<config hash>";
    }

    @Override
    public boolean equals(Object o) {
        // DO NOT OVERRIDE THIS METHOD; may leak the hash value and/or break
        // the hashCode/equals contract
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        // DO NOT OVERRIDE THIS METHOD; may leak the hash value
        return super.hashCode();
    }

    /**
     * Return whether this hash matches another hash. Should be used only in testing code.
     *
     * @param that the other hash to test against; may be null
     *
     * @return whether the two hashes are identical
     */
    // Visible for testing
    boolean matches(ConfigHash that) {
        return that != null && this.hash.equals(that.hash);
    }
}
