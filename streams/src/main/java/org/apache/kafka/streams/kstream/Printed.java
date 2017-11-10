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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.errors.TopologyException;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * An object to define the options used when printing a {@link KStream}.
 *
 * @param <K> key type
 * @param <V> value type
 * @see KStream#print(Printed)
 */
public class Printed<K, V> {
    protected final PrintWriter printWriter;
    protected String label;
    protected KeyValueMapper<? super K, ? super V, String> mapper = new KeyValueMapper<K, V, String>() {
        @Override
        public String apply(final K key, final V value) {
            return String.format("%s, %s", key, value);
        }
    };

    private Printed(final PrintWriter printWriter) {
        this.printWriter = printWriter;
    }

    /**
     * Copy constructor.
     * @param printed   instance of {@link Printed} to copy
     */
    protected Printed(final Printed<K, V> printed) {
        this.printWriter = printed.printWriter;
        this.label = printed.label;
        this.mapper = printed.mapper;
    }

    /**
     * Print the records of a {@link KStream} to a file.
     *
     * @param filePath path of the file
     * @param <K>      key type
     * @param <V>      value type
     * @return a new Printed instance
     */
    public static <K, V> Printed<K, V> toFile(final String filePath) {
        Objects.requireNonNull(filePath, "filePath can't be null");
        if (filePath.trim().isEmpty()) {
            throw new TopologyException("filePath can't be an empty string");
        }
        try {
            return new Printed<>(new PrintWriter(filePath, StandardCharsets.UTF_8.name()));
        } catch (final FileNotFoundException | UnsupportedEncodingException e) {
            throw new TopologyException("Unable to write stream to file at [" + filePath + "] " + e.getMessage());
        }
    }

    /**
     * Print the records of a {@link KStream} to system out.
     *
     * @param <K> key type
     * @param <V> value type
     * @return a new Printed instance
     */
    public static <K, V> Printed<K, V> toSysOut() {
        return new Printed<>((PrintWriter) null);
    }

    /**
     * Print the records of a {@link KStream} with the provided label.
     *
     * @param label label to use
     * @return this
     */
    public Printed<K, V> withLabel(final String label) {
        Objects.requireNonNull(label, "label can't be null");
        this.label = label;
        return this;
    }

    /**
     * Print the records of a {@link KStream} with the provided {@link KeyValueMapper}
     * The provided KeyValueMapper's mapped value type must be {@code String}.
     * <p>
     * The example below shows how to customize output data.
     * <pre>{@code
     * final KeyValueMapper<Integer, String, String> mapper = new KeyValueMapper<Integer, String, String>() {
     *     public String apply(Integer key, String value) {
     *         return String.format("(%d, %s)", key, value);
     *     }
     * };
     * }</pre>
     *
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     *
     * @param mapper mapper to use
     * @return this
     */
    public Printed<K, V> withKeyValueMapper(final KeyValueMapper<? super K, ? super V, String> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        this.mapper = mapper;
        return this;
    }
}
