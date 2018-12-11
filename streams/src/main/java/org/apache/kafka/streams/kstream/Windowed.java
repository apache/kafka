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


/**
 * The result key type of a windowed stream aggregation.
 * <p>
 * If a {@link KStream} gets grouped and aggregated using a window-aggregation the resulting {@link KTable} is a
 * so-called "windowed {@link KTable}" with a combined key type that encodes the corresponding aggregation window and
 * the original record key.
 * Thus, a windowed {@link KTable} has type {@code <Windowed<K>,V>}.
 *
 * @param <K> type of the key
 * @see KGroupedStream#windowedBy(Windows)
 * @see KGroupedStream#windowedBy(SessionWindows)
 */
public class Windowed<K> {

    private final K key;

    private final Window window;

    public Windowed(final K key, final Window window) {
        this.key = key;
        this.window = window;
    }

    /**
     * Return the key of the window.
     *
     * @return the key of the window
     */
    public K key() {
        return key;
    }

    /**
     * Return the window containing the values associated with this key.
     *
     * @return the window containing the values
     */
    public Window window() {
        return window;
    }

    @Override
    public String toString() {
        return "[" + key + "@" + window.start() + "/" + window.end() + "]";
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof Windowed)) {
            return false;
        }
        final Windowed<?> that = (Windowed) obj;
        return window.equals(that.window) && key.equals(that.key);
    }

    @Override
    public int hashCode() {
        final long n = ((long) window.hashCode() << 32) | key.hashCode();
        return (int) (n % 0xFFFFFFFFL);
    }
}
