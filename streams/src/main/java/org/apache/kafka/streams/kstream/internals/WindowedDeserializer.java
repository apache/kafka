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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.internals.WindowStoreUtils;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 *  The inner deserializer class can be specified by setting the property key.deserializer.inner.class,
 *  value.deserializer.inner.class or deserializer.inner.class,
 *  if the no-arg constructor is called and hence it is not passed during initialization.
 *  Note that the first two take precedence over the last.
 */
public class WindowedDeserializer<T> implements Deserializer<Windowed<T>> {

    private static final int TIMESTAMP_SIZE = 8;
    private final Long windowSize;
    
    private Deserializer<T> inner;
    
    // Default constructor needed by Kafka
    public WindowedDeserializer() {
        this(null, Long.MAX_VALUE);
    }
    
    public WindowedDeserializer(final Long windowSize) {
       this(null, windowSize);
    }
    
    public WindowedDeserializer(final Deserializer<T> inner) {
        this(inner, Long.MAX_VALUE);
    }

    public WindowedDeserializer(final Deserializer<T> inner,
                                final long windowSize) {
        this.inner = inner;
        this.windowSize = windowSize;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (inner == null) {
            String propertyName = isKey ? "key.deserializer.inner.class" : "value.deserializer.inner.class";
            Object innerDeserializerClass = configs.get(propertyName);
            propertyName = (innerDeserializerClass == null) ? "deserializer.inner.class" : propertyName;
            String value = null;
            try {
                value = (String) configs.get(propertyName);
                inner = Deserializer.class.cast(Utils.newInstance(value, Deserializer.class));
                inner.configure(configs, isKey);
            } catch (ClassNotFoundException e) {
                throw new ConfigException(propertyName, value, "Class " + value + " could not be found.");
            }
        }
    }

    @Override
    public Windowed<T> deserialize(String topic, byte[] data) {

        byte[] bytes = new byte[data.length - TIMESTAMP_SIZE];

        System.arraycopy(data, 0, bytes, 0, bytes.length);
        
        long start = ByteBuffer.wrap(data).getLong(data.length - TIMESTAMP_SIZE);
        
        Window timeWindow = windowSize != Long.MAX_VALUE ? WindowStoreUtils.timeWindowForSize(start, windowSize) : new UnlimitedWindow(start);
        return new Windowed<T>(inner.deserialize(topic, bytes), timeWindow);
    }


    @Override
    public void close() {
        inner.close();
    }
    
    // Only for testing
    public Deserializer<T> innerDeserializer() {
        return inner;
    }
    
    public Long getWindowSize() {
        return this.windowSize;
    }
}
