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
package org.apache.kafka.common.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;


public class ListSerializerTest {
    private final ListSerializer<?> listSerializer = new ListSerializer<>();
    private final Map<String, Object> props = new HashMap<>();
    private final String nonExistingClass = "non.existing.class";
    private static class FakeObject {
    }

    @Test
    public void testListKeySerializerNoArgConstructorsWithClassName() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class.getName());
        listSerializer.configure(props, true);
        final Serializer<?> inner = listSerializer.getInnerSerializer();
        assertNotNull(inner, "Inner serializer should be not null");
        assertInstanceOf(StringSerializer.class, inner, "Inner serializer type should be StringSerializer");
    }

    @Test
    public void testListValueSerializerNoArgConstructorsWithClassName() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, Serdes.StringSerde.class.getName());
        listSerializer.configure(props, false);
        final Serializer<?> inner = listSerializer.getInnerSerializer();
        assertNotNull(inner, "Inner serializer should be not null");
        assertInstanceOf(StringSerializer.class, inner, "Inner serializer type should be StringSerializer");
    }

    @Test
    public void testListKeySerializerNoArgConstructorsWithClassObject() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        listSerializer.configure(props, true);
        final Serializer<?> inner = listSerializer.getInnerSerializer();
        assertNotNull(inner, "Inner serializer should be not null");
        assertInstanceOf(StringSerializer.class, inner, "Inner serializer type should be StringSerializer");
    }

    @Test
    public void testListValueSerializerNoArgConstructorsWithClassObject() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        listSerializer.configure(props, false);
        final Serializer<?> inner = listSerializer.getInnerSerializer();
        assertNotNull(inner, "Inner serializer should be not null");
        assertInstanceOf(StringSerializer.class, inner, "Inner serializer type should be StringSerializer");
    }

    @Test
    public void testListSerializerNoArgConstructorsShouldThrowConfigExceptionDueMissingProp() {
        ConfigException exception = assertThrows(
            ConfigException.class,
            () -> listSerializer.configure(props, true)
        );
        assertEquals("Not able to determine the serializer class because it was neither passed via the constructor nor set in the config.", exception.getMessage());

        exception = assertThrows(
            ConfigException.class,
            () -> listSerializer.configure(props, false)
        );
        assertEquals("Not able to determine the serializer class because it was neither passed via the constructor nor set in the config.", exception.getMessage());
    }

    @Test
    public void testListKeySerializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidClass() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, new FakeObject());
        final KafkaException exception = assertThrows(
            KafkaException.class,
            () -> listSerializer.configure(props, true)
        );
        assertEquals("Could not create a serializer class instance using \"" + CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS + "\" property.", exception.getMessage());
    }

    @Test
    public void testListValueSerializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidClass() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, new FakeObject());
        final KafkaException exception = assertThrows(
            KafkaException.class,
            () -> listSerializer.configure(props, false)
        );
        assertEquals("Could not create a serializer class instance using \"" + CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS + "\" property.", exception.getMessage());
    }

    @Test
    public void testListKeySerializerNoArgConstructorsShouldThrowKafkaExceptionDueClassNotFound() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, nonExistingClass);
        final KafkaException exception = assertThrows(
            KafkaException.class,
            () -> listSerializer.configure(props, true)
        );
        assertEquals("Invalid value non.existing.class for configuration " + CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS + ": Serializer class " + nonExistingClass + " could not be found.", exception.getMessage());
    }

    @Test
    public void testListValueSerializerNoArgConstructorsShouldThrowKafkaExceptionDueClassNotFound() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, nonExistingClass);
        final KafkaException exception = assertThrows(
            KafkaException.class,
            () -> listSerializer.configure(props, false)
        );
        assertEquals("Invalid value non.existing.class for configuration " + CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS + ": Serializer class " + nonExistingClass + " could not be found.", exception.getMessage());
    }

    @Test
    public void testListKeySerializerShouldThrowConfigExceptionDueAlreadyInitialized() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        final ListSerializer<Integer> initializedListSerializer = new ListSerializer<>(Serdes.Integer().serializer());
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> initializedListSerializer.configure(props, true)
        );
        assertEquals("List serializer was already initialized using a non-default constructor", exception.getMessage());
    }

    @Test
    public void testListValueSerializerShouldThrowConfigExceptionDueAlreadyInitialized() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        final ListSerializer<Integer> initializedListSerializer = new ListSerializer<>(Serdes.Integer().serializer());
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> initializedListSerializer.configure(props, false)
        );
        assertEquals("List serializer was already initialized using a non-default constructor", exception.getMessage());
    }

}
