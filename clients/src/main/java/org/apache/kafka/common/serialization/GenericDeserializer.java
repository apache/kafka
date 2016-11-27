/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.common.serialization;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This deserializer can deserialize any object of POJO class
 * 
 * @author Jason Guo <habren@163.com>
 *
 * @param <T>
 *            POJO class. The class should have a constructor without any
 *            arguments and have setter and getter for every member variable
 * 
 */

public class GenericDeserializer<T> implements Deserializer<T> {

    private Class<T> type;
    private ObjectMapper objectMapper = new ObjectMapper();

    public GenericDeserializer() {
    }

    public GenericDeserializer(Class<T> type) {
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (type != null) {
            return;
        }

        String typeProp = isKey ? "key.deserializer.type" : "value.deserializer.type";
        String typeName = (String) configs.get(typeProp);
        try {
            type = (Class<T>) Class.forName(typeName);
        } catch (Exception ex) {
            throw new SerializationException("Failed to initialize GenericDeserializer for " + typeName, ex);
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return this.objectMapper.readValue(data, type);
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public void close() {
    }

}