package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class CustomJsonSerde<T> implements Serde<T> {

  private final ObjectMapper objectMapper;
  private final Class<T> clazz;

  public CustomJsonSerde(Class<T> clazz) {
    this.objectMapper = new ObjectMapper();
    this.clazz = clazz;
  }

  @Override
  public Serializer<T> serializer() {
    return new Serializer<T>() {
      @Override
      public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
      }

      @Override
      public byte[] serialize(String topic, T data) {
        if (data == null) {
          return null;
        }
        try {
          return objectMapper.writeValueAsBytes(data);
        } catch (IOException e) {
          throw new SerializationException("Error serializing JSON message", e);
        }
      }

      @Override
      public void close() {
        // No resources to close
      }
    };
  }

  @Override
  public Deserializer<T> deserializer() {
    return new Deserializer<T>() {
      @Override
      public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
      }

      @Override
      public T deserialize(String topic, byte[] data) {
        if (data == null) {
          return null;
        }
        try {
          return objectMapper.readValue(data, clazz);
        } catch (IOException e) {
          throw new SerializationException("Error deserializing JSON message", e);
        }
      }

      @Override
      public void close() {
        // No resources to close
      }
    };
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // No configuration needed
  }

  @Override
  public void close() {
    // No resources to close
  }

  public static <T> CustomJsonSerde<T> json(Class<T> clazz) {
    return new CustomJsonSerde<>(clazz);
  }
}