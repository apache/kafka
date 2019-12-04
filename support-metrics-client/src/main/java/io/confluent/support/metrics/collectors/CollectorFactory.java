/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.support.metrics.collectors;

import java.lang.reflect.Constructor;
import java.util.Properties;

import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.CollectorType;
import io.confluent.support.metrics.common.time.TimeUtils;
import kafka.server.KafkaServer;

public class CollectorFactory {

  private final CollectorType type;
  private final Collector collector;

  private static CollectorConstructorSupplier
      basicCollectorSupplier =
      new CollectorFactory.CollectorConstructorSupplier(new CollectorFactory.ConstructorSupplier() {
        @Override
        public Constructor get() throws ClassNotFoundException, NoSuchMethodException {
          return Class.forName("io.confluent.support.metrics.collectors.BasicCollector")
              .getConstructor(KafkaServer.class, TimeUtils.class);
        }
      });

  private static CollectorConstructorSupplier
      fullCollectorSupplier =
      new CollectorFactory.CollectorConstructorSupplier(new CollectorFactory.ConstructorSupplier() {
        @Override
        public Constructor get() throws ClassNotFoundException, NoSuchMethodException {
          return Class.forName("io.confluent.support.metrics.collectors.FullCollector")
              .getConstructor(KafkaServer.class, Properties.class, Runtime.class, TimeUtils.class);
        }
      });

  private interface ConstructorSupplier {

    Constructor get() throws ClassNotFoundException, NoSuchMethodException;
  }

  // this code's structure is based on Compressor's structure @see{org.apache.kafka.common.record
  // .Compressor}
  private static class CollectorConstructorSupplier {

    final CollectorFactory.ConstructorSupplier delegate;
    transient volatile boolean initialized;
    transient Constructor value;

    public CollectorConstructorSupplier(CollectorFactory.ConstructorSupplier delegate) {
      this.delegate = delegate;
    }

    public Constructor get() throws NoSuchMethodException, ClassNotFoundException {
      if (!initialized) {
        synchronized (this) {
          if (!initialized) {
            Constructor constructor = delegate.get();
            value = constructor;
            initialized = true;
            return constructor;
          }
        }
      }
      return value;
    }
  }

  public CollectorFactory(
      CollectorType type,
      TimeUtils time,
      KafkaServer server,
      Properties serverConfiguration,
      Runtime serverRuntime
  ) {
    this.type = type;
    try {
      switch (type) {
        case BASIC:
          collector = (Collector) basicCollectorSupplier.get().newInstance(server, time);
          break;
        case FULL:
          collector = (Collector) fullCollectorSupplier.get().newInstance(
              server,
              serverConfiguration,
              serverRuntime,
              time
          );
          break;
        default:
          throw new IllegalArgumentException("Unknown collector type");
      }
    } catch (Exception e) {
      throw new RuntimeException("Collector factory failed to create instance.", e);
    }
  }


  public Collector getCollector() {
    return collector;
  }

  public CollectorType getType() {
    return type;
  }
}
