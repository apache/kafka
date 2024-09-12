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

package test.plugins;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.List;
import java.util.Map;

/**
 * Fake plugin class for testing classloading isolation.
 * See {@link org.apache.kafka.connect.runtime.isolation.TestPlugins}.
 * <p>Class which is not migrated to include a service loader manifest.
 */
public final class NonMigratedMultiPlugin implements Converter, HeaderConverter, Predicate, Transformation, ConnectorClientConfigOverridePolicy {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    return new byte[0];
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return null;
  }

  @Override
  public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
    return null;
  }

  @Override
  public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
    return new byte[0];
  }

  @Override
  public ConnectRecord apply(ConnectRecord record) {
    return null;
  }

  @Override
  public ConfigDef config() {
    return null;
  }

  @Override
  public boolean test(ConnectRecord record) {
    return false;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {

  }

  @Override
  public List<ConfigValue> validate(ConnectorClientConfigRequest connectorClientConfigRequest) {
    return null;
  }
}
