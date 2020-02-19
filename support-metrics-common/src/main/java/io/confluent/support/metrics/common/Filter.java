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

package io.confluent.support.metrics.common;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class Filter {

  private final Set<String> keysToRemove;

  /**
   * The default filter does not filter anything.
   */
  public Filter() {
    this(new HashSet<String>());
  }

  public Filter(Set<String> keysToRemove) {
    this.keysToRemove = new HashSet<>(keysToRemove);
  }

  /**
   * Returns a copy of the input with any to-be-filtered keys removed.
   */
  public Properties apply(Properties properties) {
    if (properties == null) {
      throw new IllegalArgumentException("properties must not be null");
    } else {
      if (properties.isEmpty()) {
        return new Properties();
      } else {
        Properties filtered = new Properties();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
          Object key = entry.getKey();
          Object value = entry.getValue();
          if (!keysToRemove.contains(key)) {
            filtered.put(key, value);
          }
        }
        return filtered;
      }
    }
  }

  public Set<String> getKeys() {
    return new HashSet<>(keysToRemove);
  }

}
