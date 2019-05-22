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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Version {

  private static final Logger log = LoggerFactory.getLogger(Version.class);
  private static String version = "unknown";

  static {
    try {
      Properties props = new Properties();
      try (InputStream stream = Version.class.
          getResourceAsStream("/support-metrics-common-version.properties")) {
        props.load(stream);
      }
      version = props.getProperty("version", version).trim();
    } catch (IOException e) {
      log.warn("Error while loading version:", e.getMessage());
    }
  }

  public static String getVersion() {
    return version;
  }

}
