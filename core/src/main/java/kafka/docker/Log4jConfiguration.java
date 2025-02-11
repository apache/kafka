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
package kafka.docker;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Log4jConfiguration {
    private Configuration configuration;

    @JsonProperty("Configuration")
    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }
}

@JsonPropertyOrder({ "Properties", "Appenders", "Loggers" })
@JsonIgnoreProperties(ignoreUnknown = true)
class Configuration {
    private Properties properties;
    private Appenders appenders;
    private Loggers loggers;
    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    @JsonProperty("Properties")
    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @JsonProperty("Appenders")
    public Appenders getAppenders() {
        return appenders;
    }

    public void setAppenders(Appenders appenders) {
        this.appenders = appenders;
    }

    @JsonProperty("Loggers")
    public Loggers getLoggers() {
        return loggers;
    }

    public void setLoggers(Loggers loggers) {
        this.loggers = loggers;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperties(String key, Object value) {
        additionalProperties.put(key, value);
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
class Properties {
    private final Map<String, Object> properties = new LinkedHashMap<>();

    @JsonAnyGetter
    public Map<String, Object> getProperties() {
        return properties;
    }

    @JsonAnySetter
    public void setProperties(String key, Object value) {
        properties.put(key, value);
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
class Appenders {
    private final Map<String, Object> properties = new LinkedHashMap<>();

    @JsonAnyGetter
    public Map<String, Object> getProperties() {
        return properties;
    }

    @JsonAnySetter
    public void setProperties(String key, Object value) {
        properties.put(key, value);
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
class Loggers {
    private Root root;
    private List<Logger> logger = Collections.emptyList();

    @JsonProperty("Root")
    public Root getRoot() {
        return root;
    }

    public void setRoot(Root root) {
        this.root = root;
    }

    @JsonProperty("Logger")
    public List<Logger> getLogger() {
        return logger;
    }

    public void setLogger(List<Logger> logger) {
        this.logger = logger;
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
class Root {
    private String level;
    private final Map<String, Object> otherProperties = new LinkedHashMap<>();

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    @JsonAnyGetter
    public Map<String, Object> getOtherProperties() {
        return otherProperties;
    }

    @JsonAnySetter
    public void setOtherProperties(String key, Object value) {
        otherProperties.put(key, value);
    }
}

@JsonPropertyOrder({ "name", "level" })
@JsonIgnoreProperties(ignoreUnknown = true)
class Logger {
    private String name;
    private String level;
    private final Map<String, Object> otherProperties = new LinkedHashMap<>();

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("level")
    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    @JsonAnyGetter
    public Map<String, Object> getOtherProperties() {
        return otherProperties;
    }

    @JsonAnySetter
    public void setOtherProperties(String key, Object value) {
        otherProperties.put(key, value);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        Logger logger = (Logger) o;
        return Objects.equals(name, logger.name) &&
            Objects.equals(level, logger.level) &&
            Objects.equals(otherProperties, logger.otherProperties);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(name);
        result = 31 * result + Objects.hashCode(level);
        result = 31 * result + Objects.hashCode(otherProperties);
        return result;
    }
}
