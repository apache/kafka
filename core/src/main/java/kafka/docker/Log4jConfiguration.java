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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

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

class Configuration {
    private Properties properties;
    private Appenders appenders;
    private Loggers loggers;

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
}

class Properties {
    private List<Property> property = Collections.emptyList();

    @JsonProperty("Property")
    public List<Property> getProperty() {
        return property;
    }

    public void setProperty(List<Property> property) {
        this.property = property;
    }
}

class Property {
    private String name;
    private String value;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}

class Appenders {
    private Console console;

    private List<RollingFile> rollingFile = Collections.emptyList();

    @JsonProperty("Console")
    public Console getConsole() {
        return console;
    }

    public void setConsole(Console console) {
        this.console = console;
    }

    @JsonProperty("RollingFile")
    public List<RollingFile> getRollingFile() {
        return rollingFile;
    }

    public void setRollingFile(List<RollingFile> rollingFile) {
        this.rollingFile = rollingFile;
    }
}

class Console {
    private String name;
    private String target;
    private PatternLayout patternLayout;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    @JsonProperty("PatternLayout")
    public PatternLayout getPatternLayout() {
        return patternLayout;
    }

    public void setPatternLayout(PatternLayout patternLayout) {
        this.patternLayout = patternLayout;
    }
}

class RollingFile {
    private String name;
    private String fileName;
    private String filePattern;
    private PatternLayout patternLayout;
    private TimeBasedTriggeringPolicy timeBasedTriggeringPolicy;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFilePattern() {
        return filePattern;
    }

    public void setFilePattern(String filePattern) {
        this.filePattern = filePattern;
    }

    @JsonProperty("PatternLayout")
    public PatternLayout getPatternLayout() {
        return patternLayout;
    }

    public void setPatternLayout(PatternLayout patternLayout) {
        this.patternLayout = patternLayout;
    }

    @JsonProperty("TimeBasedTriggeringPolicy")
    public TimeBasedTriggeringPolicy getTimeBasedTriggeringPolicy() {
        return timeBasedTriggeringPolicy;
    }

    public void setTimeBasedTriggeringPolicy(TimeBasedTriggeringPolicy timeBasedTriggeringPolicy) {
        this.timeBasedTriggeringPolicy = timeBasedTriggeringPolicy;
    }
}

class PatternLayout {
    private String pattern;

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }
}

class TimeBasedTriggeringPolicy {
    private boolean modulate;
    private int interval;

    public boolean isModulate() {
        return modulate;
    }

    public void setModulate(boolean modulate) {
        this.modulate = modulate;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }
}

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

class Root {
    private String level;
    private List<AppenderRef> appenderRef = Collections.emptyList();

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    @JsonProperty("AppenderRef")
    public List<AppenderRef> getAppenderRef() {
        return appenderRef;
    }

    public void setAppenderRef(List<AppenderRef> appenderRef) {
        this.appenderRef = appenderRef;
    }
}

class Logger {
    private String name;
    private String level;
    private Boolean additivity;
    private AppenderRef appenderRef;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public Boolean isAdditivity() {
        return additivity;
    }

    public void setAdditivity(boolean additivity) {
        this.additivity = additivity;
    }

    @JsonProperty("AppenderRef")
    public AppenderRef getAppenderRef() {
        return appenderRef;
    }

    public void setAppenderRef(AppenderRef appenderRef) {
        this.appenderRef = appenderRef;
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof Logger logger)) return false;
        return Objects.equals(name, logger.name) && Objects.equals(level, logger.level) && Objects.equals(additivity, logger.additivity) && Objects.equals(appenderRef, logger.appenderRef);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(name);
        result = 31 * result + Objects.hashCode(level);
        result = 31 * result + Objects.hashCode(additivity);
        result = 31 * result + Objects.hashCode(appenderRef);
        return result;
    }
}

class AppenderRef {
    private String ref;

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }
}