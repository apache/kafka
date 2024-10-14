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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.InvalidConfigurationException;

import java.util.Properties;
import java.io.InputStream;
import java.io.FileInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProduceRequestParserFactory {
    public static final Logger log = LoggerFactory.getLogger(ProduceRequestParserFactory.class);

    public static final String PRODUCE_REQUEST_PARSER_PROPERTY = "org.apache.kafka.common.requests.ProduceRequestParser";
    public static final String PRODUCE_REQUEST_PARSER_ENV = "KAFKA_PRODUCE_REQUEST_PARSER";
    public static final String PRODUCE_REQUEST_PARSER_DEFAULT = "org.apache.kafka.common.requests.DefaultProduceRequestParser";

    private static String getProduceRequestParserClassName() {
        String produceRequestParserClassName = System.getProperty(PRODUCE_REQUEST_PARSER_PROPERTY);
        if (null != produceRequestParserClassName) {
            log.info("ProduceRequestParser class {} from property {}", produceRequestParserClassName, PRODUCE_REQUEST_PARSER_PROPERTY);
            return produceRequestParserClassName;
        }

        produceRequestParserClassName = System.getenv(PRODUCE_REQUEST_PARSER_ENV);
        if (null != produceRequestParserClassName) {
            log.info("ProduceRequestParser class {} from env {}", produceRequestParserClassName, PRODUCE_REQUEST_PARSER_ENV);
            return produceRequestParserClassName;
        }

        produceRequestParserClassName = getProduceRequestParserClassNameFromConfigFile();
        if (null != produceRequestParserClassName) {
            return produceRequestParserClassName;
        }

        produceRequestParserClassName = PRODUCE_REQUEST_PARSER_DEFAULT;
        log.info("ProduceRequestParser class {} from default {}", produceRequestParserClassName, PRODUCE_REQUEST_PARSER_DEFAULT);
        return produceRequestParserClassName;
    }

    private static String getProduceRequestParserClassNameFromConfigFile() {
        String commandLine = System.getProperty("sun.java.command");
        if(null == commandLine) {
            return null;
        }

        String[] commandLineArgs = commandLine.split("\\s+");
        String configFileName = null;
        if(commandLineArgs.length < 2) {
            return null;
        }

        configFileName = commandLineArgs[1];
        if(null == configFileName) {
            return null;
        }

        Properties properties = new Properties();
        try {
            InputStream inputStream = new FileInputStream(configFileName);
            properties.load(inputStream);
            inputStream.close();
            inputStream = null;
        } catch(Exception e) {
            log.trace("Failed to load {}", configFileName, e);
            return null;
        }

        String produceRequestParserClassName = null;
        try {
            produceRequestParserClassName = properties.getProperty(PRODUCE_REQUEST_PARSER_PROPERTY);
        } catch(Exception e) {
            log.trace("{} not found in {}", PRODUCE_REQUEST_PARSER_PROPERTY, configFileName, e);
            return null;
        }

        if(null == produceRequestParserClassName) {
            return null;
        }

        log.info("ProduceRequestParser class {} from config {}", produceRequestParserClassName, configFileName);
        return produceRequestParserClassName;
    }


    public static ProduceRequestParser getProduceRequestParser() {
        try {
            String produceRequestParserClassName = getProduceRequestParserClassName();
            return (ProduceRequestParser) Class.forName(produceRequestParserClassName).getConstructor().newInstance();
        } catch (Exception e) {
            String message = "Failed to initialize";
            log.error(message, e);
            throw new InvalidConfigurationException(message, e);
        }
    }
}

