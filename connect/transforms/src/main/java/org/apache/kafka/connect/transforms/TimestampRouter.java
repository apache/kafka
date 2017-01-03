/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class TimestampRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("topic.format", ConfigDef.Type.STRING, "${topic}-${timestamp}", ConfigDef.Importance.HIGH,
                    "Format string which can contain ``${topic}`` and ``${timestamp}`` as placeholders for the topic and timestamp, respectively.")
            .define("timestamp.format", ConfigDef.Type.STRING, "yyyyMMdd", ConfigDef.Importance.HIGH,
                    "Format string for the timestamp that is compatible with java.text.SimpleDateFormat.");

    private static class Config extends AbstractConfig {
        public Config(Map<?, ?> originals) {
            super(CONFIG_DEF, originals, false);
        }
    }

    private String topicFormat;
    private ThreadLocal<SimpleDateFormat> timestampFormat;

    @Override
    public void init(Map<String, Object> props) {
        final Config config = new Config(props);

        topicFormat = config.getString("topic.format");

        final String timestampFormatStr = config.getString("timestamp.format");
        timestampFormat = new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat(timestampFormatStr);
            }
        };
    }

    @Override
    public R apply(R record) {
        final Long timestamp = record.timestamp();
        if (timestamp == null) {
            throw new DataException("Timestamp missing on record: " + record);
        }
        final String formattedTimestamp = timestampFormat.get().format(new Date(timestamp));
        final String updatedTopic = topicFormat.replace("${topic}", record.topic()).replace("${timestamp}", formattedTimestamp);
        return record.newRecord(
                updatedTopic,
                record.keySchema(), record.key(),
                record.valueSchema(), record.value(),
                record.timestamp()
        );
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

}
