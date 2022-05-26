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
package org.apache.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ToStructByRegexTransformTest {
    private ToStructByRegexTransform<SourceRecord> testForm = new ToStructByRegexTransform.Value<>();

    @Test
    public void schemalessPlainTextTest() {

        Map<String, String> configMap = new HashMap<>();
        configMap.put("mapping", "protocol,domain,path");
        configMap.put("regex", "^(https?):\\/\\/([^/]*)/(.*)");

        testForm.configure(configMap);
        SourceRecord result = testForm.apply(new SourceRecord(null, null, "", 0, null, "https://kafka.apache.org/documentation/#connect"));

        assertThat(((Map<?, ?>) result.value()).get("protocol"), is("https"));
        assertThat(((Map<?, ?>) result.value()).get("domain"), is("kafka.apache.org"));
        assertThat(((Map<?, ?>) result.value()).get("path"), is("documentation/#connect"));

    }

    @Test
    public void schemaPlainTextTest() {

        Map<String, String> configMap = new HashMap<>();
        configMap.put("regex", "^(https?):\\/\\/([^/]*)/(.*)");
        configMap.put("mapping", "protocol,domain,path");

        testForm.configure(configMap);

        final Schema inputSampleSchema = SchemaBuilder.struct().name("name").version(1).doc("doc")
            .field("message", Schema.STRING_SCHEMA).build();

        final Struct testData = new Struct(inputSampleSchema).put("message", "https://kafka.apache.org/documentation/#connect");

        SourceRecord result = testForm.apply(new SourceRecord(null, null, "", 0, inputSampleSchema, testData));

        assertThat(((Struct) result.value()).get("protocol"), is("https"));
        assertThat(((Struct) result.value()).get("domain"), is("kafka.apache.org"));
        assertThat(((Struct) result.value()).get("path"), is("documentation/#connect"));

    }

    @Test
    public void apacheLogSchemalessTest() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("regex", "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(GET|POST|OPTIONS|HEAD|PUT|DELETE|PATCH) (.+?) (.+?)\" (\\d{3}) ([0-9|-]+) ([0-9|-]+) \"([^\"]+)\" \"([^\"]+)\"");
        configMap.put("mapping", "IP,RemoteUser,AuthedRemoteUser,DateTime,Method,Request,Protocol,Response,BytesSent,Ms,Referrer,UserAgent");

        Map<String, String> testData = new HashMap<>();
        testData.put("message", "111.61.73.113 - - [08/Aug/2019:18:15:29 +0900] \"OPTIONS /api/v1/service_config HTTP/1.1\" 200 - 101989 \"http://local.test.com/\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36\"");

        testForm.configure(configMap);
        SourceRecord result = testForm.apply(new SourceRecord(null, null, "", 0, null, testData));

        assertThat(((Map<?, ?>) result.value()).get("IP"), is("111.61.73.113"));
        assertThat(((Map<?, ?>) result.value()).get("RemoteUser"), is("-"));
        assertThat(((Map<?, ?>) result.value()).get("AuthedRemoteUser"), is("-"));
        assertThat(((Map<?, ?>) result.value()).get("DateTime"), is("08/Aug/2019:18:15:29 +0900"));
        assertThat(((Map<?, ?>) result.value()).get("Method"), is("OPTIONS"));
        assertThat(((Map<?, ?>) result.value()).get("Request"), is("/api/v1/service_config"));
        assertThat(((Map<?, ?>) result.value()).get("Protocol"), is("HTTP/1.1"));
        assertThat(((Map<?, ?>) result.value()).get("Response"), is("200"));
        assertThat(((Map<?, ?>) result.value()).get("BytesSent"), is("-"));
        assertThat(((Map<?, ?>) result.value()).get("Ms"), is("101989"));
        assertThat(((Map<?, ?>) result.value()).get("Referrer"), is("http://local.test.com/"));
        assertThat(((Map<?, ?>) result.value()).get("UserAgent"), is("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36"));

    }

}
