package org.apache.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ToStructByRegexTransformTest {
    private ToStructByRegexTransform<SourceRecord> testForm = new ToStructByRegexTransform.Value<>();

    @Test
    public void schemalessStructFieldTest(){

        Map<String, String> configMap = new HashMap<>();
        configMap.put("regex", "^(https?):\\/\\/([^/]*)/(.*)");
        configMap.put("mapping", "protocol,domain,path");
        configMap.put("struct.field", "url");

        Map<String, String> testData = new HashMap<>();
        testData.put("url", "https://kafka.apache.org/documentation/#connect");

        testForm.configure(configMap);
        SourceRecord result = testForm.apply(new SourceRecord(null, null, "", 0, null, testData));

        assertThat(((Map<String,?>)result.value()).get("protocol"), is("https"));
        assertThat(((Map<String,?>)result.value()).get("domain"), is("kafka.apache.org"));
        assertThat(((Map<String,?>)result.value()).get("path"), is("documentation/#connect"));

    }

    @Test
    public void schemalessStructTimemillisTest() {
        System.out.println(System.currentTimeMillis());
        Map<String, String> configMap = new HashMap<>();
        configMap.put("regex", "^(.{3,4})_(.*)_(pc|mw|ios|and)([0-9]{3})_([0-9]{13})");
        configMap.put("mapping", "env,serviceId,device,sequence,datetime:TIMEMILLIS");
        configMap.put("struct.field", "code");

        Map<String, String> testData = new HashMap<>();
        testData.put("code", "dev_kafka_pc001_1580372261372");

        testForm.configure(configMap);
        SourceRecord result = testForm.apply(new SourceRecord(null, null, "", 0, null, testData));

        assertThat(((Map<String, ?>)result.value()).get("env"), is("dev"));
        assertThat(((Map<String, ?>)result.value()).get("serviceId"), is("kafka"));
        assertThat(((Map<String, ?>)result.value()).get("device"), is("pc"));
        assertThat(((Map<String, ?>)result.value()).get("sequence"), is("001"));
        assertThat(((Map<String, ?>)result.value()).get("datetime"), is(new Date(1580372261372L)));
    }


    @Test
    public void schemalessPlainTextTest(){

        Map<String, String> configMap = new HashMap<>();
        configMap.put("mapping", "protocol,domain,path");
        configMap.put("regex", "^(https?):\\/\\/([^/]*)/(.*)");

        testForm.configure(configMap);
        SourceRecord result = testForm.apply(new SourceRecord(null, null, "", 0, null, "https://kafka.apache.org/documentation/#connect"));

        assertThat(((Map<String,?>)result.value()).get("protocol"), is("https"));
        assertThat(((Map<String,?>)result.value()).get("domain"), is("kafka.apache.org"));
        assertThat(((Map<String,?>)result.value()).get("path"), is("documentation/#connect"));

    }


    @Test
    public void schemalessStructFieldWithTypeTest(){

        Map<String, String> configMap = new HashMap<>();
        configMap.put("regex", "^(https?):\\/\\/([^/]*)/([0-9]+)");
        configMap.put("mapping", "protocol,domain,path:NUMBER");
        configMap.put("struct.field", "url");

        Map<String, String> testData = new HashMap<>();
        testData.put("url", "https://kafka.apache.org/1234");

        testForm.configure(configMap);
        SourceRecord result = testForm.apply(new SourceRecord(null, null, "", 0, null, testData));

        assertThat(((Map<String,?>)result.value()).get("protocol"), is("https"));
        assertThat(((Map<String,?>)result.value()).get("domain"), is("kafka.apache.org"));
        assertThat(((Map<String,?>)result.value()).get("path"), is(1234L));

    }


    @Test
    public void schemaStructFieldTest(){

        Map<String, String> configMap = new HashMap<>();
        configMap.put("regex", "^(https?):\\/\\/([^/]*)/(.*)");
        configMap.put("mapping", "protocol,domain,path");
        configMap.put("struct.field", "url");

        testForm.configure(configMap);

        final Schema inputSampleSchema = SchemaBuilder.struct().name("name").version(1).doc("doc")
                .field("url", Schema.STRING_SCHEMA).build();

        final Struct testData = new Struct(inputSampleSchema).put("url", "https://kafka.apache.org/documentation/#connect");

        SourceRecord result = testForm.apply(new SourceRecord(null, null, "", 0, inputSampleSchema, testData));

        assertThat(((Struct)result.value()).get("protocol"), is("https"));
        assertThat(((Struct)result.value()).get("domain"), is("kafka.apache.org"));
        assertThat(((Struct)result.value()).get("path"), is("documentation/#connect"));

    }

    @Test
    public void apacheLogSchemalessTest(){
        Map<String, String> configMap = new HashMap<>();
        configMap.put("regex", "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(GET|POST|OPTIONS|HEAD|PUT|DELETE|PATCH) (.+?) (.+?)\" (\\d{3}) ([0-9|-]+) ([0-9|-]+) \"([^\"]+)\" \"([^\"]+)\"");
        configMap.put("mapping", "IP,RemoteUser,AuthedRemoteUser,DateTime,Method,Request,Protocol,Response,BytesSent,Ms:NUMBER,Referrer,UserAgent");
        configMap.put("struct.field", "apacheLog");

        Map<String, String> testData = new HashMap<>();
        testData.put("apacheLog", "111.61.73.113 - - [08/Aug/2019:18:15:29 +0900] \"OPTIONS /api/v1/service_config HTTP/1.1\" 200 - 101989 \"http://local.test.com/\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36\"");

        testForm.configure(configMap);
        SourceRecord result = testForm.apply(new SourceRecord(null, null, "", 0, null, testData));

        assertThat(((Map<String,?>)result.value()).get("IP"), is("111.61.73.113"));
        assertThat(((Map<String,?>)result.value()).get("RemoteUser"), is("-"));
        assertThat(((Map<String,?>)result.value()).get("AuthedRemoteUser"), is("-"));
        assertThat(((Map<String,?>)result.value()).get("DateTime"), is("08/Aug/2019:18:15:29 +0900"));
        assertThat(((Map<String,?>)result.value()).get("Method"), is("OPTIONS"));
        assertThat(((Map<String,?>)result.value()).get("Request"), is("/api/v1/service_config"));
        assertThat(((Map<String,?>)result.value()).get("Protocol"), is("HTTP/1.1"));
        assertThat(((Map<String,?>)result.value()).get("Response"), is("200"));
        assertThat(((Map<String,?>)result.value()).get("BytesSent"), is("-"));
        assertThat(((Map<String,?>)result.value()).get("Ms"), is(101989L));
        assertThat(((Map<String,?>)result.value()).get("Referrer"), is("http://local.test.com/"));
        assertThat(((Map<String,?>)result.value()).get("UserAgent"), is("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36"));


    }

}