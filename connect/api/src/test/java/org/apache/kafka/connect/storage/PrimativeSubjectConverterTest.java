package org.apache.kafka.connect.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by pearcem on 03/05/2017.
 */
public class PrimativeSubjectConverterTest {

    private SubjectConverter converter;

    @Before
    public void setup(){
        converter = new PrimativeSubjectConverter();
        
        Map<String, Object> configs = new HashMap<>();

        configs.put("converter.subject.MyStringHeader", "String");
        configs.put("converter.subject.MyIntegerHeader", "int32");
        configs.put("converter.subject.MyShortHeader", "int16");
        configs.put("converter.subject.MyBytesHeader", "bytes");

        converter.configure(configs, false);
    }
    
    @Test
    public void testBytes() throws UnsupportedEncodingException {

        byte[] fromKafka = "test".getBytes();
        SchemaAndValue schemaAndValue = converter.toConnectData("topic", "MyBytesHeader", fromKafka);

        assertEquals(Schema.BYTES_SCHEMA, schemaAndValue.schema());
        assertTrue(Arrays.equals(fromKafka, (byte[]) schemaAndValue.value()));

        byte[] toKafka = converter.fromConnectData("topic", "MyBytesHeader", schemaAndValue.schema(), schemaAndValue.value() );

        assertTrue(Arrays.equals(fromKafka, toKafka));

    }

    @Test
    public void testInteger() throws UnsupportedEncodingException {

        Integer integer = 42;
        IntegerSerializer integerSerializer = new IntegerSerializer();
        byte[] fromKafka = integerSerializer.serialize("", integer);
        
        SchemaAndValue schemaAndValue = converter.toConnectData("topic", "MyIntegerHeader", fromKafka);

        assertEquals(Schema.INT32_SCHEMA, schemaAndValue.schema());
        assertEquals(integer, schemaAndValue.value());

        byte[] toKafka = converter.fromConnectData("topic", "MyIntegerHeader", schemaAndValue.schema(), schemaAndValue.value() );

        assertTrue(Arrays.equals(fromKafka, toKafka));

    }

    @Test
    public void testString() throws UnsupportedEncodingException {

        String string = "test";
        StringSerializer stringSerializer = new StringSerializer();
        byte[] fromKafka = stringSerializer.serialize("", string);
        
        
        SchemaAndValue schemaAndValue = converter.toConnectData("topic", "MyStringHeader", fromKafka);

        assertEquals(Schema.STRING_SCHEMA, schemaAndValue.schema());
        assertEquals(string, schemaAndValue.value());
        
        
        byte[] toKafka = converter.fromConnectData("topic", "MyStringHeader", schemaAndValue.schema(), schemaAndValue.value() );
        
        assertTrue(Arrays.equals(fromKafka, toKafka));

        
        
    }


}
