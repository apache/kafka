package org.apache.kafka.tools;

import joptsimple.OptionSpec;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.compress.GzipOutputStream;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

public class TestCompression {
    public static void main(String[] args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (TerseException e) {
            System.err.println(e.getMessage());
            return 1;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    private static void execute(String... args) throws Exception {
//        TestCompressionCommandOptions options = new TestCompressionCommandOptions(args);



        String[] types = {"gzip", "snappy", "lz4", "zstd"};
        String bootstrapServers = "localhost:9092";
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.toString(true));
            properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        // TODO: change to read from file
        String[] records = new String[1000];
        for (int i = 0; i < 1000; i++) {
            // generate random 10kb string records
            records[i] = generateRandomString(1024);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            producer.send(new ProducerRecord<>("topic1", "key", records[i]));
        }
        long end = System.currentTimeMillis();
        System.out.println("Time taken: " + (end - start) + "ms");
    }

    private static KafkaProducer<String, String> createProducer(TestCompressionCommandOptions options) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.bootstrapServers());
        properties.setProperty(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return new KafkaProducer<>(properties);
    }




    public void testCompression(){
        //test records for 5kb, 10kb, 20kb, 40kb, 80kb
        int [] recordSizes = {5 * 1024, 10 * 1024, 20 * 1024, 40 * 1024, 80 * 1024};

        for (int recordSize : recordSizes) {
            System.out.println("------------");
            System.out.println("Test with record size: " + recordSize);
            String[] records = new String[1000];
            for (int i = 0; i < 1000; i++) {
                // generate random 10kb string records
                records[i] = generateRandomString(recordSize);
            }
            int size = 0;
            for (String record : records) {
                size += record.getBytes().length;
            }
            System.out.println("Size: " + size);


            int [] bufferSizes = {512,  2048, 8 * 1024, 16 * 1024, 32 * 1024};

            for (int bufferSize : bufferSizes) {
                new TestCompression().test(bufferSize, records, size);
            }
        }
    }


    public void test(int bufferSize, String[] records, int size){
        System.out.println("Start to test with buffer size: " + bufferSize);
        // record start time and end time
        long startTime = System.currentTimeMillis();

        try {
            int sizeInBytesAfterConversion = estimateCompressedSizeInBytes(size, CompressionType.GZIP);
            ByteBufferOutputStream output = new ByteBufferOutputStream(ByteBuffer.allocate(sizeInBytesAfterConversion));
            DataOutputStream stream = new DataOutputStream(new BufferedOutputStream(new GzipOutputStream(output, bufferSize, 3), 16 * 1024));

            for (String record : records) {
                stream.write(record.getBytes());
            }
            System.out.println("Size after compression: " + output.buffer().position());
            stream.close();
            long endTime = System.currentTimeMillis();
            System.out.println("Time taken: " + (endTime - startTime) + "ms");
            // size

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static String generateRandomString(int size) {
        Random random = new Random();
        StringBuilder stringBuilder = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            char randomChar = (char) ('a' + random.nextInt(26)); // Generates random lowercase letters
            stringBuilder.append(randomChar);
        }
        return stringBuilder.toString();
    }


    private static int estimateCompressedSizeInBytes(int size, CompressionType compressionType) {
        return compressionType == CompressionType.NONE ? size : Math.min(Math.max(size / 2, 1024), 1 << 16);
    }

    static class TestCompressionCommandOptions extends CommandDefaultOptions {
        private final OptionSpec<String> bootstrapServerOpt;


        public TestCompressionCommandOptions(String[] args) {
            super(args);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: the server(s) to use for bootstrapping")
                .withRequiredArg()
                .describedAs("The server(s) to use for bootstrapping")
                .ofType(String.class).defaultsTo("localhost:9092");

            options = parser.parse(args);
        }

        private String bootstrapServers() {
            return options.valueOf(bootstrapServerOpt);
        }
    }
}
