package kafka.clients.tools;

import java.util.Arrays;
import java.util.Properties;

import kafka.clients.producer.Callback;
import kafka.clients.producer.KafkaProducer;
import kafka.clients.producer.ProducerConfig;
import kafka.clients.producer.ProducerRecord;
import kafka.clients.producer.RecordSend;
import kafka.common.ByteSerialization;

public class ProducerPerformance {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("USAGE: java " + ProducerPerformance.class.getName() + " url num_messages message_size");
            System.exit(1);
        }
        String url = args[0];
        int numMessages = Integer.parseInt(args[1]);
        int messageSize = Integer.parseInt(args[2]);
        Properties props = new Properties();
        props.setProperty(ProducerConfig.REQUIRED_ACKS_CONFIG, "1");
        props.setProperty(ProducerConfig.BROKER_LIST_CONFIG, url);
        props.setProperty(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, Integer.toString(5 * 1000));
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteSerialization.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteSerialization.class.getName());

        KafkaProducer producer = new KafkaProducer(props);
        Callback callback = new Callback() {
            public void onCompletion(RecordSend send) {
                try {
                    send.offset();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        byte[] payload = new byte[messageSize];
        Arrays.fill(payload, (byte) 1);
        ProducerRecord record = new ProducerRecord("test", payload);
        long start = System.currentTimeMillis();
        long maxLatency = -1L;
        long totalLatency = 0;
        int reportingInterval = 1000000;
        for (int i = 0; i < numMessages; i++) {
            long sendStart = System.currentTimeMillis();
            producer.send(record, null);
            long sendEllapsed = System.currentTimeMillis() - sendStart;
            maxLatency = Math.max(maxLatency, sendEllapsed);
            totalLatency += sendEllapsed;
            if (i % reportingInterval == 0) {
                System.out.printf("%d  max latency = %d ms, avg latency = %.5f\n",
                                  i,
                                  maxLatency,
                                  (totalLatency / (double) reportingInterval));
                totalLatency = 0L;
                maxLatency = -1L;
            }
        }
        long ellapsed = System.currentTimeMillis() - start;
        double msgsSec = 1000.0 * numMessages / (double) ellapsed;
        double mbSec = msgsSec * messageSize / (1024.0 * 1024.0);
        System.out.printf("%d messages sent in %d ms ms. %.2f messages per second (%.2f mb/sec).", numMessages, ellapsed, msgsSec, mbSec);
        producer.close();
    }

}
