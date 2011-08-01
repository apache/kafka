package kafka.perf;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Random;
import java.io.IOException;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.print.attribute.standard.Compression;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import kafka.message.CompressionCodec;
import kafka.message.NoCompressionCodec;
import kafka.perf.consumer.SimplePerfConsumer;
import kafka.perf.jmx.BrokerJmxClient;
import kafka.perf.producer.Producer;


public class KafkaPerfSimulator implements KafkaSimulatorMXBean
{
    /*Command line parser options*/

    private static final String NUM_PRODUCER = "numProducer";
    private static final String NUM_CONSUMER = "numConsumer";
    private static final String NUM_TOPIC = "numTopic";
    private static final String NUM_PARTS = "numParts";
    private static final String TEST_TIME = "time";
    private static final String KAFKA_SERVER = "kafkaServer";
    private static final String MSG_SIZE = "msgSize";
    private static final String FETCH_SIZE = "fetchSize";
    private static final String BUFFER_SIZE = "bufferSize";
    private static final String XAXIS = "xaxis";
    private static final String COMPRESSION_CODEC = "compressionCodec";
    private static final String KAFKA_SERVER_LOG_DIR = "serverLogDir";

    /* Default values */
    private static int numProducer = 20;
    private static int numTopic = 10;
    private static int numParts = 1;
    private static int numConsumers = 10;
    private static long timeToRunMs = 60000L * 1;
    private static int compressionCodec = NoCompressionCodec.codec();

    private static String kafkaServersURL = "";
    private final static int kafkaServerPort = 9092;
    private final static String kafkaServerLogDir = "/tmp/kafka-logs";
    private final static int connectionTimeOut = 100000;
    private final static int reconnectInterval = 10000;
    private static int kafkaBufferSize = 64*1024;
    private static int messageSize = 200;
    private static int fetchSize = 1024 *1024; /*1MB*/
    private static int batchSize = 200;
    final static String producerName = "-Producer-";
    final static String consumerName = "-Consumer-";
    private static String reportFileName = "";
    private static String xaxisLabel = "";
    private final static String REPORT_FILE = "reportFile";

    private Producer [] producers;
    private SimplePerfConsumer [] consumers;

    public void startProducers() throws UnknownHostException
    {
        producers = new Producer[numProducer];
        Random random = new Random();
        String [] hosts = kafkaServersURL.split(",");
        for(int i = 0; i < numProducer; i++ )
        {
            String topic;
            String kafkaServerURL;
            if(numTopic >= numProducer)
                topic = "topic" +i;
            else
                topic = "topic" + random.nextInt(numTopic);

            if(hosts.length >= numProducer)
                kafkaServerURL = hosts[i];
            else
                kafkaServerURL = hosts[random.nextInt(hosts.length)];

            producers[i] = new Producer(topic,kafkaServerURL, kafkaServerPort, kafkaBufferSize, connectionTimeOut, reconnectInterval,
                    messageSize, InetAddress.getLocalHost().getHostAddress()+ producerName +i, batchSize,
                    numParts, compressionCodec);
        }

        // Start the threads
        for(int i = 0; i < numProducer; i++)
            producers[i].start();

    }

    public void startConsumers() throws UnknownHostException
    {
        consumers = new SimplePerfConsumer[numConsumers];
        Random random = new Random();
        String [] hosts = kafkaServersURL.split(",");
        for(int i = 0; i < numConsumers; i++ )
        {
            String topic;
            String kafkaServerURL;
            if(numTopic >= numConsumers)
                topic = "topic" +i;
            else
                topic = "topic" + random.nextInt(numTopic);

            if(hosts.length >= numConsumers)
                kafkaServerURL = hosts[i];
            else
                kafkaServerURL = hosts[random.nextInt(hosts.length)];

            consumers[i] = new SimplePerfConsumer(topic,kafkaServerURL, kafkaServerPort, kafkaBufferSize, connectionTimeOut, reconnectInterval,
                    fetchSize, InetAddress.getLocalHost().getHostAddress()+ consumerName +i, this.numParts);
        }

        // Start the threads
        for(int i = 0; i < numConsumers; i++)
            consumers[i].start();
    }

    public KafkaPerfSimulator() throws UnknownHostException
    {
        startProducers();
        startConsumers();

    }


    public String getMBytesSentPs()
    {
        if(producers == null ||producers.length == 0)
            return "";
        StringBuffer msg = new StringBuffer();
        for(int i = 0; i < numProducer -1; i++)
            msg.append(producers[i].getMBytesSentPs()  +",");
        msg.append(producers[numProducer -1].getMBytesSentPs());
        System.out.println(msg);
        return msg.toString();
    }

    public double getAvgMBytesSentPs()
    {
        if(producers == null ||producers.length == 0)
            return 0;
        double total = 0;

        for(int i = 0; i < numProducer -1; i++)
            total = total + producers[i].getMBytesSentPs();
        total = total +producers[numProducer -1].getMBytesSentPs();
        return total /numProducer;
    }

    public double getAvgMessagesSentPs()
    {
        if(producers == null ||producers.length == 0)
            return 0;
        double total = 0;

        for(int i = 0; i < numProducer -1; i++)
            total = total + producers[i].getMessagesSentPs();
        total = total +producers[numProducer -1].getMessagesSentPs();
        return total /numProducer;
    }


    public String getMessagesSentPs()
    {
        if(producers == null ||producers.length == 0)
            return "";
        StringBuffer msg = new StringBuffer();
        for(int i = 0; i < numProducer -1; i++)
            msg.append(producers[i].getMessagesSentPs() +",");
        msg.append(producers[numProducer -1].getMessagesSentPs());
        System.out.println(msg);
        return msg.toString();
    }

    public String getProducers()
    {
        if(producers == null || producers.length == 0)
            return "";
        StringBuffer name = new StringBuffer();
        for(int i = 0; i < numProducer -1; i++)
            name.append(producers[i].getProducerName() +",");
        name.append(producers[numProducer -1].getProducerName());
        return name.toString();
    }

    public String getMBytesRecPs()
    {
        StringBuffer msg = new StringBuffer();
        for(int i = 0; i < numConsumers -1; i++)
            msg.append(consumers[i].getMBytesRecPs() +",");
        msg.append(consumers[numConsumers -1].getMBytesRecPs());
        //System.out.println(msg);
        return msg.toString();
    }

    public double getAvgMBytesRecPs()
    {
        if(consumers == null ||consumers.length == 0)
            return 0;

        double total = 0;
        for(int i = 0; i < numConsumers -1; i++)
            total = total + consumers[i].getMBytesRecPs();

        total = total + consumers[numConsumers -1].getMBytesRecPs();

        return total / numConsumers;
    }

    public double getAvgMessagesRecPs()
    {
        if(consumers == null ||consumers.length == 0)
            return 0;

        double total = 0;
        for(int i = 0; i < numConsumers -1; i++)
            total = total + consumers[i].getMessagesRecPs();;

        total = total + consumers[numConsumers -1].getMessagesRecPs();

        return total / numConsumers;
    }


    public String getMessagesRecPs()
    {
        StringBuffer msg = new StringBuffer();
        for(int i = 0; i < numConsumers -1; i++)
            msg.append(consumers[i].getMessagesRecPs() +",");
        msg.append(consumers[numConsumers -1].getMessagesRecPs());
        //System.out.println(msg);
        return msg.toString();
    }

    public String getConsumers()
    {
        StringBuffer name = new StringBuffer();
        for(int i = 0; i < numConsumers -1; i++)
            name.append(consumers[i].getConsumerName() +",");
        name.append(consumers[numConsumers -1].getConsumerName());
        return name.toString();
    }

    public String getXaxisLabel()
    {
        if(NUM_PRODUCER.equals(xaxisLabel))
            return "Number of Producers";

        if(NUM_CONSUMER.equals(xaxisLabel))
            return "Number of Consumers";

        if(NUM_TOPIC.equals(xaxisLabel))
            return "Number of Topics";

        return "";
    }

    public String getXAxisVal()
    {
        if(NUM_PRODUCER.equals(xaxisLabel))
            return ""+numProducer;

        if(NUM_CONSUMER.equals(xaxisLabel))
            return "" + numConsumers;

        if(NUM_TOPIC.equals(xaxisLabel))
            return ""+numTopic;

        return "";
    }

    private static OptionParser createParser()
    {
        OptionParser parser = new OptionParser();
        /* required arguments */
        parser.accepts(KAFKA_SERVER, "kafka server url").withRequiredArg().ofType(String.class);
        parser.accepts(REPORT_FILE, "report file name").withRequiredArg().ofType(String.class);
        parser.accepts(XAXIS, "report xaxis").withRequiredArg().ofType(String.class);

        /* optional values */
        parser.accepts(NUM_PRODUCER, "number of producers").withOptionalArg().ofType(Integer.class);
        parser.accepts(NUM_CONSUMER, "number of consumers").withOptionalArg().ofType(Integer.class);
        parser.accepts(NUM_TOPIC, "number of topic").withOptionalArg().ofType(Integer.class);
        parser.accepts(NUM_PARTS, "number of partitions").withOptionalArg().ofType(Integer.class);
        parser.accepts(TEST_TIME, "time to run tests").withOptionalArg().ofType(Integer.class);
        parser.accepts(MSG_SIZE, "message size").withOptionalArg().ofType(Integer.class);
        parser.accepts(FETCH_SIZE, "fetch size").withOptionalArg().ofType(Integer.class);
        parser.accepts(COMPRESSION_CODEC, "compression").withOptionalArg().ofType(Integer.class);

        parser.accepts(KAFKA_SERVER_LOG_DIR, "kafka server log directory").withOptionalArg().ofType(String.class);
        return parser;
    }

    private static void  getOptions(OptionParser parser, String[] args)
    {
        OptionSet options = parser.parse(args);

        if(!(options.hasArgument(KAFKA_SERVER) || options.hasArgument(REPORT_FILE)
                ||  options.hasArgument(XAXIS)))
            printUsage();

        if(options.hasArgument(COMPRESSION_CODEC) && !options.hasArgument(KAFKA_SERVER_LOG_DIR)) {
            System.err.println("If compression is enabled, the value for serverLogDir cannot be empty");
            System.exit(1);
        }

        kafkaServersURL = (String)options.valueOf(KAFKA_SERVER);
        reportFileName= (String)options.valueOf(REPORT_FILE);
        xaxisLabel = (String)options.valueOf(XAXIS);

        System.out.println("server: " + kafkaServersURL + " report: " + reportFileName + " xaxisLabel : "
                + xaxisLabel);

        if(options.hasArgument(NUM_PRODUCER))
            numProducer = ((Integer)options.valueOf(NUM_PRODUCER)).intValue();

        if(options.hasArgument(NUM_CONSUMER))
            numConsumers = ((Integer)options.valueOf(NUM_CONSUMER)).intValue();

        if(options.hasArgument(NUM_TOPIC))
            numTopic = ((Integer)options.valueOf(NUM_TOPIC)).intValue();

        if(options.hasArgument(NUM_PARTS))
            numParts = ((Integer)options.valueOf(NUM_PARTS)).intValue();

        if(options.hasArgument(TEST_TIME))
            timeToRunMs = ((Integer)options.valueOf(TEST_TIME)).intValue() * 60 * 1000;

        if(options.hasArgument(MSG_SIZE))
            messageSize = ((Integer)options.valueOf(MSG_SIZE)).intValue();

        if(options.hasArgument(FETCH_SIZE))
            fetchSize = ((Integer)options.valueOf(FETCH_SIZE)).intValue();

        if(options.hasArgument(COMPRESSION_CODEC))
            compressionCodec = ((Integer) options.valueOf(COMPRESSION_CODEC)).intValue();

        System.out.println("numTopic: " + numTopic);
    }

    private static void printUsage()
    {
        System.out.println("kafka server name is requied");
        System.exit(0);
    }

    public String getKafkaServersURL() {
        return kafkaServersURL;
    }

    public String getKafkaServerLogDir() {
        return kafkaServerLogDir;
    }

    public long getTotalBytesSent() {
        long bytes = 0;
        for(int i = 0;i < numProducer; i++) {
            bytes += producers[i].getTotalBytesSent();
        }
        return bytes;
    }

    public static void main(String[] args) throws Exception {

        //create parser and get options
        getOptions(createParser(), args);

        BrokerJmxClient brokerStats = new BrokerJmxClient(kafkaServersURL, 9999, timeToRunMs);
        KafkaPerfSimulator sim = new KafkaPerfSimulator();
        PerfTimer timer = new PerfTimer( brokerStats, sim, numConsumers, numProducer,numParts, numTopic, timeToRunMs,
                reportFileName, compressionCodec);
        timer.start();
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName mbeanName = new ObjectName("kafka.perf:type=Simulator");
        mbs.registerMBean(sim, mbeanName);
        while(true);
    }
}
