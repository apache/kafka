package kafka.tools;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import kafka.utils.CommandLineUtils;

public class VerboseProducer {
  OptionParser commandLineParser;
  Map<String, OptionSpec<?>> commandLineOptions = new HashMap<String, OptionSpec<?>>();
  
  String topic;
  String sync;
  private Properties producerProps = new Properties();
  private Producer<String, String> producer;
  private int numMessages;
  
  public VerboseProducer(String[] args) throws IOException {
    this.configureParser();
    this.parseCommandLineArgs(args);
    this.producer = new KafkaProducer<String, String>(producerProps);
  }

  /** Set up the command-line options. */
  private void configureParser() {
    this.commandLineParser = new OptionParser();
    ArgumentAcceptingOptionSpec<String> topicOpt = commandLineParser.accepts("topic", "REQUIRED: The topic id to produce messages to.")
        .withRequiredArg()
        .describedAs("topic")
        .ofType(String.class);
    commandLineOptions.put("topic", topicOpt);

    ArgumentAcceptingOptionSpec<String>  brokerListOpt = commandLineParser.accepts("broker-list", "REQUIRED: The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
        .withRequiredArg()
        .describedAs("broker-list")
        .ofType(String.class);
    commandLineOptions.put("broker-list", brokerListOpt);

    
    ArgumentAcceptingOptionSpec<String>  numMessagesOpt = commandLineParser.accepts("num-messages", "REQUIRED: The number of messages to produce.")
        .withRequiredArg()
        .describedAs("num-messages")
        .ofType(String.class);
    commandLineOptions.put("num-messages", numMessagesOpt);    
    
    //    syncOpt = parser.accepts("sync", "If set message send requests to the brokers are synchronously, one at a time as they arrive.")
//    val producerRequestRequiredAcks = options.valueOf(producerRequestRequiredAcksOpt).intValue()

    
    OptionSpecBuilder helpOpt
        = commandLineParser.accepts("help", "Print this message.");
    commandLineOptions.put("help", helpOpt);
  }
  
  /** Validate command-line arguments and parse into properties object. */
  public void parseCommandLineArgs(String[] args) throws IOException {
    
    OptionSpec[] requiredArgs = new OptionSpec[]{commandLineOptions.get("topic"),
                                                 commandLineOptions.get("broker-list"),
                                                 commandLineOptions.get("num-messages")};
    if(args.length == 0) {
      CommandLineUtils.printUsageAndDie(
          commandLineParser, "Read data from standard input and publish it to Kafka.");
    }
    
    OptionSet options = commandLineParser.parse(args);
    if (options.has(commandLineOptions.get("help"))) {
      commandLineParser.printHelpOn(System.out);
      System.exit(0);
    }
    checkRequiredArgs(commandLineParser, options, requiredArgs);
    
    this.numMessages = Integer.parseInt((String) options.valueOf("num-messages"));
    this.topic = (String) options.valueOf("topic");

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf("broker-list"));
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
    
    // No producer retries
    producerProps.put("retries", "0");
  }

  private static void checkRequiredArgs(
      OptionParser parser, OptionSet options, OptionSpec[] required) throws IOException 
  {
    for (OptionSpec arg : required) {
      if (!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"");
        parser.printHelpOn(System.err);
        System.exit(1);
      }
    }
  }

  /**
   * Produce a message with given value and no key.
   */
  public void send(String value) {
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);
    producer.send(record, new PrintInfoCallback(value));
  }

  /** Need to close the producer to flush any remaining messages if we're in async mode. */
  public void close() {
    producer.close();
  }

  /** 
   * Return JSON string encapsulating basic information about the exception, as well
   * as the value which triggered the exception.
   */
  String errorString(Exception e, String value) {
    return "{\"producer\": \"VerboseProducer\", " 
           + "\"exception\": \"" + e.getClass() + "\"," 
           + "\"message\": \"" + e.getMessage() + "\","
           + "\"topic\": \"" + this.topic + "\","
           + "\"value\": \"" + value + "\""
           + "}";
    
  }
  
  String successString(String value, RecordMetadata recordMetadata) {
    return "{\"producer\": \"VerboseProducer\", "
           + "\"topic\": \"" + this.topic + "\","
           + "\"partition\": \"" + recordMetadata.partition() + "\","
           + "\"offset\": \"" + recordMetadata.offset() + "\","
           + "\"value\": \"" + value + "\""
           + "}";
  }
  
  /**
   * Callback which prints errors to stdout when the producer fails to send.
   */
  private class PrintInfoCallback implements Callback {
    private String value;
    
    PrintInfoCallback(String value) {
      this.value = value;
    }
    
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e == null) {
        System.out.println(successString(this.value, recordMetadata));
      }
      else {
        System.out.println(errorString(e, this.value));
      }
    }
  }

  public static void main(String[] args) throws IOException {
    
    VerboseProducer producer = new VerboseProducer(args);

    for (int i = 0; i < producer.numMessages; i++) {
      producer.send(String.format("%d", i));
    }
    
    producer.close();
  }
}