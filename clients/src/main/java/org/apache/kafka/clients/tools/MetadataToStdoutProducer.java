package org.apache.kafka.clients.tools;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

/**
 * Primarily intended for use with system testing, this producer prints metadata
 * in the form of JSON to stdout on each "send" request. For example, this helps
 * with end-to-end correctness tests by making externally visible which messages have been
 * acked and which have not.
 * 
 * When used as a command-line tool, it produces a fixed number of increasing integers.
 * If logging is left enabled, log output on stdout can be easily ignored by checking
 * whether a given line is valid JSON.
 */
public class MetadataToStdoutProducer {
  OptionParser commandLineParser;
  Map<String, OptionSpec<?>> commandLineOptions = new HashMap<String, OptionSpec<?>>();
  
  String topic;
  private Properties producerProps = new Properties();
  private Producer<String, String> producer;
  private int numMessages;
  
  public MetadataToStdoutProducer(String[] args) throws IOException {
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

    
    ArgumentAcceptingOptionSpec<Integer>  numMessagesOpt = commandLineParser.accepts("num-messages", "REQUIRED: The number of messages to produce.")
        .withRequiredArg()
        .describedAs("num-messages")
        .ofType(Integer.class);
    commandLineOptions.put("num-messages", numMessagesOpt);
    
    OptionSpecBuilder helpOpt
        = commandLineParser.accepts("help", "Print this message.");
    commandLineOptions.put("help", helpOpt);
  }
  
  /** Validate command-line arguments and parse into properties object. */
  public void parseCommandLineArgs(String[] args) throws IOException {
    
    OptionSpec[] requiredArgs = new OptionSpec[]{commandLineOptions.get("topic"),
                                                 commandLineOptions.get("broker-list"),
                                                 commandLineOptions.get("num-messages")};
    
    OptionSet options = commandLineParser.parse(args);
    if (options.has(commandLineOptions.get("help"))) {
      commandLineParser.printHelpOn(System.out);
      System.exit(0);
    }
    checkRequiredArgs(commandLineParser, options, requiredArgs);
    
    this.numMessages = (Integer) options.valueOf("num-messages");
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
  public void send(String key, String value) {
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
    try {
      producer.send(record, new PrintInfoCallback(key, value));
    }
    catch (Exception e) {
      System.out.println(errorString(e, key, value));
    }
  }

  /** Need to close the producer to flush any remaining messages if we're in async mode. */
  public void close() {
    producer.close();
  }

  /** 
   * Return JSON string encapsulating basic information about the exception, as well
   * as the key and value which triggered the exception.
   */
  String errorString(Exception e, String key, String value) {
    assert e != null: "Expected non-null exception.";

    JSONObject obj = new JSONObject();
    obj.put("producer", "MetadataToStdoutProducer");
    obj.put("exception", e.getClass().toString());
    obj.put("message", e.getMessage());
    obj.put("topic", this.topic);
    obj.put("key", key);
    obj.put("value", value);
    return obj.toString();
  }
  
  String successString(RecordMetadata recordMetadata, String key, String value) {
    assert recordMetadata != null: "Expected non-null recordMetadata object.";

    JSONObject obj = new JSONObject();
    obj.put("producer", "MetadataToStdoutProducer");
    obj.put("topic", this.topic);
    obj.put("partition", recordMetadata.partition());
    obj.put("offset", recordMetadata.offset());
    obj.put("key", key);
    obj.put("value", value);
    return obj.toString();
  }
  
  /**
   * Callback which prints errors to stdout when the producer fails to send.
   */
  private class PrintInfoCallback implements Callback {
    private String key;
    private String value;
    
    PrintInfoCallback(String key, String value) {
      this.key = key;
      this.value = value;
    }
    
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e == null) {
        System.out.println(successString(recordMetadata, this.key, this.value));
      }
      else {
        System.out.println(errorString(e, this.key, this.value));
      }
    }
  }

  public static void main(String[] args) throws IOException {
    
    MetadataToStdoutProducer producer = new MetadataToStdoutProducer(args);

    for (int i = 0; i < producer.numMessages; i++) {
      producer.send(null, String.format("%d", i));
    }
    
    producer.close();
  }
}
