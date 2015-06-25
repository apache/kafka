package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;

/**
 * Created by yasuhiro on 6/17/15.
 */
public class Test {

  public static void main(String[] args) throws NotCopartitionedException {

    RecordCollector collector = new RecordCollector() {
      public <K, V> void send(ProducerRecord<K, V> record) {
        System.out.println(record.toString());
      }
    };

    KStreamContextImpl context = new KStreamContextImpl(collector);

    Predicate<Integer, String> lengthCheck = new Predicate<Integer, String>() {
      public boolean apply(Integer i, String s) {
        return s.length() > 1;
      }
    };

    KeyValueMapper<Integer, String, Integer, String> truncate = new KeyValueMapper<Integer, String, Integer,String>() {
      public KeyValue<Integer, String> apply(Integer i, String s) {
        if (i < s.length())
          return KeyValue.pair(i * 10, s.substring(0, i));
        else
          return KeyValue.pair(i * 10, s);
      }
      public void flush() {
        System.out.println("flushed truncate");
      }
    };

    KeyValueMapper<Integer, ArrayList<Character>, Integer, String> toCharacters = new KeyValueMapper<Integer, ArrayList<Character>, Integer, String>() {
      public KeyValue<Integer, ArrayList<Character>> apply(Integer i, String s) {
        ArrayList<Character> list = new ArrayList<Character>(s.length());
        for (char c : s.toCharArray()) {
          list.add(c);
        }
        return KeyValue.pair(i, list);
      }

      public void flush() {
        System.out.println("flushed toCharacters");
      }
    };

    ValueMapper<Iterable<Character>, String> toCharacters2 = new ValueMapper<Iterable<Character>, String>() {
      public Iterable<Character> apply(String s) {
        ArrayList<Character> list = new ArrayList<Character>(s.length());
        for (char c : s.toCharArray()) {
          list.add(c);
        }
        return list;
      }

      public void flush() {
        System.out.println("flushed toCharacters2");
      }
    };

    Processor<Integer, Character> printCharWithKey = new Processor<Integer, Character>() {
      public void apply(Integer i, Character c) {
        System.out.println(i + ":" + c);
      }
      public void punctuate(long time) {}
      public void flush() {
        System.out.println("flushed printCharWithKey");
      }
    };

    KStream<Integer,String> stream = context.from("topicABC");

    stream.filter(lengthCheck).map(truncate).flatMap(toCharacters).process(printCharWithKey);

    context.process(new ConsumerRecord<Integer, String>("topicABC", 1, 0, 1, "s"), System.currentTimeMillis());
    context.process(new ConsumerRecord<Integer, String>("topicABC", 1, 1, 2, "abcd"),  System.currentTimeMillis());
    context.process(new ConsumerRecord<Integer, String>("topicABC", 1, 2, 2, "t"),  System.currentTimeMillis());
    context.process(new ConsumerRecord<Integer, String>("topicABC", 1, 3, 3, "abcd"),  System.currentTimeMillis());
    context.process(new ConsumerRecord<Integer, String>("topicABC", 1, 4, 3, "u"), System.currentTimeMillis());

    stream = context.from("topicXYZ");
    stream.filter(lengthCheck).map(truncate).flatMapValues(toCharacters2).process(printCharWithKey);

    context.process(new ConsumerRecord<Integer, String>("topicXYZ", 1, 0, 1, "h"), System.currentTimeMillis());
    context.process(new ConsumerRecord<Integer, String>("topicXYZ", 1, 1, 2, "xyz"), System.currentTimeMillis());
    context.process(new ConsumerRecord<Integer, String>("topicXYZ", 1, 2, 3, "i"), System.currentTimeMillis());
    context.process(new ConsumerRecord<Integer, String>("topicXYZ", 1, 3, 4, "stu"), System.currentTimeMillis());
    context.process(new ConsumerRecord<Integer, String>("topicXYZ", 1, 4, 5, "j"), System.currentTimeMillis());

    KStream<Integer,String> stream1 = context.from("topic1");
    KStream<Integer,String> stream2 = context.from("topic2");

    stream1.with(new WindowByCount(3)).join(stream2.with(new WindowByCount(3)),
      new ValueJoiner<String, String, String>() {
        public String apply(String v1, String v2) {
          return v1 + v2;
        }
      }
    ).process(new Processor<Integer, String>() {
      public void apply(Integer key, String value) {
        System.out.println(key + ":" + value);
      }

      public void punctuate(long timestamp) {
      }

      public void flush() {
      }
    });

    context.process(new ConsumerRecord<Integer, String>("topic1", 1, 0, 1, "x"), System.currentTimeMillis());
    context.process(new ConsumerRecord<Integer, String>("topic2", 1, 0, 2, "A"), System.currentTimeMillis());

    context.process(new ConsumerRecord<Integer, String>("topic1", 1, 1, 2, "y"), System.currentTimeMillis());
    context.process(new ConsumerRecord<Integer, String>("topic2", 1, 1, 2, "B"), System.currentTimeMillis());

    context.process(new ConsumerRecord<Integer, String>("topic1", 1, 2, 3, "z"), System.currentTimeMillis());
    context.process(new ConsumerRecord<Integer, String>("topic2", 1, 2, 2, "C"), System.currentTimeMillis());

    context.process(new ConsumerRecord<Integer, String>("topic1", 1, 3, 4, "x"), System.currentTimeMillis());
    context.process(new ConsumerRecord<Integer, String>("topic2", 1, 3, 2, "D"), System.currentTimeMillis());

    context.process(new ConsumerRecord<Integer, String>("topic1", 1, 4, 5, "y"), System.currentTimeMillis());
    context.process(new ConsumerRecord<Integer, String>("topic2", 1, 4, 2, "E"), System.currentTimeMillis());

    context.process(new ConsumerRecord<Integer, String>("topic1", 1, 5, 6, "x"), System.currentTimeMillis());
    context.process(new ConsumerRecord<Integer, String>("topic2", 1, 5, 2, "F"), System.currentTimeMillis());

  }
}

