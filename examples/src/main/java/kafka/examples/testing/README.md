# Testing Kafka apps with mocks

Don't want to spin up a full Kafka cluster just to test your code? These examples show how to use `MockProducer` and `MockConsumer` for fast unit tests without a broker.

## Running the examples

Compile first:

```bash
./gradlew examples:compileJava
```

Then run:

```bash
./gradlew examples:runMain -PmainClass=kafka.examples.testing.MockProducerExample
./gradlew examples:runMain -PmainClass=kafka.examples.testing.MockConsumerExample
```

## MockProducer basics

`MockProducer` captures all the records you send in memory, so you can verify producer logic without a broker:

```java
MockProducer<String, String> producer = new MockProducer<>(
    true,  // auto-complete sends
    new StringSerializer(),
    new StringSerializer()
);

producer.send(new ProducerRecord<>("my-topic", "key", "value"));

// verify what got sent
assertEquals(1, producer.history().size());
assertEquals("my-topic", producer.history().get(0).topic());
```

Set the first argument to `false` if you want to manually control when sends complete (useful for testing retries and error handling). Then use `completeNext()` or `errorNext()`.

## MockConsumer basics

`MockConsumer` lets you inject test records directly:

```java
MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

// assign a partition
TopicPartition tp = new TopicPartition("my-topic", 0);
consumer.rebalance(Collections.singletonList(tp));
consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));

// add test data
consumer.addRecord(new ConsumerRecord<>("my-topic", 0, 0L, "key", "value"));

// consume it
ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
```

## What's in the examples

**MockProducerExample.java** — Topic routing, metadata checks, and error handling

**MockConsumerExample.java** — Message processing, offset commits/seeks, and rebalancing

## When to use mocks vs real brokers

Use mocks for unit tests of your logic (routing, transformations, error handling). They're fast and require no infrastructure.

Use real brokers for integration tests (end-to-end behavior, performance, transactions, exactly-once semantics).

## Writing testable code

The key is dependency injection — make your classes accept `Producer<K,V>` and `Consumer<K,V>` interfaces instead of the concrete Kafka classes:

```java
public class MyService {
    private final Producer<String, String> producer;
    
    public MyService(Producer<String, String> producer) {
        this.producer = producer;  // KafkaProducer in prod, MockProducer in tests
    }
}
```

Then tests can pass in mocks while production code uses real Kafka clients.