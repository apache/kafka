# Optimizing Kafka Streams with skipRepartition

## Introduction

Apache Kafka Streams automatically triggers repartitioning when operations change the message key before a stateful
operation like `groupByKey()`, `aggregate()`, or `count()`. This behavior ensures that data is correctly distributed
across partitions to guarantee accurate calculations.

However, in many cases, the data is already partitioned correctly, making repartitioning unnecessary and inefficient. To
address this, we introduce `skipRepartition()`, an API that allows developers to bypass the repartitioning step when it
is safe to do so, resulting in reduced latency and lower infrastructure costs.

## Motivation

Imagine a streaming e-commerce application where events are partitioned by `customerId`. We want to calculate the total
amount spent by each customer:

```java
KStream<String, Order> orders = builder.stream("orders-topic")
        .selectKey((key, order) -> order.customerId) // Already correctly partitioned!
        .groupByKey() // By default, this triggers a repartition
        .aggregate(
                () -> 0,
                (key, order, total) -> total + order.amount,
                Materialized.with(Serdes.String(), Serdes.Integer())
        );
```

* **The problem:** Even though the stream is already partitioned correctly by customerId, Kafka Streams will create an
  unnecessary repartition topic, leading to increased latency and resource consumption.
* **The solution:** We can use `skipRepartition()` to prevent this:

```java
KStream<String, Order> orders = builder.stream("orders-topic")
        .selectKey((key, order) -> order.customerId)
        .skipRepartition() // Avoids unnecessary repartitioning
        .groupByKey()
        .aggregate(
                () -> 0,
                (key, order, total) -> total + order.amount,
                Materialized.with(Serdes.String(), Serdes.Integer())
        );
```

With `skipRepartition()`, Kafka Streams will skip the repartitioning step and process the aggregation directly,
optimizing performance.

## When NOT to Use skipRepartition

Although `skipRepartition()` is a powerful optimization tool, it should not be used indiscriminately. Here are some
cases where it must be avoided:

### Stream Joins

Kafka Streams relies on repartitioning during stream joins to align records by key.

```java
KStream<String, Order> orders = builder.stream("orders-topic")
        .selectKey((key, order) -> order.customerId)
        .skipRepartition()
        .join(
                builder.table("customers-topic"),
                (order, customer) -> order.amount + customer.discount,
                Joined.with(Serdes.String(), Serdes.Integer(), Serdes.String())
        ); // May produce incorrect results!
```

Joins expect a composite key produced during repartitioning. Skipping this step may cause misaligned records.

### Interactive Queries (IQ)

Interactive queries depend on the partitioning scheme created by repartitioning. Skipping repartitioning can cause query
failures or incorrect results.

```java
KStream<String, Order> orders = builder.stream("orders-topic")
        .selectKey((key, order) -> order.customerId)
        .skipRepartition()
        .groupByKey()
        .aggregate(
                () -> 0,
                (key, order, total) -> total + order.amount,
                Materialized.as("customer-total-store")
        ); // IQ queries may break
```

## When to Use skipRepartition

`skipRepartition()` is ideal in scenarios where the stream is already partitioned correctly. Examples include:

* **Aggregations** (`groupByKey()`, `aggregate()`, `count()`) when the key is already correctly set.
* **Filtering** (`filter()`) and **transformations** (`map()`, `flatMap()`) that do not alter the partitioning key.
* When the partitioning scheme is guaranteed from the source topic.

## Real-World Analogy

Imagine a smart toll system where cars pass through sensors. Each event is partitioned by license plate number, and we
need to count how many tolls each car has passed.

### Without `skipRepartition()` (Wasted Resources)

```java
KStream<String, TollEvent> tollStream = builder.stream("toll-events")
    .selectKey((key, event) -> event.licensePlate)
    .groupByKey() // Triggers unnecessary repartitioning
    .count();
```

Kafka Streams will create a new repartition topic, even though the data is already partitioned by license plate. This
leads to unnecessary overhead.

### With `skipRepartition()` (Optimized Processing)

```java
KStream<String, TollEvent> tollStream = builder.stream("toll-events")
    .selectKey((key, event) -> event.licensePlate)
    .skipRepartition() // Preserves the existing partitioning
    .groupByKey()
    .count();
```

In this case, Kafka Streams processes the aggregation directly without creating an additional repartition topic,
resulting in faster processing and lower costs. 🚀

## Testing skipRepartition

To ensure `skipRepartition()` works correctly, we can write a test to validate the topology structure and confirm that
no
repartition topic is created:

```java

@Test
public void shouldNotContainRepartitionNodeWhenSkipRepartitionUsed() {
    StreamsBuilder builder = new StreamsBuilder();

    builder.stream("input-topic")
        .selectKey((key, value) -> key)
        .skipRepartition()
        .groupByKey()
        .count()
        .toStream();

    Topology topology = builder.build();
    `TopologyDescription`description = topology.describe();

    boolean hasRepartitionTopic = description.subtopologies().stream()
        .flatMap(s -> s.nodes().stream())
        .anyMatch(node -> node.name().contains("repartition"));

    assertFalse(hasRepartitionTopic, "Topology should not contain a repartition node when using `skipRepartition()`");
}
```

### What Does This Test Do?

1. Builds a topology with `skipRepartition()`;
2. Generates the `TopologyDescription`; and
3. Asserts that no repartition node is present, confirming that the optimization is applied.

## How It Works Internally

When `skipRepartition()` is invoked, Kafka Streams introduces a `SkipRepartitionNode` in the topology. This node
inherits from ProcessorGraphNode but overrides the `isKeyChangingOperation()` method to always return false. This
signals to Kafka Streams that the key has not changed and that repartitioning is unnecessary.

### Key Implementation Details

```java
public class SkipRepartitionNode<K, V> extends ProcessorGraphNode<K, V> {

    public SkipRepartitionNode(final String nodeName, final ProcessorParameters<K, V, ?, ?> processorParameters) {
        super(nodeName, processorParameters);
        super.keyChangingOperation(false);
    }

    @Override
    public boolean isKeyChangingOperation() {
        return false;
    }

    @Override
    public void keyChangingOperation(final boolean keyChangingOperation) {
        if (keyChangingOperation) {
            throw new IllegalArgumentException("SkipRepartitionNode cannot be key-changing as it preserves partitioning.");
        }
    }

    @Override
    public String toString() {
        return String.format("SkipRepartitionNode{} %s", super.toString());
    }
}
```

## Key Insights

* `skipRepartition()` prevents repartitioning when the data is already partitioned correctly.
* It should not be used with IQ or joins, as these operations require repartitioned data.
* Internally, SkipRepartitionNode overrides isKeyChangingOperation() to ensure the Kafka Streams DSL understands that no
  repartitioning is necessary.
* Testing the topology structure provides a robust way to verify that repartitioning has been skipped correctly.

## Conclusion

`skipRepartition()` is a powerful addition to the Kafka Streams DSL, providing better performance and lower
infrastructure costs when repartitioning is unnecessary. By understanding the internal behavior and applying it
correctly, you can significantly optimize your stream processing pipelines.

