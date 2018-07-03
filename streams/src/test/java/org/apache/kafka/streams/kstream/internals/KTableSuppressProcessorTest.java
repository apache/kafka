package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Suppression;
import org.apache.kafka.streams.kstream.Suppression.IntermediateSuppression;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;

import static org.apache.kafka.streams.kstream.Suppression.BufferFullStrategy.EMIT;

public class KTableSuppressProcessorTest {
    private static class Purchase {
        final long id;
        final long customerId;
        final int value;

        private Purchase(final long id, final long customerId, final int value) {
            this.id = id;
            this.customerId = customerId;
            this.value = value;
        }
    }

    private static class PurchaseSerde implements Serde<Purchase> {

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {

        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<Purchase> serializer() {
            return null;
        }

        @Override
        public Deserializer<Purchase> deserializer() {
            return null;
        }
    }

    public Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String purchases = "purchases";

        final KStream<String, String> siteEvents = builder.stream("/site-events");
        final KStream<Integer, Integer> keyedByPartition = siteEvents.transform(() -> new Transformer<String, String, KeyValue<Integer, Integer>>() {
            private ProcessorContext context;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<Integer, Integer> transform(final String key, final String value) {
                return new KeyValue<>(context.partition(), 1);
            }

            @Override
            public void close() {

            }
        });

        final KTable<Integer, Long> countsByPartition = keyedByPartition.groupByKey().count();

        final KGroupedTable<String, Long> singlePartition = countsByPartition.groupBy((key, value) -> new KeyValue<>("ALL", value));

        final KTable<String, Long> totalCount = singlePartition.reduce((l, r) -> l + r, (l, r) -> l - r);

        totalCount.toStream().foreach((k, v) -> {
            // k is always "ALL"
            // v is always the most recent total value
            System.out.println("The total event count is: " + v);
        });


        final KTable<Long, Purchase> input = builder.table(
            purchases,
            Consumed.with(Serdes.Long(), new PurchaseSerde())
        );

        // Fairly sloppy, but the idea is to "split" each customer id into one id per partition.
        // This way, we can first total their purchases inside each partition before aggregating them
        // across partitions
        final KTable<Long, Purchase> purchasesWithPartitionedCustomers = input.transformValues(
            () -> new ValueTransformerWithKey<Long, Purchase, Purchase>() {
                private ProcessorContext context;

                @Override
                public void init(final ProcessorContext context) {
                    this.context = context;
                }

                @Override
                public Purchase transform(final Long readOnlyKey, final Purchase purchase) {
                    final int partition = context.partition();
                    return new Purchase(
                        purchase.id,
                        purchase.customerId * 1000 + partition,
                        purchase.value
                    );
                }

                @Override
                public void close() {

                }
            });

        final KGroupedTable<Long, Integer> purchaseValueByPartitionedCustomer =
            purchasesWithPartitionedCustomers.groupBy(
                (id, purchase) -> new KeyValue<>(purchase.customerId, purchase.value)
            );

        final Suppression<Long, Integer> oncePerKeyPerSecond = Suppression.suppressIntermediateEvents(
            IntermediateSuppression
                .emitAfter(Duration.ofSeconds(1))
                .bufferKeys(5000)
                .bufferFullStrategy(EMIT)
        );
        final KTable<Long, Integer> totalValueByPartitionedCustomer =
            purchaseValueByPartitionedCustomer
                .reduce((l, r) -> l + r, (l, r) -> l - r)
                .suppress(oncePerKeyPerSecond);

        // This is where we reverse the partitioning of each customer
        final KTable<Long, Integer> aggregatedTotalValueByPartitionedCustomer =
            totalValueByPartitionedCustomer
                .groupBy((key, value) -> new KeyValue<>(key / 1000, value))
                .reduce((l, r) -> l + r, (l, r) -> l - r)
                .suppress(oncePerKeyPerSecond);

        final KTable<String, Integer> total = aggregatedTotalValueByPartitionedCustomer
            .groupBy((key, value) -> new KeyValue<>("ALL", value))
            .reduce((l, r) -> l + r, (l, r) -> l - r)
            .suppress(Suppression.suppressIntermediateEvents(
                IntermediateSuppression.emitAfter(Duration.ofSeconds(1))
            ));

        total.toStream().to("total");

        return builder.build();
    }

    public void finalResults() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input")
            .groupByKey()
            .windowedBy(
                TimeWindows.of(60_000).closeAfter(10 * 60).until(30L * 24 * 60 * 60 * 1000)
            )
            .count()
            .suppress(Suppression.finalResultsOnly());
    }
}
