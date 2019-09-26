package org.apache.kafka.streams.kstream.internals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreSupplier;

public class CogroupedKStreamImpl<K, V, T> extends AbstractStream<K, V> implements CogroupedKStream<K, V, T> {

  static final String AGGREGATE_NAME = "KSTREAMCOGROUP-AGGREGATE-";

  private Map<KGroupedStreamImpl<K, V>, Aggregator<? super K, ? super V, T>> groupPatterns;


   CogroupedKStreamImpl(final String name,
                           final Serde<K> keySerde,
                           final Serde<V> valueSerde,
                           final Set<String> sourceNodes,
                           final StreamsGraphNode streamsGraphNode,
                           final InternalStreamsBuilder builder) {
    super(name, keySerde, valueSerde, sourceNodes, streamsGraphNode, builder);
    this.groupPatterns = new HashMap<>();

  }

  @Override
  public CogroupedKStream<K, V, T> cogroup(final KGroupedStream<K, V> groupedStream, final Aggregator<? super K, ? super V, T> aggregator) {
    Objects.requireNonNull(groupedStream, "groupedStream can't be null");
    Objects.requireNonNull(aggregator, "aggregator can't be null");
    groupPatterns.put((KGroupedStreamImpl<K, V>) groupedStream, aggregator);
    return this;
  }

  @Override
  public KTable<K, T> aggregate(final Initializer<T> initializer,
                          final Materialized<K, T, KeyValueStore<Bytes, byte[]>> materialized) {
    Objects.requireNonNull(initializer, "initializer can't be null");
    Objects.requireNonNull(materialized, "materialized can't be null");
    final NamedInternal named = NamedInternal.empty();
    final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, AGGREGATE_NAME);
    return doAggregate(initializer, name, new MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>>(materialized, builder, AGGREGATE_NAME));
  }

  @Override
  public KTable<K, T> aggregate(final Initializer<T> initializer,
                          final StoreSupplier<KeyValueStore> storeSupplier) {
    return aggregate(initializer, Materialized.as(storeSupplier.get().name()));
  }

  //TODO: implement windowed stores
  @Override
  public KTable<Windowed<K>, T>  aggregate(final Initializer initializer, final Merger sessionMerger,
      final SessionWindows sessionWindows, final Materialized materialized) {
    return null;
  }

  //TODO: implement windowed stores
  @Override
  public KTable<Windowed<K>, T>  aggregate(final Initializer initializer, final Merger sessionMerger,
      final SessionWindows sessionWindows, final StoreSupplier storeSupplier) {
    return null;
  }

  //TODO: implement windowed stores
  @Override
  public KTable<Windowed<K>, T>  aggregate(final Initializer initializer, final Windows windows,
      final Materialized materialized) {
    return null;
  }

  //TODO: implement windowed stores
  @Override
  public KTable<Windowed<K>, T>  aggregate(final Initializer initializer, final Windows windows,
      final StoreSupplier storeSupplier) {
    return null;
  }

  private KTable<K, T> doAggregate(final Initializer<T> initializer,
                                       final String functionName,
                                       final MaterializedInternal<K, T, KeyValueStore<Bytes, byte[]>> materializedInternal) {
    final Collection<StreamsGraphNode> processors = new ArrayList<>();

    for (KGroupedStreamImpl<K, V> kGroupedStream: groupPatterns.keySet()) {
      final Aggregator<? super K, ? super V, T> aggregator = groupPatterns.get(kGroupedStream);
      final KStreamAggregate<K, V, T> kStreamAggregate = new KStreamAggregate<K, V, T>(materializedInternal.storeName(), initializer, aggregator);
      final StatefulProcessorNode<K, V> statefulProcessorNode =
          new StatefulProcessorNode<K, V>(
              functionName,
              new ProcessorParameters<K, V>(kStreamAggregate, functionName),
              new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize()
          );
      processors.add(statefulProcessorNode);
      builder.addGraphNode(kGroupedStream.streamsGraphNode.children(), statefulProcessorNode); ;
    }

    final KTableSource<K, V> tableSource = new KTableSource<>(materializedInternal.storeName(), materializedInternal.queryableStoreName());
    final StatefulProcessorNode<K, V> tableSourceNode =
        new StatefulProcessorNode<>(
            functionName,
            new ProcessorParameters<>(tableSource, functionName),
            new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize()
        );

    builder.addGraphNode(processors, tableSourceNode);

    return new KTableImpl<K, V, T>(functionName,
        materializedInternal.keySerde(),
        materializedInternal.valueSerde(),
        Collections.singleton(tableSourceNode.nodeName()),
        materializedInternal.queryableStoreName(),
        tableSource,
        tableSourceNode,
        builder);
  }
}
