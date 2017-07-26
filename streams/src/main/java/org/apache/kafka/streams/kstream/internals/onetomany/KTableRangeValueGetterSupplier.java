package org.apache.kafka.streams.kstream.internals.onetomany;

public interface KTableRangeValueGetterSupplier<K, V> {

	KTableRangeValueGetter<K, V> view();

}
