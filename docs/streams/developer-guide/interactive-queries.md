---
title: Interactive Queries
description: Kafka Streams interactive queries for local and remote state stores.
weight: 8
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->


Interactive queries allow you to leverage the state of your application from outside your application. The Kafka Streams enables your applications to be queryable.




The full state of your application is typically [split across many distributed instances of your application](../../architecture#streams_architecture_state), and across many state stores that are managed locally by these application instances.

![](/43/images/streams-interactive-queries-03.png)

There are local and remote components to interactively querying the state of your application.

Local state
    An application instance can query the locally managed portion of the state and directly query its own local state stores. You can use the corresponding local data in other parts of your application code, as long as it doesn't require calling the Kafka Streams API. Querying state stores is always read-only to guarantee that the underlying state stores will never be mutated out-of-band (e.g., you cannot add new entries). State stores should only be mutated by the corresponding processor topology and the input data it operates on. For more information, see Querying local state stores for an app instance.
Remote state
    

To query the full state of your application, you must connect the various fragments of the state, including:

  * query local state stores
  * discover all running instances of your application in the network and their state stores
  * communicate with these instances over the network (e.g., an RPC layer)



Connecting these fragments enables communication between instances of the same app and communication from other applications for interactive queries. For more information, see Querying remote state stores for the entire app.

Kafka Streams natively provides all of the required functionality for interactively querying the state of your application, except if you want to expose the full state of your application via interactive queries. To allow application instances to communicate over the network, you must add a Remote Procedure Call (RPC) layer to your application (e.g., REST API).

This table shows the Kafka Streams native communication support for various procedures.  
  
<table>  
<tr>  
<th>

Procedure
</th>  
<th>

Application instance
</th>  
<th>

Entire application
</th> </tr>  
<tr>  
<td>

Query local state stores of an app instance
</td>  
<td>

Supported
</td>  
<td>

Supported
</td> </tr>  
<tr>  
<td>

Make an app instance discoverable to others
</td>  
<td>

Supported
</td>  
<td>

Supported
</td> </tr>  
<tr>  
<td>

Discover all running app instances and their state stores
</td>  
<td>

Supported
</td>  
<td>

Supported
</td> </tr>  
<tr>  
<td>

Communicate with app instances over the network (RPC)
</td>  
<td>

Supported
</td>  
<td>

Not supported (you must configure)
</td> </tr> </table>

# Querying local state stores for an app instance {#querying-local-state-stores-for-an-app-instance}

A Kafka Streams application typically runs on multiple instances. The state that is locally available on any given instance is only a subset of the [application's entire state](../../architecture#streams-architecture-state). Querying the local stores on an instance will only return data locally available on that particular instance.

The method `KafkaStreams#store(...)` finds an application instance's local state stores by name and type. Note that interactive queries are not supported for [versioned state stores](/{version}/streams/developer-guide/processor-api/#versioned-key-value-state-stores) at this time.

![](/43/images/streams-interactive-queries-api-01.png)

Every application instance can directly query any of its local state stores.

The _name_ of a state store is defined when you create the store. You can create the store explicitly by using the Processor API or implicitly by using stateful operations in the DSL.

The _type_ of a state store is defined by `QueryableStoreType`. Pass a built-in implementation from [`QueryableStoreTypes`](/{version}/javadoc/org/apache/kafka/streams/state/QueryableStoreTypes.html) to [`StoreQueryParameters.fromNameAndType(...)`](/{version}/javadoc/org/apache/kafka/streams/StoreQueryParameters.html), then hand that to `KafkaStreams#store(...)`. The available built-in helpers are:

  * **`QueryableStoreTypes#keyValueStore()`** — see [Querying local key-value stores](#querying-local-key-value-stores).
  * **`QueryableStoreTypes#timestampedKeyValueStore()`** — see [Querying local key-value stores](#querying-local-key-value-stores).
  * **`QueryableStoreTypes#timestampedKeyValueStoreWithHeaders()`** — see [Header-aware stores and interactive queries](#header-aware-stores-interactive-queries).
  * **`QueryableStoreTypes#windowStore()`** — see [Querying local window stores](#querying-local-window-stores).
  * **`QueryableStoreTypes#timestampedWindowStore()`** — see [Querying local window stores](#querying-local-window-stores).
  * **`QueryableStoreTypes#timestampedWindowStoreWithHeaders()`** — see [Header-aware stores and interactive queries](#header-aware-stores-interactive-queries).
  * **`QueryableStoreTypes#sessionStore()`** — see [Querying local window stores](#querying-local-window-stores).
  * **`QueryableStoreTypes#sessionStoreWithHeaders()`** — see [Header-aware stores and interactive queries](#header-aware-stores-interactive-queries).

You can also implement your own QueryableStoreType as described in section Querying local custom state stores.

**Note**

Kafka Streams materializes one state store per stream partition. This means your application will potentially manage many underlying state stores. The API enables you to query all of the underlying stores without having to know which partition the data is in.

**Note:** For a [header-aware store](/{version}/streams/developer-guide/processor-api/#headers-in-state-stores), use the **`*WithHeaders()`** entry from the list above that corresponds to your store type when interactive query results must include record headers. See [Header-aware stores and interactive queries](#header-aware-stores-interactive-queries) to read record headers back through either the `store()` API or the IQv2 `query()` API.

## Querying local key-value stores

To query key-value state, you first build a topology that includes a state store. This example uses the DSL `count()` operator on a grouped stream, which creates a timestamped key-value store named `CountsKeyValueStore`. That store holds the latest count for each word from the topic `word-count-input`.

Note: These examples use `QueryableStoreTypes.keyValueStore()` and `ReadOnlyKeyValueStore<String, Long>`, so interactive queries return values only (the counts). The materialized store also retains timestamps; use `QueryableStoreTypes.timestampedKeyValueStore()` and `ReadOnlyKeyValueStore<String, ValueAndTimestamp<Long>>` if you need timestamps in query results.

    
    Properties  props = ...;
    StreamsBuilder builder = ...;
    KStream<String, String> textLines = ...;
    
    // Define the processing topology (here: WordCount)
    KGroupedStream<String, String> groupedByWord = textLines
      .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\W+")))
      .groupBy((key, word) -> word, Grouped.with(stringSerde, stringSerde));
    
    // Create a key-value store named "CountsKeyValueStore" for the all-time word counts
    groupedByWord.count(Materialized.<String, String, KeyValueStore<Bytes, byte[]>as("CountsKeyValueStore"));
    
    // Start an instance of the topology
    KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();

After the application has started, you can get access to "CountsKeyValueStore" and then query it via the [ReadOnlyKeyValueStore](https://github.com/apache/kafka/blob/4.3/streams/src/main/java/org/apache/kafka/streams/state/ReadOnlyKeyValueStore.java) API:
    
    
    // Get the key-value store CountsKeyValueStore
    ReadOnlyKeyValueStore<String, Long> keyValueStore =
        streams.store(StoreQueryParameters.fromNameAndType(
            "CountsKeyValueStore", QueryableStoreTypes.keyValueStore()));
    
    // Get value by key
    System.out.println("count for hello:" + keyValueStore.get("hello"));
    
    // Get the values for a range of keys available in this application instance
    KeyValueIterator<String, Long> range = keyValueStore.range("all", "streams");
    while (range.hasNext()) {
      KeyValue<String, Long> next = range.next();
      System.out.println("count for " + next.key + ": " + next.value);
    }
    
    // Get the values for all of the keys available in this application instance
    KeyValueIterator<String, Long> range = keyValueStore.all();
    while (range.hasNext()) {
      KeyValue<String, Long> next = range.next();
      System.out.println("count for " + next.key + ": " + next.value);
    }

You can also materialize the results of stateless operators by using the overloaded methods that take a `queryableStoreName` as shown in the example below:
    
    
    StreamsBuilder builder = ...;
    KTable<String, Integer> regionCounts = ...;
    
    // materialize the result of filtering corresponding to odd numbers
    // the "queryableStoreName" can be subsequently queried.
    KTable<String, Integer> oddCounts = numberLines.filter((region, count) -> (count % 2 != 0),
      Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>as("queryableStoreName"));
    
    // do not materialize the result of filtering corresponding to even numbers
    // this means that these results will not be materialized and cannot be queried.
    KTable<String, Integer> oddCounts = numberLines.filter((region, count) -> (count % 2 == 0));

## Querying local window stores

A window store will potentially have many results for any given key because the key can be present in multiple windows. However, there is only one result per window for a given key.

To query a windowed store, you first build a topology with a windowed aggregation (for example, using `windowedBy` followed by `count()`). This example uses `count()` to create a timestamped window store named `CountsWindowStore` with 1-minute windows for per-word counts.

Note: These examples use `QueryableStoreTypes.windowStore()` and `ReadOnlyWindowStore<String, Long>`, so interactive queries return values only per window. The materialized store also retains timestamps; use `QueryableStoreTypes.timestampedWindowStore()` and `ReadOnlyWindowStore<String, ValueAndTimestamp<Long>>` if you need timestamps in query results.

    
    StreamsBuilder builder = ...;
    KStream<String, String> textLines = ...;
    
    // Define the processing topology (here: WordCount)
    KGroupedStream<String, String> groupedByWord = textLines
      .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\W+")))
      .groupBy((key, word) -> word, Grouped.with(stringSerde, stringSerde));
    
    // Create a window state store named "CountsWindowStore" that contains the word counts for every minute
    groupedByWord.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60)))
      .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>as("CountsWindowStore"));

After the application has started, you can get access to "CountsWindowStore" and then query it via the [ReadOnlyWindowStore](https://github.com/apache/kafka/blob/4.3/streams/src/main/java/org/apache/kafka/streams/state/ReadOnlyWindowStore.java) API:
    
    
    // Get the window store named "CountsWindowStore"
    ReadOnlyWindowStore<String, Long> windowStore =
        streams.store(StoreQueryParameters.fromNameAndType(
            "CountsWindowStore", QueryableStoreTypes.windowStore()));
    
    // Fetch values for the key "world" for all of the windows available in this application instance.
    // To get *all* available windows we fetch windows from the beginning of time until now.
    Instant timeFrom = Instant.ofEpochMilli(0); // beginning of time = oldest available
    Instant timeTo = Instant.now(); // now (in processing-time)
    WindowStoreIterator<Long> iterator = windowStore.fetch("world", timeFrom, timeTo);
    while (iterator.hasNext()) {
      KeyValue<Long, Long> next = iterator.next();
      long windowTimestamp = next.key;
      System.out.println("Count of 'world' @ time " + windowTimestamp + " is " + next.value);
    }

## Header-aware stores and interactive queries {#header-aware-stores-interactive-queries}

A [header-aware store](/{version}/streams/developer-guide/processor-api/#headers-in-state-stores) — built with a `*WithHeaders` supplier and its matching builder ([KIP-1271](../../upgrade-guide/#kip-1271-headers-aware-stores)) — persists each record's [headers](</{version}/javadoc/org/apache/kafka/streams/processor/api/Record.html#headers()>) alongside its value (and, for key-value and window stores, its timestamp). This section shows how to read those headers back interactively, through both the `store()` API and the IQv2 `query()` API.

### Reading headers with the `store()` API

Look up the store with the `*WithHeaders()` entry from `QueryableStoreTypes` that matches your store type. The returned `ReadOnly*Store` surfaces the headers as part of its value type: [ValueTimestampHeaders](/{version}/javadoc/org/apache/kafka/streams/state/ValueTimestampHeaders.html) for key-value and window stores, and [AggregationWithHeaders](/{version}/javadoc/org/apache/kafka/streams/state/AggregationWithHeaders.html) for session stores. These examples assume a header-aware store built with a `*WithHeaders` supplier, as shown in [Headers in State Stores](/{version}/streams/developer-guide/processor-api/#headers-in-state-stores). There are only three such helpers — `timestampedKeyValueStoreWithHeaders()`, `timestampedWindowStoreWithHeaders()`, and `sessionStoreWithHeaders()`; there is no `*WithHeaders()` helper for a plain (non-timestamped) key-value or window store. What the store returns also depends on the supplier the `*WithHeaders` builder wraps (see the store-build table below): on the adapter paths the `store()` API degrades silently — empty headers, or a `-1` timestamp — rather than failing the way the IQv2 `query()` API does.
    
    
    // Key-value store built with a *WithHeaders supplier
    ReadOnlyKeyValueStore<String, ValueTimestampHeaders<Long>> keyValueStore =
        streams.store(StoreQueryParameters.fromNameAndType(
            "counts-store", QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()));
    
    ValueTimestampHeaders<Long> vth = keyValueStore.get("hello");
    if (vth != null) {
      System.out.println("value:     " + vth.value());
      System.out.println("timestamp: " + vth.timestamp());
      System.out.println("headers:   " + vth.headers());
    }
    
    // Window store built with a *WithHeaders supplier
    ReadOnlyWindowStore<String, ValueTimestampHeaders<Long>> windowStore =
        streams.store(StoreQueryParameters.fromNameAndType(
            "counts-window-store", QueryableStoreTypes.timestampedWindowStoreWithHeaders()));
    
    // fetch returns a WindowStoreIterator whose values carry headers
    try (WindowStoreIterator<ValueTimestampHeaders<Long>> it =
             windowStore.fetch("hello", Instant.ofEpochMilli(0), Instant.now())) {
      while (it.hasNext()) {
        ValueTimestampHeaders<Long> wv = it.next().value;
        System.out.println("value: " + wv.value() + " headers: " + wv.headers());
      }
    }

Session stores return `AggregationWithHeaders<V>`, which exposes the aggregated value via `aggregation()` (not `value()`) and the headers via `headers()`.
    
    
    // Session store built with a *WithHeaders supplier
    ReadOnlySessionStore<String, AggregationWithHeaders<Long>> sessionStore =
        streams.store(StoreQueryParameters.fromNameAndType(
            "counts-session-store", QueryableStoreTypes.sessionStoreWithHeaders()));
    
    try (KeyValueIterator<Windowed<String>, AggregationWithHeaders<Long>> it =
             sessionStore.fetch("hello")) {
      while (it.hasNext()) {
        AggregationWithHeaders<Long> awh = it.next().value;
        System.out.println("aggregation: " + awh.aggregation());
        System.out.println("headers:     " + awh.headers());
      }
    }

### Reading headers with the IQv2 `query()` API

Interactive Queries v2 (IQv2) is the query-based interactive-queries API: instead of accessing a store object directly, you build a `Query`, wrap it in a [StateQueryRequest](/{version}/javadoc/org/apache/kafka/streams/query/StateQueryRequest.html), and run it with `KafkaStreams#query(...)`. The call returns a [StateQueryResult](/{version}/javadoc/org/apache/kafka/streams/query/StateQueryResult.html) that holds a per-partition [QueryResult](/{version}/javadoc/org/apache/kafka/streams/query/QueryResult.html): use `getOnlyPartitionResult()` for a single-key lookup, or `getPartitionResults()` for the full `Map<Integer, QueryResult<R>>`. Each `QueryResult` exposes the query result via `getResult()` and its data-freshness `Position` via `getPosition()`. To require a minimum freshness on the request side, bound it with `StateQueryRequest#withPositionBound(...)`; a not-up-to-bound failure (described in the behavior notes below) means the store had not yet reached that bound.

Before [KIP-1356](../../upgrade-guide/#kip-1356-iqv2-header-queries), no IQv2 query type exposed record headers. [KIP-1356](../../upgrade-guide/#kip-1356-iqv2-header-queries) adds four `@Evolving` query types whose results carry headers. Each returns a [ReadOnlyRecord](/{version}/javadoc/org/apache/kafka/streams/processor/api/ReadOnlyRecord.html) — a read-only view exposing `key()`, `value()`, `timestamp()`, and `headers()` — or, for the range and window queries, a closeable [ReadOnlyRecordIterator](/{version}/javadoc/org/apache/kafka/streams/state/ReadOnlyRecordIterator.html) of such records. `headers()` is never null (an empty `Headers` when the record had none) and must be treated as read-only: records served as IQv2 results have their headers frozen, so adding or removing a header (for example `add(...)`) throws `IllegalStateException`. The freeze is shallow, though — the byte array behind an individual header value can still be mutated in place, so treat header values as read-only too.

`TimestampedKeyWithHeadersQuery` is a single-key lookup against a header-aware key-value store, parallel to `TimestampedKeyQuery`:
    
    
    TimestampedKeyWithHeadersQuery<String, Long> query =
        TimestampedKeyWithHeadersQuery.withKey("hello");
    
    StateQueryRequest<ReadOnlyRecord<String, Long>> request =
        StateQueryRequest.inStore("counts-store").withQuery(query);
    
    StateQueryResult<ReadOnlyRecord<String, Long>> result = streams.query(request);
    QueryResult<ReadOnlyRecord<String, Long>> partitionResult = result.getOnlyPartitionResult();
    if (partitionResult != null && partitionResult.isSuccess()) {
      ReadOnlyRecord<String, Long> record = partitionResult.getResult();
      if (record != null) {
        System.out.println("value:   " + record.value());
        System.out.println("headers: " + record.headers());
      }
    }

Chain `skipCache()` when building the query — `TimestampedKeyWithHeadersQuery.withKey("hello").skipCache()` — to bypass the record cache and read directly from the underlying store. The query types are immutable, so `skipCache()` returns a new query rather than mutating the one you already built (of the four header-aware queries, only this single-key one offers `skipCache()`).

`TimestampedRangeWithHeadersQuery` is a key-range scan, parallel to `TimestampedRangeQuery`. It returns a `ReadOnlyRecordIterator`, so close it when done (for example, with try-with-resources). A range can span several local partitions, so iterate `getPartitionResults()`:
    
    
    TimestampedRangeWithHeadersQuery<String, Long> query =
        TimestampedRangeWithHeadersQuery.withRange("a", "n");
    
    StateQueryRequest<ReadOnlyRecordIterator<String, Long>> request =
        StateQueryRequest.inStore("counts-store").withQuery(query);
    
    StateQueryResult<ReadOnlyRecordIterator<String, Long>> result = streams.query(request);
    for (QueryResult<ReadOnlyRecordIterator<String, Long>> partition : result.getPartitionResults().values()) {
      if (partition.isFailure()) {
        System.out.println("failed: " + partition.getFailureReason() + " - " + partition.getFailureMessage());
        continue;
      }
      try (ReadOnlyRecordIterator<String, Long> iterator = partition.getResult()) {
        while (iterator.hasNext()) {
          ReadOnlyRecord<String, Long> record = iterator.next();
          System.out.println(record.key() + " -> " + record.value() + " " + record.headers());
        }
      }
    }

Use `withLowerBound`, `withUpperBound`, or `withNoBounds` for open-ended or full scans. Results are unordered by default; call `withAscendingKeys()` or `withDescendingKeys()` to fix the order, which is defined over the serialized `byte[]` of the keys rather than their logical order.

`TimestampedWindowKeyWithHeadersQuery` fetches all windows for a single key within a window-start range from a header-aware window store. It parallels `WindowKeyQuery`, but with a different result shape: `WindowKeyQuery` returns a `WindowStoreIterator<V>` keyed by the window-start `long`, whereas this query returns a `ReadOnlyRecordIterator<Windowed<K>, V>` whose records are keyed by `Windowed<K>` (the window lives in the key; `timestamp()` is the stored record event-time). Build and consume it as for the range query above, but note the `Windowed<String>` in the request and result types:
    
    
    TimestampedWindowKeyWithHeadersQuery<String, Long> query =
        TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(
            "hello", Instant.ofEpochMilli(0), Instant.now());
    
    StateQueryRequest<ReadOnlyRecordIterator<Windowed<String>, Long>> request =
        StateQueryRequest.inStore("counts-window-store").withQuery(query);
    
    StateQueryResult<ReadOnlyRecordIterator<Windowed<String>, Long>> result = streams.query(request);
    // Iterate result.getPartitionResults() and close each ReadOnlyRecordIterator, as in the range example.

`TimestampedWindowRangeWithHeadersQuery` is parallel to `WindowRangeQuery` and has two forms. Use `withWindowStartRange(timeFrom, timeTo)` against a header-aware window store to fetch every key across a window-start range, or `withKey(key)` against a header-aware session store to fetch all sessions for a key (for session results, `timestamp()` is the session-window end). As with `WindowRangeQuery`, each store accepts only its corresponding form; submitting the wrong form fails with an unknown-query-type error. Both forms are `Query<ReadOnlyRecordIterator<Windowed<K>, V>>` — including the session `withKey` form, whose records are keyed by the session's `Windowed<K>`.
    
    
    // Window store: every key across a window-start range
    TimestampedWindowRangeWithHeadersQuery<String, Long> byWindow =
        TimestampedWindowRangeWithHeadersQuery.withWindowStartRange(
            Instant.ofEpochMilli(0), Instant.now());
    
    // Session store: all sessions for one key
    TimestampedWindowRangeWithHeadersQuery<String, Long> byKey =
        TimestampedWindowRangeWithHeadersQuery.withKey("hello");
    
    // Both forms have the same result type (element type ReadOnlyRecord<Windowed<String>, Long>),
    // but each must target its own store type — submitting the wrong form fails with an unknown-query-type error:
    StateQueryRequest<ReadOnlyRecordIterator<Windowed<String>, Long>> windowRequest =
        StateQueryRequest.inStore("counts-window-store").withQuery(byWindow);
    StateQueryRequest<ReadOnlyRecordIterator<Windowed<String>, Long>> sessionRequest =
        StateQueryRequest.inStore("counts-session-store").withQuery(byKey);
    
    StateQueryResult<ReadOnlyRecordIterator<Windowed<String>, Long>> result = streams.query(windowRequest);
    // Iterate result.getPartitionResults() and close each ReadOnlyRecordIterator, as in the range example.

**Behavior notes**

  * **Window start range is required.** As with the existing window queries, `TimestampedWindowKeyWithHeadersQuery` and the `withWindowStartRange` form of `TimestampedWindowRangeWithHeadersQuery` require a closed window-start range — both `timeFrom` and `timeTo` must be present, and both bounds are inclusive.
  * **Close iterators exactly once.** The range and window queries return a `ReadOnlyRecordIterator`; close it when you are done — always, even if a `next()` call throws partway through — or the underlying store iterator (and the store's `num-open-iterators` metric) leaks. A try-with-resources block does this correctly. The iterator does not support `remove()`.
  * **Read-your-writes applies only to the single-key query.** Only `TimestampedKeyWithHeadersQuery` reads through the record cache, so it sees a write that has not yet been flushed to the store — unless you call `skipCache()`, or the entry has already been flushed. The range, window, and session queries bypass the cache entirely, so a not-yet-flushed write is invisible to them and, with a position bound, fails with a not-up-to-bound error.

**How the store was built determines what the queries return.** For key-value and window stores, the outcome depends on the supplier the `*WithHeaders` builder wraps:

<table>
<tr>
<th>

`*WithHeaders` store built over…
</th>
<th>

Headers
</th>
<th>

Query outcome
</th> </tr>
<tr>
<td>

Native (RocksDB) header supplier
</td>
<td>

Returned
</td>
<td>

All succeed
</td> </tr>
<tr>
<td>

In-memory non-header supplier
</td>
<td>

Returned (a marker keeps the header-format bytes verbatim)
</td>
<td>

All succeed
</td> </tr>
<tr>
<td>

Persistent *timestamped* non-header supplier
</td>
<td>

Empty
</td>
<td>

All succeed
</td> </tr>
<tr>
<td>

Persistent *plain* non-header supplier
</td>
<td>

Returned while cache-served; otherwise —
</td>
<td>

Store-served point query fails with a store-exception error (a cache-served read still succeeds, with real value, timestamp, and headers, until the cache is flushed or `skipCache()` is used); the range, window-key, and `withWindowStartRange` window-range iterators throw a `StreamsException` mid-iteration. (The `withKey` form of the window-range query targets session stores, covered in the note below.)
</td> </tr>
<tr>
<td>

*(no `*WithHeaders` builder at all)*
</td>
<td>

—
</td>
<td>

Unknown-query-type
</td> </tr> </table>

Session stores have no plain/timestamped split, but they do split on persistence: a `*WithHeaders` session store built over a non-header **persistent** supplier uses a single adapter and behaves like the *timestamped* row above (empty `headers()`), while one built over an **in-memory** supplier uses a marker and behaves like the *in-memory* row (headers returned). Either way, the `withKey` form of `TimestampedWindowRangeWithHeadersQuery` (the session-store form) never throws — a session window always carries a valid end timestamp — so it surfaces a `null` `value()` only where the stored value itself is null.

The pre-existing IQv2 query types (`KeyQuery`, `TimestampedKeyQuery`, `RangeQuery`, `TimestampedRangeQuery`, `WindowKeyQuery`, `WindowRangeQuery`) also run against header-aware stores, returning header-stripped results, and behave identically whether the header store was built on the native or the *timestamped* adapter path. The *plain* adapter is not equivalent: it surfaces a `-1` timestamp rather than a real event-time, and its window queries return plain values instead of `ValueAndTimestamp`.

## Querying local custom state stores

**Note**

Only the [Processor API](/{version}/streams/developer-guide/processor-api/#implementing-custom-state-stores) supports custom state stores.

Before querying the custom state stores you must implement these interfaces:

  * Your custom state store must implement `StateStore`.
  * You must have an interface to represent the operations available on the store.
  * You must provide an implementation of `StoreBuilder` for creating instances of your store.
  * It is recommended that you provide an interface that restricts access to read-only operations. This prevents users of this API from mutating the state of your running Kafka Streams application out-of-band.



The class/interface hierarchy for your custom store might look something like:
    
    
    public class MyCustomStore<K,V> implements StateStore, MyWriteableCustomStore<K,V> {
      // implementation of the actual store
    }
    
    // Read-write interface for MyCustomStore
    public interface MyWriteableCustomStore<K,V> extends MyReadableCustomStore<K,V> {
      void write(K Key, V value);
    }
    
    // Read-only interface for MyCustomStore
    public interface MyReadableCustomStore<K,V> {
      V read(K key);
    }
    
    public class MyCustomStoreBuilder implements StoreBuilder {
      // implementation of the supplier for MyCustomStore
    }

To make this store queryable you must:

  * Provide an implementation of [QueryableStoreType](https://github.com/apache/kafka/blob/4.3/streams/src/main/java/org/apache/kafka/streams/state/QueryableStoreType.java).
  * Provide a wrapper class that has access to all of the underlying instances of the store and is used for querying.



Here is how to implement `QueryableStoreType`:
    
    
    public class MyCustomStoreType<K,V> implements QueryableStoreType<MyReadableCustomStore<K,V>> {
    
      // Only accept StateStores that are of type MyCustomStore
      public boolean accepts(final StateStore stateStore) {
        return stateStore instanceOf MyCustomStore;
      }
    
      public MyReadableCustomStore<K,V> create(final StateStoreProvider storeProvider, final String storeName) {
          return new MyCustomStoreTypeWrapper(storeProvider, storeName, this);
      }
    
    }

A wrapper class is required because each instance of a Kafka Streams application may run multiple stream tasks and manage multiple local instances of a particular state store. The wrapper class hides this complexity and lets you query a "logical" state store by name without having to know about all of the underlying local instances of that state store.

When implementing your wrapper class you must use the [StateStoreProvider](https://github.com/apache/kafka/blob/4.3/streams/src/main/java/org/apache/kafka/streams/state/internals/StateStoreProvider.java) interface to get access to the underlying instances of your store. `StateStoreProvider#stores(String storeName, QueryableStoreType<T> queryableStoreType)` returns a `List` of state stores with the given storeName and of the type as defined by `queryableStoreType`.

Here is an example implementation of the wrapper:
    
    
    // We strongly recommended implementing a read-only interface
    // to restrict usage of the store to safe read operations!
    public class MyCustomStoreTypeWrapper<K,V> implements MyReadableCustomStore<K,V> {
    
      private final QueryableStoreType<MyReadableCustomStore<K, V>> customStoreType;
      private final String storeName;
      private final StateStoreProvider provider;
    
      public CustomStoreTypeWrapper(final StateStoreProvider provider,
                                  final String storeName,
                                  final QueryableStoreType<MyReadableCustomStore<K, V>> customStoreType) {
    
        // ... assign fields ...
      }
    
      // Implement a safe read method
      @Override
      public V read(final K key) {
        // Get all the stores with storeName and of customStoreType
        final List<MyReadableCustomStore<K, V>> stores = provider.getStores(storeName, customStoreType);
        // Try and find the value for the given key
        final Optional<V> value = stores.stream().filter(store -> store.read(key) != null).findFirst();
        // Return the value if it exists
        return value.orElse(null);
      }
    
    }

You can now find and query your custom store:
    
    
    Topology topology = ...;
    ProcessorSupplier processorSuppler = ...;
    
    // Create CustomStoreSupplier for store name the-custom-store
    MyCustomStoreBuilder customStoreBuilder = new MyCustomStoreBuilder("the-custom-store") //...;
    // Add the source topic
    topology.addSource("input", "inputTopic");
    // Add a custom processor that reads from the source topic
    topology.addProcessor("the-processor", processorSupplier, "input");
    // Connect your custom state store to the custom processor above
    topology.addStateStore(customStoreBuilder, "the-processor");
    
    KafkaStreams streams = new KafkaStreams(topology, config);
    streams.start();
    
    // Get access to the custom store
    MyReadableCustomStore<String,String> store =
        streams.store(StoreQueryParameters.fromNameAndType("the-custom-store", new MyCustomStoreType<String,String>()));
    // Query the store
    String value = store.read("key");

# Querying remote state stores for the entire app

To query remote states for the entire app, you must expose the application's full state to other applications, including applications that are running on different machines.

For example, you have a Kafka Streams application that processes user events in a multi-player video game, and you want to retrieve the latest status of each user directly and display it in a mobile app. Here are the required steps to make the full state of your application queryable:

  1. Add an RPC layer to your application so that the instances of your application can be interacted with via the network (e.g., a REST API, Thrift, a custom protocol, and so on). The instances must respond to interactive queries. You can follow the reference examples provided to get started.
  2. Expose the RPC endpoints of your application's instances via the `application.server` configuration setting of Kafka Streams. Because RPC endpoints must be unique within a network, each instance has its own value for this configuration setting. This makes an application instance discoverable by other instances.
  3. In the RPC layer, discover remote application instances and their state stores and query locally available state stores to make the full state of your application queryable. The remote application instances can forward queries to other app instances if a particular instance lacks the local data to respond to a query. The locally available state stores can directly respond to queries.



![](/43/images/streams-interactive-queries-api-02.png)

Discover any running instances of the same application as well as the respective RPC endpoints they expose for interactive queries

## Adding an RPC layer to your application

There are many ways to add an RPC layer. The only requirements are that the RPC layer is embedded within the Kafka Streams application and that it exposes an endpoint that other application instances and applications can connect to.

## Exposing the RPC endpoints of your application

To enable remote state store discovery in a distributed Kafka Streams application, you must set the [configuration property](../config-streams#streams-developer-guide-required-configs) in the config properties. The `application.server` property defines a unique `host:port` pair that points to the RPC endpoint of the respective instance of a Kafka Streams application. The value of this configuration property will vary across the instances of your application. When this property is set, Kafka Streams will keep track of the RPC endpoint information for every instance of an application, its state stores, and assigned stream partitions through instances of [StreamsMetadata](/{version}/javadoc/org/apache/kafka/streams/state/StreamsMetadata.html).

**Tip**

Consider leveraging the exposed RPC endpoints of your application for further functionality, such as piggybacking additional inter-application communication that goes beyond interactive queries.

This example shows how to configure and run a Kafka Streams application that supports the discovery of its state stores.
    
    
    Properties props = new Properties();
    // Set the unique RPC endpoint of this application instance through which it
    // can be interactively queried.  In a real application, the value would most
    // probably not be hardcoded but derived dynamically.
    String rpcEndpoint = "host1:4460";
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, rpcEndpoint);
    // ... further settings may follow here ...
    
    StreamsBuilder builder = new StreamsBuilder();
    
    KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, "word-count-input");
    
    final KGroupedStream<String, String> groupedByWord = textLines
        .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\W+")))
        .groupBy((key, word) -> word, Grouped.with(stringSerde, stringSerde));
    
    // This call to `count()` creates a state store named "word-count".
    // The state store is discoverable and can be queried interactively.
    groupedByWord.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>as("word-count"));
    
    // Start an instance of the topology
    KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();
    
    // Then, create and start the actual RPC service for remote access to this
    // application instance's local state stores.
    //
    // This service should be started on the same host and port as defined above by
    // the property `StreamsConfig.APPLICATION_SERVER_CONFIG`.  The example below is
    // fictitious, but we provide end-to-end demo applications (such as KafkaMusicExample)
    // that showcase how to implement such a service to get you started.
    MyRPCService rpcService = ...;
    rpcService.listenAt(rpcEndpoint);

## Discovering and accessing application instances and their local state stores

The following methods return [StreamsMetadata](/{version}/javadoc/org/apache/kafka/streams/state/StreamsMetadata.html) objects, which provide meta-information about application instances such as their RPC endpoint and locally available state stores.

  * `KafkaStreams#allMetadata()`: find all instances of this application
  * `KafkaStreams#allMetadataForStore(String storeName)`: find those applications instances that manage local instances of the state store "storeName"
  * `KafkaStreams#metadataForKey(String storeName, K key, Serializer<K> keySerializer)`: using the default stream partitioning strategy, find the one application instance that holds the data for the given key in the given state store
  * `KafkaStreams#metadataForKey(String storeName, K key, StreamPartitioner<K, ?> partitioner)`: using `partitioner`, find the one application instance that holds the data for the given key in the given state store



Attention

If `application.server` is not configured for an application instance, then the above methods will not find any [StreamsMetadata](/{version}/javadoc/org/apache/kafka/streams/state/StreamsMetadata.html) for it.

For example, we can now find the `StreamsMetadata` for the state store named "word-count" that we defined in the code example shown in the previous section:
    
    
    KafkaStreams streams = ...;
    // Find all the locations of local instances of the state store named "word-count"
    Collection<StreamsMetadata> wordCountHosts = streams.allMetadataForStore("word-count");
    
    // For illustrative purposes, we assume using an HTTP client to talk to remote app instances.
    HttpClient http = ...;
    
    // Get the word count for word (aka key) 'alice': Approach 1
    //
    // We first find the one app instance that manages the count for 'alice' in its local state stores.
    StreamsMetadata metadata = streams.metadataForKey("word-count", "alice", Serdes.String().serializer());
    // Then, we query only that single app instance for the latest count of 'alice'.
    // Note: The RPC URL shown below is fictitious and only serves to illustrate the idea.  Ultimately,
    // the URL (or, in general, the method of communication) will depend on the RPC layer you opted to
    // implement.  Again, we provide end-to-end demo applications (such as KafkaMusicExample) that showcase
    // how to implement such an RPC layer.
    Long result = http.getLong("http://" + metadata.host() + ":" + metadata.port() + "/word-count/alice");
    
    // Get the word count for word (aka key) 'alice': Approach 2
    //
    // Alternatively, we could also choose (say) a brute-force approach where we query every app instance
    // until we find the one that happens to know about 'alice'.
    Optional<Long> result = streams.allMetadataForStore("word-count")
        .stream()
        .map(streamsMetadata -> {
            // Construct the (fictituous) full endpoint URL to query the current remote application instance
            String url = "http://" + streamsMetadata.host() + ":" + streamsMetadata.port() + "/word-count/alice";
            // Read and return the count for 'alice', if any.
            return http.getLong(url);
        })
        .filter(s -> s != null)
        .findFirst();

At this point the full state of the application is interactively queryable:

  * You can discover the running instances of the application and the state stores they manage locally.
  * Through the RPC layer that was added to the application, you can communicate with these application instances over the network and query them for locally available state.
  * The application instances are able to serve such queries because they can directly query their own local state stores and respond via the RPC layer.
  * Collectively, this allows us to query the full state of the entire application.



To see an end-to-end application with interactive queries, review the demo applications.

  * [Documentation](/documentation)
  * [Kafka Streams](/documentation/streams)
  * [Developer Guide](/documentation/streams/developer-guide/)


