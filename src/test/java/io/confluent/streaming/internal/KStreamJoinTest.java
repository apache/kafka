package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import io.confluent.streaming.testutil.MockKStreamContext;
import io.confluent.streaming.testutil.MockKStreamTopology;
import io.confluent.streaming.testutil.TestProcessor;
import io.confluent.streaming.testutil.UnlimitedWindow;
import io.confluent.streaming.util.Util;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KStreamJoinTest {

  private String topicName = "topic";

  private KStreamMetadata streamMetadata = new KStreamMetadata(Collections.singletonMap(topicName, new PartitioningInfo(1)));

  private ValueJoiner<String, String, String> joiner = new ValueJoiner<String, String, String>() {
    @Override
    public String apply(String value1, String value2) {
      return value1 + "+" + value2;
    }
  };

  private ValueMapper<String, String> valueMapper = new ValueMapper<String, String>() {
    @Override
    public String apply(String value) {
      return "#" + value;
    }
  };

  private ValueMapper<Iterable<String>, String> valueMapper2 = new ValueMapper<Iterable<String>, String>() {
    @Override
    public Iterable<String> apply(String value) {
      return (Iterable<String>) Util.mkSet(value);
    }
  };

  private KeyValueMapper<Integer, String, Integer, String> keyValueMapper =
    new KeyValueMapper<Integer, String, Integer, String>() {
      @Override
      public KeyValue<Integer, String> apply(Integer key, String value) {
        return KeyValue.pair(key, value);
      }
    };

  KeyValueMapper<Integer, Iterable<String>, Integer, String> keyValueMapper2 =
    new KeyValueMapper<Integer, Iterable<String>, Integer, String>() {
      @Override
      public KeyValue<Integer, Iterable<String>> apply(Integer key, String value) {
        return KeyValue.pair(key, (Iterable<String>) Util.mkSet(value));
      }
    };

  @Test
  public void testJoin() {

    final int[] expectedKeys = new int[]{0, 1, 2, 3};

    KStreamSource<Integer, String> stream1;
    KStreamSource<Integer, String> stream2;
    KStreamWindowed<Integer, String> windowed1;
    KStreamWindowed<Integer, String> windowed2;
    TestProcessor<Integer, String> processor;
    String[] expected;

<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
>>>>>>> new api model
    processor = new TestProcessor<>();
    stream1 = new KStreamSource<>(null, initializer);
    stream2 = new KStreamSource<>(null, initializer);
    windowed1 = stream1.with(new UnlimitedWindow<Integer, String>());
    windowed2 = stream2.with(new UnlimitedWindow<Integer, String>());

    boolean exceptionRaised = false;

    try {
      windowed1.join(windowed2, joiner).process(processor);

      KStreamContext context = new MockKStreamContext(null, null);
      stream1.bind(context, streamMetadata);
      stream2.bind(context, streamMetadata);

    } catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertFalse(exceptionRaised);

    // push two items to the main stream. the other stream's window is empty

    for (int i = 0; i < 2; i++) {
      stream1.receive(expectedKeys[i], "X" + expectedKeys[i], 0L);
    }

    assertEquals(0, processor.processed.size());

    // push two items to the other stream. the main stream's window has two items

    for (int i = 0; i < 2; i++) {
      stream2.receive(expectedKeys[i], "Y" + expectedKeys[i], 0L);
    }

    assertEquals(2, processor.processed.size());

    expected = new String[]{"0:X0+Y0", "1:X1+Y1"};

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }

    processor.processed.clear();

    // push all items to the main stream. this should produce two items.

    for (int i = 0; i < expectedKeys.length; i++) {
      stream1.receive(expectedKeys[i], "X" + expectedKeys[i], 0L);
    }

    assertEquals(2, processor.processed.size());

    expected = new String[]{"0:X0+Y0", "1:X1+Y1"};

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }

    processor.processed.clear();

    // there will be previous two items + all items in the main stream's window, thus two are duplicates.

    // push all items to the other stream. this should produce 6 items
    for (int i = 0; i < expectedKeys.length; i++) {
      stream2.receive(expectedKeys[i], "Y" + expectedKeys[i], 0L);
    }

    assertEquals(6, processor.processed.size());

    expected = new String[]{"0:X0+Y0", "0:X0+Y0", "1:X1+Y1", "1:X1+Y1", "2:X2+Y2", "3:X3+Y3"};

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }
  }

  @Test
  public void testJoinPrior() {

    final int[] expectedKeys = new int[]{0, 1, 2, 3};

    KStreamSource<Integer, String> stream1;
    KStreamSource<Integer, String> stream2;
    KStreamWindowed<Integer, String> windowed1;
    KStreamWindowed<Integer, String> windowed2;
    TestProcessor<Integer, String> processor;
    String[] expected;

<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
>>>>>>> new api model
    processor = new TestProcessor<>();
    stream1 = new KStreamSource<>(null, initializer);
    stream2 = new KStreamSource<>(null, initializer);
    windowed1 = stream1.with(new UnlimitedWindow<Integer, String>());
    windowed2 = stream2.with(new UnlimitedWindow<Integer, String>());

    boolean exceptionRaised = false;

    try {
      windowed1.joinPrior(windowed2, joiner).process(processor);

      KStreamContext context = new MockKStreamContext(null, null);
      stream1.bind(context, streamMetadata);
      stream2.bind(context, streamMetadata);

    } catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertFalse(exceptionRaised);

    // push two items to the main stream. the other stream's window is empty

    for (int i = 0; i < 2; i++) {
      stream1.receive(expectedKeys[i], "X" + expectedKeys[i], i);
    }

    assertEquals(0, processor.processed.size());

    // push two items to the other stream. the main stream's window has two items
    // no corresponding item in the main window has a newer timestamp

    for (int i = 0; i < 2; i++) {
      stream2.receive(expectedKeys[i], "Y" + expectedKeys[i], i + 1);
    }

    assertEquals(0, processor.processed.size());

    processor.processed.clear();

    // push all items with newer timestamps to the main stream. this should produce two items.

    for (int i = 0; i < expectedKeys.length; i++) {
      stream1.receive(expectedKeys[i], "X" + expectedKeys[i], i + 2);
    }

    assertEquals(2, processor.processed.size());

    expected = new String[]{"0:X0+Y0", "1:X1+Y1"};

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }

    processor.processed.clear();

    // there will be previous two items + all items in the main stream's window, thus two are duplicates.

    // push all items with older timestamps to the other stream. this should produce six items
    for (int i = 0; i < expectedKeys.length; i++) {
      stream2.receive(expectedKeys[i], "Y" + expectedKeys[i], i);
    }

    assertEquals(6, processor.processed.size());

    expected = new String[]{"0:X0+Y0", "0:X0+Y0", "1:X1+Y1", "1:X1+Y1", "2:X2+Y2", "3:X3+Y3"};

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }

  }

  @Test
  public void testMap() {
    KStreamSource<Integer, String> stream1;
    KStreamSource<Integer, String> stream2;
    KStream<Integer, String> mapped1;
    KStream<Integer, String> mapped2;
    KStreamWindowed<Integer, String> windowed1;
    KStreamWindowed<Integer, String> windowed2;
    TestProcessor<Integer, String> processor;

<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
    processor = new TestProcessor<>();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
    processor = new TestProcessor<>();
    stream1 = new KStreamSource<>(null, initializer);
    stream2 = new KStreamSource<>(null, initializer);
    mapped1 = stream1.map(keyValueMapper);
    mapped2 = stream2.map(keyValueMapper);
>>>>>>> new api model

    boolean exceptionRaised;

    try {
      stream1 = new KStreamSource<>(null, initializer);
      stream2 = new KStreamSource<>(null, initializer);
      mapped1 = stream1.map(keyValueMapper);
      mapped2 = stream2.map(keyValueMapper);

      exceptionRaised = false;
      windowed1 = stream1.with(new UnlimitedWindow<Integer, String>());
      windowed2 = mapped2.with(new UnlimitedWindow<Integer, String>());

      windowed1.join(windowed2, joiner).process(processor);

      KStreamContext context = new MockKStreamContext(null, null);
      stream1.bind(context, streamMetadata);
      stream2.bind(context, streamMetadata);

    } catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertTrue(exceptionRaised);

    try {
      stream1 = new KStreamSource<>(null, initializer);
      stream2 = new KStreamSource<>(null, initializer);
      mapped1 = stream1.map(keyValueMapper);
      mapped2 = stream2.map(keyValueMapper);

      exceptionRaised = false;
      windowed1 = mapped1.with(new UnlimitedWindow<Integer, String>());
      windowed2 = stream2.with(new UnlimitedWindow<Integer, String>());

      windowed1.join(windowed2, joiner).process(processor);

      KStreamContext context = new MockKStreamContext(null, null);
      stream1.bind(context, streamMetadata);
      stream2.bind(context, streamMetadata);

    } catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertTrue(exceptionRaised);

    try {
      stream1 = new KStreamSource<>(null, initializer);
      stream2 = new KStreamSource<>(null, initializer);
      mapped1 = stream1.map(keyValueMapper);
      mapped2 = stream2.map(keyValueMapper);

      exceptionRaised = false;
      windowed1 = mapped1.with(new UnlimitedWindow<Integer, String>());
      windowed2 = mapped2.with(new UnlimitedWindow<Integer, String>());

      windowed1.join(windowed2, joiner).process(processor);

      KStreamContext context = new MockKStreamContext(null, null);
      stream1.bind(context, streamMetadata);
      stream2.bind(context, streamMetadata);

    } catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertTrue(exceptionRaised);
  }

  @Test
  public void testFlatMap() {
    KStreamSource<Integer, String> stream1;
    KStreamSource<Integer, String> stream2;
    KStream<Integer, String> mapped1;
    KStream<Integer, String> mapped2;
    KStreamWindowed<Integer, String> windowed1;
    KStreamWindowed<Integer, String> windowed2;
    TestProcessor<Integer, String> processor;

<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
    processor = new TestProcessor<>();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
    processor = new TestProcessor<>();
    stream1 = new KStreamSource<>(null, initializer);
    stream2 = new KStreamSource<>(null, initializer);
    mapped1 = stream1.flatMap(keyValueMapper2);
    mapped2 = stream2.flatMap(keyValueMapper2);
>>>>>>> new api model

    boolean exceptionRaised;

    try {
      stream1 = new KStreamSource<>(null, initializer);
      stream2 = new KStreamSource<>(null, initializer);
      mapped1 = stream1.flatMap(keyValueMapper2);
      mapped2 = stream2.flatMap(keyValueMapper2);

      exceptionRaised = false;
      windowed1 = stream1.with(new UnlimitedWindow<Integer, String>());
      windowed2 = mapped2.with(new UnlimitedWindow<Integer, String>());

      windowed1.join(windowed2, joiner).process(processor);

      KStreamContext context = new MockKStreamContext(null, null);
      stream1.bind(context, streamMetadata);
      stream2.bind(context, streamMetadata);

    } catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertTrue(exceptionRaised);

    try {
      stream1 = new KStreamSource<>(null, initializer);
      stream2 = new KStreamSource<>(null, initializer);
      mapped1 = stream1.flatMap(keyValueMapper2);
      mapped2 = stream2.flatMap(keyValueMapper2);

      exceptionRaised = false;
      windowed1 = mapped1.with(new UnlimitedWindow<Integer, String>());
      windowed2 = stream2.with(new UnlimitedWindow<Integer, String>());

      windowed1.join(windowed2, joiner).process(processor);

      KStreamContext context = new MockKStreamContext(null, null);
      stream1.bind(context, streamMetadata);
      stream2.bind(context, streamMetadata);

    } catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertTrue(exceptionRaised);

    try {
      stream1 = new KStreamSource<>(null, initializer);
      stream2 = new KStreamSource<>(null, initializer);
      mapped1 = stream1.flatMap(keyValueMapper2);
      mapped2 = stream2.flatMap(keyValueMapper2);

      exceptionRaised = false;
      windowed1 = mapped1.with(new UnlimitedWindow<Integer, String>());
      windowed2 = mapped2.with(new UnlimitedWindow<Integer, String>());

      windowed1.join(windowed2, joiner).process(processor);

      KStreamContext context = new MockKStreamContext(null, null);
      stream1.bind(context, streamMetadata);
      stream2.bind(context, streamMetadata);

    } catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertTrue(exceptionRaised);
  }

  @Test
  public void testMapValues() {
    KStreamSource<Integer, String> stream1;
    KStreamSource<Integer, String> stream2;
    KStream<Integer, String> mapped1;
    KStream<Integer, String> mapped2;
    KStreamWindowed<Integer, String> windowed1;
    KStreamWindowed<Integer, String> windowed2;
    TestProcessor<Integer, String> processor;

<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
    processor = new TestProcessor<>();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
    processor = new TestProcessor<>();
    stream1 = new KStreamSource<>(null, initializer);
    stream2 = new KStreamSource<>(null, initializer);
    mapped1 = stream1.mapValues(valueMapper);
    mapped2 = stream2.mapValues(valueMapper);
>>>>>>> new api model

    boolean exceptionRaised;

    try {
      stream1 = new KStreamSource<>(null, initializer);
      stream2 = new KStreamSource<>(null, initializer);
      mapped1 = stream1.mapValues(valueMapper);
      mapped2 = stream2.mapValues(valueMapper);

      exceptionRaised = false;
      windowed1 = stream1.with(new UnlimitedWindow<Integer, String>());
      windowed2 = mapped2.with(new UnlimitedWindow<Integer, String>());

      windowed1.join(windowed2, joiner).process(processor);

      KStreamContext context = new MockKStreamContext(null, null);
      stream1.bind(context, streamMetadata);
      stream2.bind(context, streamMetadata);

    } catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertFalse(exceptionRaised);

    try {
      stream1 = new KStreamSource<>(null, initializer);
      stream2 = new KStreamSource<>(null, initializer);
      mapped1 = stream1.mapValues(valueMapper);
      mapped2 = stream2.mapValues(valueMapper);

      exceptionRaised = false;
      windowed1 = mapped1.with(new UnlimitedWindow<Integer, String>());
      windowed2 = stream2.with(new UnlimitedWindow<Integer, String>());

      windowed1.join(windowed2, joiner).process(processor);

      KStreamContext context = new MockKStreamContext(null, null);
      stream1.bind(context, streamMetadata);
      stream2.bind(context, streamMetadata);

    } catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertFalse(exceptionRaised);

    try {
      stream1 = new KStreamSource<>(null, initializer);
      stream2 = new KStreamSource<>(null, initializer);
      mapped1 = stream1.mapValues(valueMapper);
      mapped2 = stream2.mapValues(valueMapper);

      exceptionRaised = false;
      windowed1 = mapped1.with(new UnlimitedWindow<Integer, String>());
      windowed2 = mapped2.with(new UnlimitedWindow<Integer, String>());

      windowed1.join(windowed2, joiner).process(processor);

      KStreamContext context = new MockKStreamContext(null, null);
      stream1.bind(context, streamMetadata);
      stream2.bind(context, streamMetadata);

    } catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertFalse(exceptionRaised);
  }

  @Test
  public void testFlatMapValues() {
    KStreamSource<Integer, String> stream1;
    KStreamSource<Integer, String> stream2;
    KStream<Integer, String> mapped1;
    KStream<Integer, String> mapped2;
    KStreamWindowed<Integer, String> windowed1;
    KStreamWindowed<Integer, String> windowed2;
    TestProcessor<Integer, String> processor;

<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
    processor = new TestProcessor<>();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
    processor = new TestProcessor<>();
    stream1 = new KStreamSource<>(null, initializer);
    stream2 = new KStreamSource<>(null, initializer);
    mapped1 = stream1.flatMapValues(valueMapper2);
    mapped2 = stream2.flatMapValues(valueMapper2);
>>>>>>> new api model

    boolean exceptionRaised;

    try {
      stream1 = new KStreamSource<>(null, initializer);
      stream2 = new KStreamSource<>(null, initializer);
      mapped1 = stream1.flatMapValues(valueMapper2);
      mapped2 = stream2.flatMapValues(valueMapper2);

      exceptionRaised = false;
      windowed1 = stream1.with(new UnlimitedWindow<Integer, String>());
      windowed2 = mapped2.with(new UnlimitedWindow<Integer, String>());

      windowed1.join(windowed2, joiner).process(processor);

      KStreamContext context = new MockKStreamContext(null, null);
      stream1.bind(context, streamMetadata);
      stream2.bind(context, streamMetadata);

    } catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertFalse(exceptionRaised);

    try {
      stream1 = new KStreamSource<>(null, initializer);
      stream2 = new KStreamSource<>(null, initializer);
      mapped1 = stream1.flatMapValues(valueMapper2);
      mapped2 = stream2.flatMapValues(valueMapper2);

      exceptionRaised = false;
      windowed1 = mapped1.with(new UnlimitedWindow<Integer, String>());
      windowed2 = stream2.with(new UnlimitedWindow<Integer, String>());

      windowed1.join(windowed2, joiner).process(processor);

      KStreamContext context = new MockKStreamContext(null, null);
      stream1.bind(context, streamMetadata);
      stream2.bind(context, streamMetadata);

    } catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertFalse(exceptionRaised);

    try {
      stream1 = new KStreamSource<>(null, initializer);
      stream2 = new KStreamSource<>(null, initializer);
      mapped1 = stream1.flatMapValues(valueMapper2);
      mapped2 = stream2.flatMapValues(valueMapper2);

      exceptionRaised = false;
      windowed1 = mapped1.with(new UnlimitedWindow<Integer, String>());
      windowed2 = mapped2.with(new UnlimitedWindow<Integer, String>());

      windowed1.join(windowed2, joiner).process(processor);

      KStreamContext context = new MockKStreamContext(null, null);
      stream1.bind(context, streamMetadata);
      stream2.bind(context, streamMetadata);

    } catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertFalse(exceptionRaised);
  }

}
