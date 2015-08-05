package org.apache.kafka.stream;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FilteredIteratorTest {

  @Test
  public void testFiltering() {
    List<Integer> list = Arrays.asList(3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5);

    Iterator<String> filtered = new FilteredIterator<String, Integer>(list.iterator()) {
      protected String filter(Integer i) {
        if (i % 3 == 0) return i.toString();
        return null;
      }
    };

    List<String> expected = Arrays.asList("3", "9", "6", "3");
    List<String> result = new ArrayList<String>();

    while (filtered.hasNext()) {
      result.add(filtered.next());
    }

    assertEquals(expected, result);
  }

  @Test
  public void testEmptySource() {
    List<Integer> list = new ArrayList<Integer>();

    Iterator<String> filtered = new FilteredIterator<String, Integer>(list.iterator()) {
      protected String filter(Integer i) {
        if (i % 3 == 0) return i.toString();
        return null;
      }
    };

    List<String> expected = new ArrayList<String>();
    List<String> result = new ArrayList<String>();

    while (filtered.hasNext()) {
      result.add(filtered.next());
    }

    assertEquals(expected, result);
  }

  @Test
  public void testNoMatch() {
    List<Integer> list = Arrays.asList(3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5);

    Iterator<String> filtered = new FilteredIterator<String, Integer>(list.iterator()) {
      protected String filter(Integer i) {
        if (i % 7 == 0) return i.toString();
        return null;
      }
    };

    List<String> expected = new ArrayList<String>();
    List<String> result = new ArrayList<String>();

    while (filtered.hasNext()) {
      result.add(filtered.next());
    }

    assertEquals(expected, result);
  }

}
