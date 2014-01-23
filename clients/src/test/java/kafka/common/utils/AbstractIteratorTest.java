package kafka.common.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Test;

public class AbstractIteratorTest {

    @Test
    public void testIterator() {
        int max = 10;
        List<Integer> l = new ArrayList<Integer>();
        for (int i = 0; i < max; i++)
            l.add(i);
        ListIterator<Integer> iter = new ListIterator<Integer>(l);
        for (int i = 0; i < max; i++) {
            Integer value = i;
            assertEquals(value, iter.peek());
            assertTrue(iter.hasNext());
            assertEquals(value, iter.next());
        }
        assertFalse(iter.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void testEmptyIterator() {
        Iterator<Object> iter = new ListIterator<Object>(Arrays.asList());
        iter.next();
    }

    class ListIterator<T> extends AbstractIterator<T> {
        private List<T> list;
        private int position = 0;

        public ListIterator(List<T> l) {
            this.list = l;
        }

        public T makeNext() {
            if (position < list.size())
                return list.get(position++);
            else
                return allDone();
        }
    }
}
