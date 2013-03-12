package kafka.utils

import junit.framework.Assert._
import org.scalatest.Assertions
import org.junit.{Test, After, Before}

class IteratorTemplateTest extends Assertions {
  
  val lst = (0 until 10).toSeq
  val iterator = new IteratorTemplate[Int]() {
    var i = 0
    override def makeNext() = {
      if(i >= lst.size) {
        allDone()
      } else {
        val item = lst(i)
        i += 1
        item
      }
    }
  }

  @Test
  def testIterator() {
    for(i <- 0 until 10) {
      assertEquals("We should have an item to read.", true, iterator.hasNext)
      assertEquals("Checking again shouldn't change anything.", true, iterator.hasNext)
      assertEquals("Peeking at the item should show the right thing.", i, iterator.peek)
      assertEquals("Peeking again shouldn't change anything", i, iterator.peek)
      assertEquals("Getting the item should give the right thing.", i, iterator.next)
    }
    assertEquals("All gone!", false, iterator.hasNext)
    intercept[NoSuchElementException] {
      iterator.peek
    }
    intercept[NoSuchElementException] {
      iterator.next
    }
  }
  
}