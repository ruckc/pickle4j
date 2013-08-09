/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.util;

import java.io.File;
import java.util.*;
import org.junit.*;
import static org.junit.Assert.*;
  
public class PersistentQueueTest extends PersistentCollectionTestBase {
  
  protected Queue<String> queue;
  
  protected Collection<String> createPersistentCollection(File dataDir) {
    queue = new PersistentQueue<String>(dataDir);
    return queue;
  }
  
  @Test
  public void testEmptyQueue() {
    assertEquals(0, queue.size());
    assertNull(queue.peek());
    assertNull(queue.poll());
  }
  
  @Test
  public void testOfferOneItem() {
    final String item = item(1);
    assertTrue(queue.offer(item));
    assertEquals(1, queue.size());
    assertEquals(item, queue.peek());
    assertEquals(1, queue.size());
    assertEquals(item, queue.poll());
    assertEquals(0, queue.size());
  }
  
  @Test
  public void testOfferManyItems() {
    final int N = 10;
    for (int i = 0; i < N; i++) {
      assertTrue(queue.offer(item(i)));
    }
    for (int i = 0; i < N/2; i++) {
      int size = queue.size();
      assertEquals(N - i, size);
      assertEquals(item(i), queue.peek());
      assertEquals(size, queue.size());
      assertEquals(item(i), queue.poll());
      assertEquals(size - 1, queue.size());
    }
    queue.clear();
    assertEquals(0, queue.size());
  }
  
  @Test
  public void testIteratorNoRemove() {
    final int N = 10;
    for (int i = 0; i < N; i++) {
      assertTrue(queue.offer(item(i)));
    }
    Iterator<String> iterator = queue.iterator();
    assertTrue(iterator.hasNext());
    int i = 0;
    while (iterator.hasNext()) {
      String item = iterator.next();
      assertEquals(item(i), item);
      assertTrue(i < N);
      i++;
    }
    assertEquals(N, queue.size());
  }
}