/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.util;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
/*import org.apache.commons.logging.*;*/
import org.junit.*;
import static org.junit.Assert.*;
import org.pickle.logging.Log;

public class PersistentBlockingQueueTest extends PersistentQueueTest {
  
  private BlockingQueue<String> blockingQueue;
  
  @Override
  protected Collection<String> createPersistentCollection(File dataDir) {
    blockingQueue = new PersistentBlockingQueue<String>(dataDir);
    super.queue = blockingQueue;
    return blockingQueue;
  }
  
  @Test
  public void testOneConsumerOnEmpty() throws InterruptedException {
    final long delay = 250;
    long before = System.currentTimeMillis();
    blockingQueue.poll(delay, TimeUnit.MILLISECONDS);
    long after = System.currentTimeMillis();
    assertTrue(after - before >= delay);
  }
  
  @Test
  public void testManyConsumersOneProducer() throws InterruptedException {
    final long producerDelay = 10;
    final int numConsumers = 8;
    final int numItems = 10 * numConsumers;
    
    // Consumers
    final List<String> consumedItems = Collections.synchronizedList(new ArrayList<String>());
    ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
    for (int c = 0; c < numConsumers; c++) {
      final int cid = c + 1;
      executor.execute(new Runnable() {
        public void run() {
          try {
            // Read until consumedItems is full.
            while (consumedItems.size() < numItems) {
              // Wait a reasonable amount of time for the next item.
              String item = blockingQueue.poll(2 * producerDelay * numConsumers, TimeUnit.MILLISECONDS);
              if (item != null) {
                consumedItems.add(item);
                Log.debug(String.format("Consumer %d read %s", cid, item));
              }
              else {
                Log.debug(String.format("Consumer %d read null", cid));
              }
            }
            Log.debug(String.format("Consumer %d finished with no exceptions", cid));
          }
          catch (InterruptedException e) {
            // Exiting the loop will cause this test to fail.
            Log.debug(String.format("Consumer %d finished with an exception", cid), e);
          }
        }
      });
    }
    
    // Producer
    for (int i = 0; i < numItems; i++) {
      assertTrue(blockingQueue.offer(item(i)));
      Thread.sleep(producerDelay);
    }
    
    // Shutdown lets the consumers finish their tasks, but doesn't wait for them.
    executor.shutdown();
    
    // Give some time for all consumers to finished.  If some haven't, then something is wrong
    // and this assertion will fail.
    assertTrue(executor.awaitTermination(2 * producerDelay * numItems, TimeUnit.MILLISECONDS));
    
    // The queue should be empty.
    assertEquals(0, blockingQueue.size());
    assertNull(blockingQueue.poll());
    
    // One of each item should be in the consumedItems list.
    assertEquals(numItems, consumedItems.size());
    Collections.sort(consumedItems);
    for (int i = 0; i < numItems; i++) {
      assertEquals(item(i), consumedItems.get(i));
    }
  }
  
  @Test
  public void testDrainTo() {
    final int numItems = 20;
    final int firstDrain = 5;
    List<String> target = new ArrayList<String>();
    for (int i = 0; i < numItems; i++) {
      blockingQueue.offer(item(i));
    }
    
    blockingQueue.drainTo(target, firstDrain);
    assertEquals(firstDrain, target.size());
    assertEquals(numItems - firstDrain, blockingQueue.size());
    
    blockingQueue.drainTo(target);
    assertEquals(numItems, target.size());
    assertEquals(0, blockingQueue.size());

    for (int i = 0; i < numItems; i++) {
      assertEquals(item(i), target.get(i));
    }
  }
}