/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.util;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;

public class PersistentBlockingQueue<E extends Serializable> extends PersistentQueue<E>
    implements BlockingQueue<E> {
      
  private int size;
  
  public PersistentBlockingQueue(File dataDir) {
    super(dataDir);
    size = size();
  }
  
  public synchronized int drainTo(Collection<? super E> collection)  {
    return drainTo(collection, -1);
  }
  
  public synchronized int drainTo(Collection<? super E> collection, int maxElements)  {
    if (collection == this) {
      throw new IllegalArgumentException("A Queue cannot be drained to itself.");
    }
    // TODO: Test this.
    int numToDrain = maxElements >= 0 ? Math.min(maxElements, size()) : size();
    for (int i = 0; i < numToDrain; i++) {
      collection.add(poll());
    }
    return numToDrain;
  }
  
  @Override
  public synchronized boolean offer(E object) {
    boolean accepted = super.offer(object);
    if (accepted) {
      size++;
      notify();
    }
    return accepted;
  }
  
  public synchronized boolean offer(E object, long timeout, TimeUnit unit) {
    return offer(object);
  }
  
  public synchronized void put(E object) {
    offer(object);
  }
  
  public int remainingCapacity() {
    return Integer.MAX_VALUE;
  }
  
  @Override
  public synchronized E poll() {
    E object = super.poll();
    if (object != null) {
      size--;
    }
    return object;
  }

  public synchronized E poll(long timeout, TimeUnit unit) throws InterruptedException {    
    if (size == 0) {
      unit.timedWait(this, timeout);
    }
    return poll();
  }

  public synchronized E take()  throws InterruptedException {
    if (size == 0) {
      wait();
    }
    return poll();
  }
}
