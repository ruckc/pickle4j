/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.util;

import java.io.File;
import java.util.*;
import java.sql.SQLException;
import org.h2.tools.DeleteDbFiles;
import org.junit.*;
import static org.junit.Assert.*;
import org.pickle.Disposable;
import org.pickle.logging.Log;

public abstract class PersistentCollectionTestBase {
  
  private final File dataDir =
      new File(String.format("%s/%s", System.getProperty("java.io.tmpdir"), getClass().getName()));
  
  private Collection<String> collection;
  
  protected abstract Collection<String> createPersistentCollection(File dataDir);

  protected static String item(int i) {
    return String.format("item-%04d", i);
  }
  
  protected static String[] items(int from, int to) {
    String[] items = new String[to - from + 1];
    for (int i = 0; i < items.length; i++) {
      items[i] = item(i + from);
    }
    return items;
  }
  
  @Before
  public void setUp() {
    dataDir.mkdir();
    collection = createPersistentCollection(dataDir);
    collection.clear();
  }
  
  @After
  public void tearDown() {
    try {
      if (collection != null) {
        // Most persistent collections should be disposable, but some, such as
        // PersistentMap's keySet() and entrySet(), are not because they should not
        // be disposed indepently of their creator.
        if (collection instanceof Disposable) {
          ((Disposable) collection).dispose();
        }
        DeleteDbFiles.execute(dataDir.getAbsolutePath(), null, true);
        assertTrue(dataDir.delete());
      }
    }
    catch (SQLException e) {
      Log.error("Could not delete database files.", e);
    }
  }
  
  @Test
  public void testEmptyCollection() {
    assertEquals(0, collection.size());
    assertTrue(collection.isEmpty());
    assertFalse(collection.contains(item(0)));
    assertFalse(collection.iterator().hasNext());
  }
  
  /**
   * Tests for add(), addAll(), remove(), removeAll(), contains(), size(), isEmpty(), clear()
   */
  @Test
  public void testBasicCollectionMethods() {
    final String[] items = items(0, 2);

    // Adding one at a time.
    for (int i = 0; i < items.length; i++) {
      String item = items[i];
      assertFalse(collection.contains(item));
      assertTrue(collection.add(item));
      assertFalse(collection.isEmpty());
      assertEquals(i + 1, collection.size());
      assertTrue(collection.contains(item));
    }
    assertFalse(collection.contains(item(99)));
    
    // Removing one at a time.
    for (int i = 0; i < items.length; i++) {
      String item = items[i];
      assertTrue(collection.remove(item));
      assertFalse(collection.contains(item));
      assertEquals(i == items.length - 1, collection.isEmpty());
      assertEquals(items.length - i - 1, collection.size());
    }
    assertTrue(collection.isEmpty());
    
    // Adding all at once
    assertTrue(collection.addAll(Arrays.asList(items)));
    assertFalse(collection.isEmpty());
    assertEquals(items.length, collection.size());
    for (String item : items) {
      assertTrue(collection.contains(item));
    }

    // Removing all at once.
    assertTrue(collection.removeAll(Arrays.asList(items)));
    assertTrue(collection.isEmpty());
    assertEquals(0, collection.size());
    for (String item : items) {
      assertFalse(collection.contains(item));
    }

    // Clearing
    collection.clear();
    assertTrue(collection.isEmpty());
    assertEquals(0, collection.size());
  }
  
  @Test
  public void testIteratorWithoutRemove() {
    final String[] items = items(0, 5);
    collection.addAll(Arrays.asList(items));

    // Simple iteraiton
    Iterator<String> it = collection.iterator();
    assertTrue(it.hasNext());
    int count = 0;
    while (it.hasNext()) {
      assertEquals(items[count], it.next());
      count++;
    }
    assertEquals(items.length, count);
 
    // Going past the end
    try {
      it.next();
      fail();
    }
    catch (NoSuchElementException e) {
      // Expected.
    }
  }
  
  @Test
  public void testIteratorWithRemove() {
    final String[] items = items(0, 5);
    collection.addAll(Arrays.asList(items));
    Iterator<String> it;
    
    // Remove one item.
    it = collection.iterator();
    assertTrue(it.hasNext());
    int count = 0;
    while (it.hasNext()) {
      assertTrue(count < items.length);
      // TODO: Test that remove() throws when next() is not first called.
      String item = it.next();
      assertEquals(items[count], item);
      if (count == 2) {
        it.remove();
        assertEquals(items.length - 1, collection.size());
      }
      count++;
    }
    assertEquals(items.length, count);
    
    // Remove every item.  Note that one item in the middle of the queue was removed above.
    // This insures that the iterator handles skipped IDs.
    Log.debug("Removing all %d items via iterator.", collection.size());
    it = collection.iterator();
    while (it.hasNext()) {
      it.next();
      it.remove();
    }
    assertEquals(0, collection.size());
    assertTrue(collection.isEmpty());
  }
}