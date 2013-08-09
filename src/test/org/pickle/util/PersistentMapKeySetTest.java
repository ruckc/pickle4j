/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.util;

import java.io.File;
import java.util.*;
import org.junit.*;
import static org.junit.Assert.*;

public class PersistentMapKeySetTest /* extends PersistentCollectionTestBase */ {
  
  private PersistentMap<String, String> map;
  
  protected Collection<String> createPersistentCollection(File dataDir) {
    map = new PersistentMap<String, String>(dataDir);
    return map.keySet();
  }

//  @Override
  public void tearDown() {
    map.dispose();
//    super.tearDown();
  }

  @Test
  @Ignore("PersistentMap needs to be implemented")
  public void testEmptyMap() {
    assertEquals(0, map.size());
  }
}

