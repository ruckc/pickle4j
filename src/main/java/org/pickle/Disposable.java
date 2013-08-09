/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle;

/**
 * Objects that implement Disposable indicate that they hold resources that should be
 * explicitly closed, rather than relying on garbage collection.
 */
public interface Disposable {
  /**
   * Closes resources held by this object.
   */
  public void dispose();
}