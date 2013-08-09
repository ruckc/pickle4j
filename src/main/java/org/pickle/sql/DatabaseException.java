/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.sql;

public class DatabaseException extends RuntimeException {
  
  public DatabaseException(String message) {
    super(message);
  }
  
  public DatabaseException(String message, Throwable throwable) {
    super(message, throwable);
  }
}