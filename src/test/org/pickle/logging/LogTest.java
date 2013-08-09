/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.logging;

import java.util.*;
import java.util.logging.*;
import org.junit.*;
import static org.junit.Assert.*;

public class LogTest {
  private static Logger jlog = Logger.getLogger(LogTest.class.getName());
  private Level saveLevel;
  private Handler handler;
  private List<LogRecord> records;
  
  @Before
  public void setUp() {
    records = new ArrayList<LogRecord>();
    handler = new Handler() {
      public void publish(LogRecord record) { records.add(record); }
      public void close() { }
      public void flush() { }
    };
    jlog.addHandler(handler);
    saveLevel = jlog.getLevel();
  }
  
  @After
  public void tearDown() {
    jlog.removeHandler(handler);
    jlog.setLevel(saveLevel);
  }
  
  @Test
  public void testLogging() {
    jlog.setLevel(Level.INFO);
    Log.info("foo");
    Log.info("foo%s%d", "A", 1);
    Log.info("foo%s", "B", new RuntimeException("expected"));
    jlog.setLevel(Level.WARNING);
    Log.info("nope");
    assertEquals(3, records.size());

    LogRecord record = records.get(0);
    assertEquals("foo", record.getMessage());
    assertEquals(0, record.getParameters().length);
    assertTrue(Math.abs(record.getMillis() - System.currentTimeMillis()) < 100); 
    assertNull(record.getThrown());
    assertEquals(getClass().getName(), record.getLoggerName());
    assertEquals(getClass().getName(), record.getSourceClassName());
    assertEquals("testLogging", record.getSourceMethodName());

    record = records.get(1);
    assertEquals("fooA1", record.getMessage());
    assertTrue(Arrays.equals(new Object[] { "A", 1 }, record.getParameters()));

    record = records.get(2);
    assertEquals("fooB", record.getMessage());
    assertEquals("expected", record.getThrown().getMessage());
  }
  
  @Test
  public void testPerformance() {
    long start;
    long end;
    final int warmUpCount = 5;
    final int testCount = 250;
    final double tolerance = 0.05;
    final String message = "log record from LogTest#testPerformance()";
    
    jlog.setLevel(Level.INFO);
    for (int i = 0; i < warmUpCount; i++) {
      final String warm = "warm up from LogTest#testPerformance()";
      jlog.info(warm);
      Log.info(warm);
    }
    
    start = System.currentTimeMillis();
    for (int i = 0; i < testCount; i++) {
      jlog.info(message);
    }
    end = System.currentTimeMillis();
    long javaLogTime = end - start;

    start = System.currentTimeMillis();
    for (int i = 0; i < testCount; i++) {
      Log.info(message);
    }
    end = System.currentTimeMillis();
    long myLogTime = end - start;
    
    assertEquals((2 * warmUpCount) + (2 * testCount), records.size());

    Log.info("%s time = %d ms", Logger.class.getName(), javaLogTime);
    Log.info("%s time = %d ms", Log.class.getName(), myLogTime);
    double deltaPercent = ((double) (myLogTime - javaLogTime)) / javaLogTime;
    Log.info("delta = %f %%", deltaPercent * 100.0);
    if (myLogTime > javaLogTime) {
      assertTrue("Log is executing too slowly.", Math.abs(deltaPercent) < tolerance);
    }
  }
}
