/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.logging;

import java.util.Map;
import java.util.HashMap;
import java.util.logging.*;

/**
 * This class simplifies the use of Java logging without impacting performance.  It provides the following
 * advantages:
 * <ul>
 * <li>The logger name is automatically set to be the fully-qualified class name of the caller, by using
 *     stack introspection.</li>
 * <li>Log messages are formatted using the {@link String#format(String, Object...)} patterns
 *     and variable arguments.  The parameters are also assigned to the underlying {@link LogRecord}.</li>
 * <li>If the last parameter to a method is a {@link Throwable}, it will be assigned to the {@link LogRecord}. 
 * <li>The method names align more closely with the log4j method names.</li>
 * </ul>
 * All methods are static wrappers around a {@link Logger}.  Each Logger is created as needed and then cached,
 * one per calling class.
 */
public final class Log {
  private Log() { }
  
  /**
   * Class names are mapped to Loggers.
   */
  private static final Map<String, Logger> loggers = new HashMap<String, Logger>();
  
  /**
   * Logs a debug message as {@link Level#FINE}.
   */
  public static void debug(String message, Object... params) {
    log(Level.FINE, message, params);
  }

  /**
   * Logs an informational message as {@link Level#INFO}.
   */  
  public static void info(String message, Object... params) {
    log(Level.INFO, message, params);
  }
  
  /**
   * Logs a warning message as {@link Level#WARNING}.
   */
  public static void warn(String message, Object... params) {
    log(Level.WARNING, message, params);
  }

  /**
   * Logs an error message as {@link Level#SEVERE}.
   */  
  public static void error(String message, Object... params) {
    log(Level.SEVERE, message, params);
  }
  
  /**
   * Logs an error message using the specified {@link Level}.
   */
  private static void log(Level level, String message, Object... params) {
    // TODO: Make this private until I adjust the stackPositionOfCaller so it can be called publicly.
    final int stackPositionOfCaller = 2;
    // See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6375302 to understand why
    // new Throwable().getStackTrace() is much faster than Thread.currentThread().getStackTrace().
    StackTraceElement caller = new Throwable().getStackTrace()[stackPositionOfCaller];
    String className = caller.getClassName();
    Logger logger;
    synchronized (loggers) {
      logger = loggers.get(className);
      if (logger == null) {
        logger = Logger.getLogger(className);
        loggers.put(className, logger);
      }
    }

    if (logger.isLoggable(level)) {
      String formattedMessage;
      Throwable thrown = null;
      if (params.length == 0) {
        formattedMessage = message;
      }
      else {
        Object last = params[params.length - 1];
        if (last instanceof Throwable) {
          Object[] subParams = new Object[params.length - 1];
          System.arraycopy(params, 0, subParams, 0, subParams.length);
          formattedMessage = String.format(message, subParams);
          thrown = (Throwable) last;
        }
        else {
          formattedMessage = String.format(message, params);
        }
      }
      LogRecord record = new LogRecord(level, formattedMessage);
      record.setLoggerName(logger.getName());
      record.setSourceClassName(className);
      record.setSourceMethodName(caller.getMethodName());
      record.setThrown(thrown);
      record.setParameters(params);
      logger.log(record);
    }
  }
}