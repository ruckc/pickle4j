/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class OneLineFormatter extends Formatter {
  
  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  
  public OneLineFormatter() {
  }

  @Override
  public String format(LogRecord record) {
    String stackTrace = "";
    Throwable throwable = record.getThrown();
    if (throwable != null) {
      StringWriter stringWriter = new StringWriter();
      throwable.printStackTrace(new PrintWriter(stringWriter));
      stackTrace = stringWriter.toString();
    }
    return String.format("%s - %s - %s#%s - %s\n%s",
      dateFormat.format(new Date(record.getMillis())), record.getLevel().getName(), 
      record.getSourceClassName(), record.getSourceMethodName(), formatMessage(record), stackTrace);
  }
}