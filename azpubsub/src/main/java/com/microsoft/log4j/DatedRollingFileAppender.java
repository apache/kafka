package com.microsoft.log4j;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

public class DatedRollingFileAppender extends FileAppender {
  private final static String LINE_SEP_REGEX = Layout.LINE_SEP + "$";
  private final static String LINE_SEP_REPLACE_CHAR = "|";
  private final static String EXCEPTION_SEP = "###";

  /**
    The date pattern. By default, the pattern is set to
    "'.'yyyy-MM-dd" meaning daily roll over.
   */
  private final String datePattern = "'.'yyyy-MM-dd";

  /**
     The next time we estimate a roll over should occur. 
    */
  private long nextRolloverCheck = System.currentTimeMillis () - 1;

  /**
    No log file cleanup by default.
   */
  protected int maxBackupIndex = 0;

  /**
     The original name of the log file. */
  private String originalFileName = null;

  private SimpleDateFormat sdf = new SimpleDateFormat(datePattern);

  /**
    The default constructor simply calls its {@link
    FileAppender#FileAppender parents constructor}.  
   */
  public DatedRollingFileAppender() {
    super();
  }

  /**
    Instantiate a RollingFileAppender and open the file designated by
    <code>filename</code>. The opened filename will become the output
    destination for this appender.
    <p>If the <code>append</code> parameter is true, the file will be
    appended to. Otherwise, the file designated by
    <code>filename</code> will be truncated before being opened.
  */
  public DatedRollingFileAppender(Layout layout, String filename, boolean append) throws IOException {
    super(layout, filename, append);
  }

  /**
     Instantiate a FileAppender and open the file designated by
    <code>filename</code>. The opened filename will become the output
    destination for this appender.
    <p>The file will be appended to.  
   */
  public DatedRollingFileAppender(Layout layout, String filename) throws IOException {
    super(layout, filename);
  }

  /**
     Returns the value of the <b>MaxBackupIndex</b> option.
   */
  public int getMaxBackupIndex() {
    return maxBackupIndex;
  }

  public synchronized void setFile(String fileName, boolean append, boolean bufferedIO, int bufferSize) throws IOException {
    this.originalFileName = fileName;
    super.setFile(this.originalFileName + sdf.format(new Date()), append, bufferedIO, bufferSize);
    this.nextRolloverCheck = getNextCheckForTomorrow();
  }

  /**
     Set the maximum number of backup files to keep around.
     <p>The <b>MaxBackupIndex</b> option determines how many backup
     files are kept before the oldest is erased. This option takes
     a positive integer value. If set to zero, then there will be no
     backup files and the log file will be truncated when it reaches
     <code>MaxFileSize</code>.
   */
  public void setMaxBackupIndex(int maxBackups) {
    this.maxBackupIndex = maxBackups;
  }

  /**
     This method differentiates RollingFileAppender from its super
     class.
     @since 0.9.0
  */
  protected void subAppend(LoggingEvent event) {
    if (System.currentTimeMillis() >= this.nextRolloverCheck) {
      LogLog.debug("Roll over the log to next day. Current Log File name: " + this.fileName);

      try {
        // delete old file
        if (this.maxBackupIndex > 0) {
          Calendar c = Calendar.getInstance();
          c.setTime(new Date(this.nextRolloverCheck));
          c.add(Calendar.DATE, -(this.maxBackupIndex + 1));

          String deleteFileName = this.originalFileName + sdf.format(c.getTime());
          File file = new File(deleteFileName);
          if (file.exists())
            file.delete();
        }

        // This will also close the file. This is OK since multiple
        // close operations are safe.
        this.setFile(this.originalFileName, this.fileAppend, this.bufferedIO, this.bufferSize);
      }
      catch(IOException ioe) {
        if (ioe instanceof InterruptedIOException) {
          Thread.currentThread().interrupt();
        }

        LogLog.error("rollOver() failed.", ioe);
      }
    }

    // super.subAppend(event);
    // replacing super.subAppend to replace newline char with |
    String line = this.layout.format(event);
    line = line.replaceAll(DatedRollingFileAppender.LINE_SEP_REGEX, ""); // remove the end newline character added by the formatter
    line = line.replace(Layout.LINE_SEP, DatedRollingFileAppender.LINE_SEP_REPLACE_CHAR); // replace all newline with pipe
    this.qw.write(line);
    if (layout.ignoresThrowable()) {
      String[] s = event.getThrowableStrRep();
      if (s != null) {
        this.qw.write(DatedRollingFileAppender.EXCEPTION_SEP); // putting a separator for the log parser to extract the stack trace easily
        for (int i = 0; i < s.length; i++) {
          if (s[i] != null) {
            this.qw.write(s[i].replace(Layout.LINE_SEP, DatedRollingFileAppender.LINE_SEP_REPLACE_CHAR)); // replace all newline with pipe
          }
        }
      }
    }

    this.qw.write(Layout.LINE_SEP);
    if (shouldFlush(event)) {
      this.qw.flush();
    }
  }

  private long getNextCheckForTomorrow() {
    Calendar c = Calendar.getInstance();
    c.set(Calendar.HOUR_OF_DAY, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);
    c.add(Calendar.DATE, 1);
    return c.getTimeInMillis();
  }
}
