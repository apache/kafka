/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.kafka.common.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base class for running a Unix command.
 *
 * <code>Shell</code> can be used to run unix commands like <code>du</code> or
 * <code>df</code>.
 */
abstract public class Shell {

    private static final Logger LOG = LoggerFactory.getLogger(Shell.class);

    private int exitCode;

    /** return an array containing the command name & its parameters */
    protected abstract String[] execString();

    /** Parse the execution result */
    protected abstract void parseExecResult(BufferedReader lines)
        throws IOException;

    /**Time after which the executing script would be timedout*/
    protected long timeOutInterval = 0L;

    private long    interval;   // refresh interval in msec
    private long    lastTime;   // last time the command was performed
    private Process process; // sub process used to execute the command

    /**If or not script finished executing*/
    private volatile AtomicBoolean completed;

    public static final Time TIME = new SystemTime();

    public Shell() {
        this(0L);
    }

    /**
     * @param interval the minimum duration to wait before re-executing the
     *        command.
     */
    public Shell(long interval) {
        this.interval = interval;
        this.lastTime = (interval < 0) ? 0 : -interval;
    }

    /** get the exit code
     * @return the exit code of the process
     */
    public int exitCode() {
        return exitCode;
    }

    /** get the current sub-process executing the given command
     * @return process executing the command
     */
    public Process process() {
        return process;
    }

    /** check to see if a command needs to be executed and execute if needed */
    protected void run() throws IOException {
        if (lastTime + interval > TIME.currentElapsedTime())
            return;
        exitCode = 0; // reset for next run
        runCommand();
    }

    /** Run a command */
    private void runCommand() throws IOException {
        ProcessBuilder builder = new ProcessBuilder(execString());
        Timer timeOutTimer = null;
        ShellTimeoutTimerTask timeoutTimerTask = null;
        completed = new AtomicBoolean(false);

        process = builder.start();
        if (timeOutInterval > 0) {
            timeOutTimer = new Timer();
            timeoutTimerTask = new ShellTimeoutTimerTask(
                                                         this);
            //One time scheduling.
            timeOutTimer.schedule(timeoutTimerTask, timeOutInterval);
        }
        final BufferedReader errReader =
            new BufferedReader(new InputStreamReader(process
                                                     .getErrorStream()));
        BufferedReader inReader =
            new BufferedReader(new InputStreamReader(process
                                                     .getInputStream()));
        final StringBuffer errMsg = new StringBuffer();

        // read error and input streams as this would free up the buffers
        // free the error stream buffer
        Thread errThread = new Thread() {
                @Override
                public void run() {
                    try {
                        String line = errReader.readLine();
                        while ((line != null) && !isInterrupted()) {
                            errMsg.append(line);
                            errMsg.append(System.getProperty("line.separator"));
                            line = errReader.readLine();
                        }
                    } catch (IOException ioe) {
                        LOG.warn("Error reading the error stream", ioe);
                    }
                }
            };
        try {
            errThread.start();
        } catch (IllegalStateException ise) { }
        try {
            parseExecResult(inReader); // parse the output
            // clear the input stream buffer
            String line = inReader.readLine();
            while (line != null) {
                line = inReader.readLine();
            }
            // wait for the process to finish and check the exit code
            exitCode  = process.waitFor();
            try {
                // make sure that the error thread exits
                errThread.join();
            } catch (InterruptedException ie) {
                LOG.warn("Interrupted while reading the error stream", ie);
            }
            completed.set(true);
            //the timeout thread handling
            //taken care in finally block
            if (exitCode != 0) {
                throw new ExitCodeException(exitCode, errMsg.toString());
            }
        } catch (InterruptedException ie) {
            throw new IOException(ie.toString());
        } finally {
            if (timeOutTimer != null)
                timeOutTimer.cancel();

            // close the input stream
            try {
                inReader.close();
            } catch (IOException ioe) {
                LOG.warn("Error while closing the input stream", ioe);
            }
            if (!completed.get())
                errThread.interrupt();

            try {
                errReader.close();
            } catch (IOException ioe) {
                LOG.warn("Error while closing the error stream", ioe);
            }

            process.destroy();
            lastTime = TIME.currentElapsedTime();
        }
    }


    /**
     * This is an IOException with exit code added.
     */
    @SuppressWarnings("serial")
    public static class ExitCodeException extends IOException {
        int exitCode;

        public ExitCodeException(int exitCode, String message) {
            super(message);
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }
    }

    /**
     * A simple shell command executor.
     *
     * <code>ShellCommandExecutor</code>should be used in cases where the output
     * of the command needs no explicit parsing and where the command, working
     * directory and the environment remains unchanged. The output of the command
     * is stored as-is and is expected to be small.
     */
    public static class ShellCommandExecutor extends Shell {

        private String[] command;
        private StringBuffer output;

        public ShellCommandExecutor(String[] execString) {
            this(execString, 0L);
        }


        /**
         * Create a new instance of the ShellCommandExecutor to execute a command.
         *
         * @param execString The command to execute with arguments
         * @param timeout Specifies the time in milliseconds, after which the
         *                command will be killed and the status marked as timedout.
         *                If 0, the command will not be timed out.
         */

        public ShellCommandExecutor(String[] execString, long timeout) {
            command = execString.clone();
            timeOutInterval = timeout;
        }


        /** Execute the shell command. */
        public void execute() throws IOException {
            this.run();
        }

        protected String[] execString() {
            return command;
        }

        protected void parseExecResult(BufferedReader lines) throws IOException {
            output = new StringBuffer();
            char[] buf = new char[512];
            int nRead;
            while ((nRead = lines.read(buf, 0, buf.length)) > 0) {
                output.append(buf, 0, nRead);
            }
        }

        /** Get the output of the shell command.*/
        public String output() {
            return (output == null) ? "" : output.toString();
        }

        /**
         * Returns the commands of this instance.
         * Arguments with spaces in are presented with quotes round; other
         * arguments are presented raw
         *
         * @return a string representation of the object.
         */
        public String toString() {
            StringBuilder builder = new StringBuilder();
            String[] args = execString();
            for (String s : args) {
                if (s.indexOf(' ') >= 0) {
                    builder.append('"').append(s).append('"');
                } else {
                    builder.append(s);
                }
                builder.append(' ');
            }
            return builder.toString();
        }
    }

    /**
     * Static method to execute a shell command.
     * Covers most of the simple cases without requiring the user to implement
     * the <code>Shell</code> interface.
     * @param cmd shell command to execute.
     * @return the output of the executed command.
     */
    public static String execCommand(String ... cmd) throws IOException {
        return execCommand(cmd, 0L);
    }

    /**
     * Static method to execute a shell command.
     * Covers most of the simple cases without requiring the user to implement
     * the <code>Shell</code> interface.
     * @param cmd shell command to execute.
     * @param timeout time in milliseconds after which script should be marked timeout
     * @return the output of the executed command.
     */
    public static String execCommand(String[] cmd, long timeout) throws IOException {
        ShellCommandExecutor exec = new ShellCommandExecutor(cmd, timeout);
        exec.execute();
        return exec.output();
    }

    /**
     * Timer which is used to timeout scripts spawned off by shell.
     */
    private static class ShellTimeoutTimerTask extends TimerTask {

        private Shell shell;

        public ShellTimeoutTimerTask(Shell shell) {
            this.shell = shell;
        }

        @Override
        public void run() {
            Process p = shell.process();
            try {
                p.exitValue();
            } catch (Exception e) {
                //Process has not terminated.
                //So check if it has completed
                //if not just destroy it.
                if (p != null && !shell.completed.get()) {
                    p.destroy();
                }
            }
        }
    }

}
