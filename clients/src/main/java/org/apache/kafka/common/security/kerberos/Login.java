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

package org.apache.kafka.common.security.kerberos;

/**
 * This class is responsible for refreshing Kerberos credentials for
 * logins for both Kafka client and server.
 */

import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.Subject;

import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator.ClientCallbackHandler;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.utils.Shell;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.SystemTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.security.URIParameter;
import java.util.Date;
import java.util.Random;
import java.util.Set;

public class Login {
    private static final Logger log = LoggerFactory.getLogger(Login.class);

    // LoginThread will sleep until 80% of time from last refresh to
    // ticket's expiry has been reached, at which time it will wake
    // and try to renew the ticket.
    private static final float TICKET_RENEW_WINDOW = 0.80f;

    /**
     * Percentage of random jitter added to the renewal time
     */
    private static final float TICKET_RENEW_JITTER = 0.05f;

    // Regardless of TICKET_RENEW_WINDOW setting above and the ticket expiry time,
    // thread will not sleep between refresh attempts any less than 1 minute (60*1000 milliseconds = 1 minute).
    // Change the '1' to e.g. 5, to change this to 5 minutes.
    private static final long MIN_TIME_BEFORE_RELOGIN = 1 * 60 * 1000L;

    private volatile Subject subject = null;
    private Thread t = null;
    private boolean isKrbTicket = false;
    private boolean isUsingTicketCache = false;

    /** Random number generator */
    private static Random rng = new Random();

    private LoginContext login = null;
    private String loginContextName = null;
    private String jaasConfigFilePath = null;
    private String principal = null;
    private Configuration jaasConfig = null;
    private Time time = new SystemTime();
    // Initialize 'lastLogin' to do a login at first time
    private long lastLogin = time.currentElapsedTime() - MIN_TIME_BEFORE_RELOGIN;
    public CallbackHandler callbackHandler = new ClientCallbackHandler(null);

    /**
     * Login constructor. The constructor starts the thread used
     * to periodically re-login to the Kerberos Ticket Granting Server.
     * @param loginContextName
     *               name of section in JAAS file that will be use to login.
     *               Passed as first param to javax.security.auth.login.LoginContext().
     * @throws javax.security.auth.login.LoginException
     *               Thrown if authentication fails.
     */
    public Login(final String loginContextName)
            throws LoginException {
        this.loginContextName = loginContextName;
        this.jaasConfigFilePath = jaasConfigFilePath;
        login = login(loginContextName);
        subject = login.getSubject();
        isKrbTicket = !subject.getPrivateCredentials(KerberosTicket.class).isEmpty();
        AppConfigurationEntry[] entries = Configuration.getConfiguration().getAppConfigurationEntry(loginContextName);
        for (AppConfigurationEntry entry: entries) {
            // there will only be a single entry, so this for() loop will only be iterated through once.
            if (entry.getOptions().get("useTicketCache") != null) {
                String val = (String) entry.getOptions().get("useTicketCache");
                if (val.equals("true"))
                    isUsingTicketCache = true;
            }
            if (entry.getOptions().get("principal") != null)
                principal = (String) entry.getOptions().get("principal");
            break;
        }

        log.debug("checking if its isKrbTicket");
        if (!isKrbTicket) {
            // if no TGT, do not bother with ticket management.
            return;
        }
        log.debug("its a krb5ticket");

        // Refresh the Ticket Granting Ticket (TGT) periodically. How often to refresh is determined by the
        // TGT's existing expiry date and the configured MIN_TIME_BEFORE_RELOGIN. For testing and development,
        // you can decrease the interval of expiration of tickets (for example, to 3 minutes) by running :
        //  "modprinc -maxlife 3mins <principal>" in kadmin.
        t = new Thread(new Runnable() {
            public void run() {
                log.info("TGT refresh thread started.");
                while (true) {  // renewal thread's main loop. if it exits from here, thread will exit.
                    KerberosTicket tgt = getTGT();
                    long now = time.currentWallTime();
                    long nextRefresh;
                    Date nextRefreshDate;
                    if (tgt == null) {
                        nextRefresh = now + MIN_TIME_BEFORE_RELOGIN;
                        nextRefreshDate = new Date(nextRefresh);
                        log.warn("No TGT found: will try again at " + nextRefreshDate);
                    } else {
                        nextRefresh = getRefreshTime(tgt);
                        long expiry = tgt.getEndTime().getTime();
                        Date expiryDate = new Date(expiry);
                        if (isUsingTicketCache && (tgt.getEndTime().equals(tgt.getRenewTill()))) {
                            log.error("The TGT cannot be renewed beyond the next expiry date: " + expiryDate + "." +
                                    "This process will not be able to authenticate new SASL connections after that " +
                                    "time (for example, it will not be authenticate a new connection with a Kafka " +
                                    "Broker).  Ask your system administrator to either increase the " +
                                    "'renew until' time by doing : 'modprinc -maxrenewlife " + principal + "' within " +
                                    "kadmin, or instead, to generate a keytab for " + principal + ". Because the TGT's " +
                                    "expiry cannot be further extended by refreshing, exiting refresh thread now.");
                            return;
                        }
                        // determine how long to sleep from looking at ticket's expiry.
                        // We should not allow the ticket to expire, but we should take into consideration
                        // MIN_TIME_BEFORE_RELOGIN. Will not sleep less than MIN_TIME_BEFORE_RELOGIN, unless doing so
                        // would cause ticket expiration.
                        if ((nextRefresh > expiry) ||
                                ((now + MIN_TIME_BEFORE_RELOGIN) > expiry)) {
                            // expiry is before next scheduled refresh).
                            log.info("refreshing now because expiry is before next scheduled refresh time.");
                            nextRefresh = now;
                        } else {
                            if (nextRefresh < (now + MIN_TIME_BEFORE_RELOGIN)) {
                                // next scheduled refresh is sooner than (now + MIN_TIME_BEFORE_LOGIN).
                                Date until = new Date(nextRefresh);
                                Date newuntil = new Date(now + MIN_TIME_BEFORE_RELOGIN);
                                log.warn("TGT refresh thread time adjusted from : " + until + " to : " + newuntil + " since "
                                        + "the former is sooner than the minimum refresh interval ("
                                        + MIN_TIME_BEFORE_RELOGIN / 1000 + " seconds) from now.");
                            }
                            nextRefresh = Math.max(nextRefresh, now + MIN_TIME_BEFORE_RELOGIN);
                        }
                        nextRefreshDate = new Date(nextRefresh);
                        if (nextRefresh > expiry) {
                            log.error("next refresh: " + nextRefreshDate + " is later than expiry " + expiryDate
                                    + ". This may indicate a clock skew problem. Check that this host and the KDC's "
                                    + "hosts' clocks are in sync. Exiting refresh thread.");
                            return;
                        }
                    }
                    if (now < nextRefresh) {
                        Date until = new Date(nextRefresh);
                        log.info("TGT refresh sleeping until: " + until.toString());
                        try {
                            Thread.sleep(nextRefresh - now);
                        } catch (InterruptedException ie) {
                            log.warn("TGT renewal thread has been interrupted and will exit.");
                            break;
                        }
                    } else {
                        log.error("nextRefresh:" + nextRefreshDate + " is in the past: exiting refresh thread. Check"
                                + " clock sync between this host and KDC - (KDC's clock is likely ahead of this host)."
                                + " Manual intervention will be required for this client to successfully authenticate."
                                + " Exiting refresh thread.");
                        return;
                    }
                    if (isUsingTicketCache) {
                        String cmd = "/usr/bin/kinit";
                        if (System.getProperty("kafka.kinit") != null) {
                            cmd = System.getProperty("kafka.kinit");
                        }
                        String kinitArgs = "-R";
                        int retry = 1;
                        while (retry >= 0) {
                            try {
                                log.debug("running ticket cache refresh command: " + cmd + " " + kinitArgs);
                                Shell.execCommand(cmd, kinitArgs);
                                break;
                            } catch (Exception e) {
                                if (retry > 0) {
                                    --retry;
                                    // sleep for 10 seconds
                                    try {
                                        Thread.sleep(10 * 1000);
                                    } catch (InterruptedException ie) {
                                        log.error("Interrupted while renewing TGT, exiting Login thread");
                                        return;
                                    }
                                } else {
                                    log.warn("Could not renew TGT due to problem running shell command: '" + cmd
                                            + " " + kinitArgs + "'" + "; exception was:" + e + ". Exiting refresh thread.", e);
                                    return;
                                }
                            }
                        }
                    }
                    try {
                        int retry = 1;
                        while (retry >= 0) {
                            try {
                                reLogin();
                                break;
                            } catch (LoginException le) {
                                if (retry > 0) {
                                    --retry;
                                    // sleep for 10 seconds.
                                    try {
                                        Thread.sleep(10 * 1000);
                                    } catch (InterruptedException e) {
                                        log.error("Interrupted during login retry after LoginException:", le);
                                        throw le;
                                    }
                                } else {
                                    log.error("Could not refresh TGT for principal: " + principal + ".", le);
                                }
                            }
                        }
                    } catch (LoginException le) {
                        log.error("Failed to refresh TGT: refresh thread exiting now.", le);
                        break;
                    }
                }
            }
        });
        t.setDaemon(true);
    }

    public void startThreadIfNeeded() {
        // thread object 't' will be null if a refresh thread is not needed.
        if (t != null) {
            t.start();
        }
    }

    public void shutdown() {
        if ((t != null) && (t.isAlive())) {
            t.interrupt();
            try {
                t.join();
            } catch (InterruptedException e) {
                log.warn("error while waiting for Login thread to shutdown: " + e);
            }
        }
    }

    public Subject subject() {
        return subject;
    }

    public String loginContextName() {
        return loginContextName;
    }

    private synchronized LoginContext login(final String loginContextName) throws LoginException {
        if (loginContextName == null) {
            throw new LoginException("loginContext name (JAAS file section header) was null. " +
                    "Please check your java.security.login.auth.config (=" +
                    System.getProperty("java.security.login.auth.config") +
                    ") and your " + JaasUtils.LOGIN_CONTEXT_SERVER + "(=" +
                    System.getProperty(JaasUtils.LOGIN_CONTEXT_CLIENT, "Client") + ")");
        }

        if (System.getProperty("java.security.auth.login.config") == null) {
            throw new IllegalArgumentException("You must pass java.security.auth.login.config in secure mode.");
        }

        File configFile = new File(System.getProperty("java.security.auth.login.config"));
        Configuration loginConf = null;
        try {
            loginConf = Configuration.getInstance("JavaLoginConfig", new URIParameter(configFile.toURI()));
            Configuration.setConfiguration(loginConf);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        LoginContext loginContext = new LoginContext(loginContextName, callbackHandler);
        loginContext.login();
        log.info("successfully logged in.");
        return loginContext;
    }

    private long getRefreshTime(KerberosTicket tgt) {
        long start = tgt.getStartTime().getTime();
        long expires = tgt.getEndTime().getTime();
        log.info("TGT valid starting at:        " + tgt.getStartTime().toString());
        log.info("TGT expires:                  " + tgt.getEndTime().toString());
        long proposedRefresh = start + (long) ((expires - start) *
                (TICKET_RENEW_WINDOW + (TICKET_RENEW_JITTER * rng.nextDouble())));

        if (proposedRefresh > expires)
            // proposedRefresh is too far in the future: it's after ticket expires: simply return now.
            return time.currentWallTime();
        else
            return proposedRefresh;
    }

    private synchronized KerberosTicket getTGT() {
        Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
        for (KerberosTicket ticket: tickets) {
            KerberosPrincipal server = ticket.getServer();
            if (server.getName().equals("krbtgt/" + server.getRealm() + "@" + server.getRealm())) {
                log.debug("Found tgt " + ticket + ".");
                return ticket;
            }
        }
        return null;
    }

    private boolean hasSufficientTimeElapsed() {
        long now = time.currentElapsedTime();
        if (now - getLastLogin() < MIN_TIME_BEFORE_RELOGIN) {
            log.warn("Not attempting to re-login since the last re-login was " +
                    "attempted less than " + (MIN_TIME_BEFORE_RELOGIN / 1000) + " seconds" +
                    " before.");
            return false;
        }
        // register most recent relogin attempt
        setLastLogin(now);
        return true;
    }

    /**
     * Returns login object
     * @return login
     */
    private LoginContext getLogin() {
        return login;
    }

    /**
     * Set the login object
     * @param login
     */
    private void setLogin(LoginContext login) {
        this.login = login;
    }

    /**
     * Set the last login time.
     * @param time the number of milliseconds since the beginning of time
     */
    private void setLastLogin(long time) {
        lastLogin = time;
    }

    /**
     * Get the time of the last login.
     * @return the number of milliseconds since the beginning of time.
     */
    private long getLastLogin() {
        return lastLogin;
    }

    /**
     * Re-login a principal. This method assumes that {@link #login(String)} has happened already.
     * @throws javax.security.auth.login.LoginException on a failure
     */
    private synchronized void reLogin()
            throws LoginException {
        if (!isKrbTicket) {
            return;
        }
        LoginContext login = getLogin();
        if (login  == null) {
            throw new LoginException("login must be done first");
        }
        if (!hasSufficientTimeElapsed()) {
            return;
        }
        log.info("Initiating logout for " + principal);
        synchronized (Login.class) {
            //clear up the kerberos state. But the tokens are not cleared! As per
            //the Java kerberos login module code, only the kerberos credentials
            //are cleared
            login.logout();
            //login and also update the subject field of this instance to
            //have the new credentials (pass it to the LoginContext constructor)
            login = new LoginContext(loginContextName, subject());
            log.info("Initiating re-login for " + principal);
            login.login();
            setLogin(login);
        }
    }
}
