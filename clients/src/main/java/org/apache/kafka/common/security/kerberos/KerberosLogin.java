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

import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.Subject;

import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.authenticator.AbstractLogin;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.utils.Shell;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Map;

/**
 * This class is responsible for refreshing Kerberos credentials for
 * logins for both Kafka client and server.
 */
public class KerberosLogin extends AbstractLogin {
    private static final Logger log = LoggerFactory.getLogger(KerberosLogin.class);

    private static final Random RNG = new Random();

    private final Time time = Time.SYSTEM;
    private Thread t;
    private boolean isKrbTicket;
    private boolean isUsingTicketCache;

    private String principal;

    // LoginThread will sleep until 80% of time from last refresh to
    // ticket's expiry has been reached, at which time it will wake
    // and try to renew the ticket.
    private double ticketRenewWindowFactor;

    /**
     * Percentage of random jitter added to the renewal time
     */
    private double ticketRenewJitter;

    // Regardless of ticketRenewWindowFactor setting above and the ticket expiry time,
    // thread will not sleep between refresh attempts any less than 1 minute (60*1000 milliseconds = 1 minute).
    // Change the '1' to e.g. 5, to change this to 5 minutes.
    private long minTimeBeforeRelogin;

    private String kinitCmd;

    private volatile Subject subject;

    private LoginContext loginContext;
    private String serviceName;
    private long lastLogin;

    public void configure(Map<String, ?> configs, JaasContext jaasContext) {
        super.configure(configs, jaasContext);
        this.ticketRenewWindowFactor = (Double) configs.get(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR);
        this.ticketRenewJitter = (Double) configs.get(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER);
        this.minTimeBeforeRelogin = (Long) configs.get(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN);
        this.kinitCmd = (String) configs.get(SaslConfigs.SASL_KERBEROS_KINIT_CMD);
        this.serviceName = getServiceName(configs, jaasContext);
    }

    /**
     * Performs login for each login module specified for the login context of this instance and starts the thread used
     * to periodically re-login to the Kerberos Ticket Granting Server.
     */
    @Override
    public LoginContext login() throws LoginException {

        this.lastLogin = currentElapsedTime();
        loginContext = super.login();
        subject = loginContext.getSubject();
        isKrbTicket = !subject.getPrivateCredentials(KerberosTicket.class).isEmpty();

        List<AppConfigurationEntry> entries = jaasContext().configurationEntries();
        if (entries.isEmpty()) {
            isUsingTicketCache = false;
            principal = null;
        } else {
            // there will only be a single entry
            AppConfigurationEntry entry = entries.get(0);
            if (entry.getOptions().get("useTicketCache") != null) {
                String val = (String) entry.getOptions().get("useTicketCache");
                isUsingTicketCache = val.equals("true");
            } else
                isUsingTicketCache = false;
            if (entry.getOptions().get("principal") != null)
                principal = (String) entry.getOptions().get("principal");
            else
                principal = null;
        }

        if (!isKrbTicket) {
            log.debug("[Principal={}]: It is not a Kerberos ticket", principal);
            t = null;
            // if no TGT, do not bother with ticket management.
            return loginContext;
        }
        log.debug("[Principal={}]: It is a Kerberos ticket", principal);

        // Refresh the Ticket Granting Ticket (TGT) periodically. How often to refresh is determined by the
        // TGT's existing expiry date and the configured minTimeBeforeRelogin. For testing and development,
        // you can decrease the interval of expiration of tickets (for example, to 3 minutes) by running:
        //  "modprinc -maxlife 3mins <principal>" in kadmin.
        t = Utils.newThread(String.format("kafka-kerberos-refresh-thread-%s", principal), new Runnable() {
            public void run() {
                log.info("[Principal={}]: TGT refresh thread started.", principal);
                while (true) {  // renewal thread's main loop. if it exits from here, thread will exit.
                    KerberosTicket tgt = getTGT();
                    long now = currentWallTime();
                    long nextRefresh;
                    Date nextRefreshDate;
                    if (tgt == null) {
                        nextRefresh = now + minTimeBeforeRelogin;
                        nextRefreshDate = new Date(nextRefresh);
                        log.warn("[Principal={}]: No TGT found: will try again at {}", principal, nextRefreshDate);
                    } else {
                        nextRefresh = getRefreshTime(tgt);
                        long expiry = tgt.getEndTime().getTime();
                        Date expiryDate = new Date(expiry);
                        if (isUsingTicketCache && tgt.getRenewTill() != null && tgt.getRenewTill().getTime() < expiry) {
                            log.warn("The TGT cannot be renewed beyond the next expiry date: {}." +
                                    "This process will not be able to authenticate new SASL connections after that " +
                                    "time (for example, it will not be able to authenticate a new connection with a Kafka " +
                                    "Broker).  Ask your system administrator to either increase the " +
                                    "'renew until' time by doing : 'modprinc -maxrenewlife {} ' within " +
                                    "kadmin, or instead, to generate a keytab for {}. Because the TGT's " +
                                    "expiry cannot be further extended by refreshing, exiting refresh thread now.",
                                    expiryDate, principal, principal);
                            return;
                        }
                        // determine how long to sleep from looking at ticket's expiry.
                        // We should not allow the ticket to expire, but we should take into consideration
                        // minTimeBeforeRelogin. Will not sleep less than minTimeBeforeRelogin, unless doing so
                        // would cause ticket expiration.
                        if ((nextRefresh > expiry) || (now + minTimeBeforeRelogin > expiry)) {
                            // expiry is before next scheduled refresh).
                            log.info("[Principal={}]: Refreshing now because expiry is before next scheduled refresh time.", principal);
                            nextRefresh = now;
                        } else {
                            if (nextRefresh < (now + minTimeBeforeRelogin)) {
                                // next scheduled refresh is sooner than (now + MIN_TIME_BEFORE_LOGIN).
                                Date until = new Date(nextRefresh);
                                Date newUntil = new Date(now + minTimeBeforeRelogin);
                                log.warn("[Principal={}]: TGT refresh thread time adjusted from {} to {} since the former is sooner " +
                                        "than the minimum refresh interval ({} seconds) from now.",
                                        principal, until, newUntil, minTimeBeforeRelogin / 1000);
                            }
                            nextRefresh = Math.max(nextRefresh, now + minTimeBeforeRelogin);
                        }
                        nextRefreshDate = new Date(nextRefresh);
                        if (nextRefresh > expiry) {
                            log.error("[Principal={}]: Next refresh: {} is later than expiry {}. This may indicate a clock skew problem." +
                                    "Check that this host and the KDC hosts' clocks are in sync. Exiting refresh thread.",
                                    principal, nextRefreshDate, expiryDate);
                            return;
                        }
                    }
                    if (now < nextRefresh) {
                        Date until = new Date(nextRefresh);
                        log.info("[Principal={}]: TGT refresh sleeping until: {}", principal, until);
                        try {
                            Thread.sleep(nextRefresh - now);
                        } catch (InterruptedException ie) {
                            log.warn("[Principal={}]: TGT renewal thread has been interrupted and will exit.", principal);
                            return;
                        }
                    } else {
                        log.error("[Principal={}]: NextRefresh: {} is in the past: exiting refresh thread. Check"
                                + " clock sync between this host and KDC - (KDC's clock is likely ahead of this host)."
                                + " Manual intervention will be required for this client to successfully authenticate."
                                + " Exiting refresh thread.", principal, nextRefreshDate);
                        return;
                    }
                    if (isUsingTicketCache) {
                        String kinitArgs = "-R";
                        int retry = 1;
                        while (retry >= 0) {
                            try {
                                log.debug("[Principal={}]: Running ticket cache refresh command: {} {}", principal, kinitCmd, kinitArgs);
                                Shell.execCommand(kinitCmd, kinitArgs);
                                break;
                            } catch (Exception e) {
                                if (retry > 0) {
                                    --retry;
                                    // sleep for 10 seconds
                                    try {
                                        Thread.sleep(10 * 1000);
                                    } catch (InterruptedException ie) {
                                        log.error("[Principal={}]: Interrupted while renewing TGT, exiting Login thread", principal);
                                        return;
                                    }
                                } else {
                                    log.warn("[Principal={}]: Could not renew TGT due to problem running shell command: '{} {}'; " +
                                            "exception was: %s. Exiting refresh thread.", 
                                            principal, kinitCmd, kinitArgs, e, e);
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
                                        log.error("[Principal={}]: Interrupted during login retry after LoginException:", principal, le);
                                        throw le;
                                    }
                                } else {
                                    log.error("[Principal={}]: Could not refresh TGT.", principal, le);
                                }
                            }
                        }
                    } catch (LoginException le) {
                        log.error("[Principal={}]: Failed to refresh TGT: refresh thread exiting now.", principal, le);
                        return;
                    }
                }
            }
        }, true);
        t.start();
        return loginContext;
    }

    @Override
    public void close() {
        if ((t != null) && (t.isAlive())) {
            t.interrupt();
            try {
                t.join();
            } catch (InterruptedException e) {
                log.warn("[Principal={}]: Error while waiting for Login thread to shutdown.", principal, e);
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public Subject subject() {
        return subject;
    }

    @Override
    public String serviceName() {
        return serviceName;
    }

    private static String getServiceName(Map<String, ?> configs, JaasContext jaasContext) {
        String jaasServiceName = jaasContext.configEntryOption(JaasUtils.SERVICE_NAME, null);
        String configServiceName = (String) configs.get(SaslConfigs.SASL_KERBEROS_SERVICE_NAME);
        if (jaasServiceName != null && configServiceName != null && !jaasServiceName.equals(configServiceName)) {
            String message = String.format("Conflicting serviceName values found in JAAS and Kafka configs " +
                "value in JAAS file %s, value in Kafka config %s", jaasServiceName, configServiceName);
            throw new IllegalArgumentException(message);
        }

        if (jaasServiceName != null)
            return jaasServiceName;
        if (configServiceName != null)
            return configServiceName;

        throw new IllegalArgumentException("No serviceName defined in either JAAS or Kafka config");
    }


    private long getRefreshTime(KerberosTicket tgt) {
        long start = tgt.getStartTime().getTime();
        long expires = tgt.getEndTime().getTime();
        log.info("[Principal={}]: TGT valid starting at: {}", principal, tgt.getStartTime());
        log.info("[Principal={}]: TGT expires: {}", principal, tgt.getEndTime());
        long proposedRefresh = start + (long) ((expires - start) *
                (ticketRenewWindowFactor + (ticketRenewJitter * RNG.nextDouble())));

        if (proposedRefresh > expires)
            // proposedRefresh is too far in the future: it's after ticket expires: simply return now.
            return currentWallTime();
        else
            return proposedRefresh;
    }

    private synchronized KerberosTicket getTGT() {
        Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
        for (KerberosTicket ticket : tickets) {
            KerberosPrincipal server = ticket.getServer();
            if (server.getName().equals("krbtgt/" + server.getRealm() + "@" + server.getRealm())) {
                log.debug("Found TGT with client principal '{}' and server principal '{}'.", ticket.getClient().getName(),
                        ticket.getServer().getName());
                return ticket;
            }
        }
        return null;
    }

    private boolean hasSufficientTimeElapsed() {
        long now = currentElapsedTime();
        if (now - lastLogin < minTimeBeforeRelogin) {
            log.warn("[Principal={}]: Not attempting to re-login since the last re-login was attempted less than {} seconds before.",
                    principal, minTimeBeforeRelogin / 1000);
            return false;
        }
        return true;
    }

    /**
     * Re-login a principal. This method assumes that {@link #login()} has happened already.
     * @throws javax.security.auth.login.LoginException on a failure
     */
    private synchronized void reLogin() throws LoginException {
        if (!isKrbTicket) {
            return;
        }
        if (loginContext == null) {
            throw new LoginException("Login must be done first");
        }
        if (!hasSufficientTimeElapsed()) {
            return;
        }
        log.info("Initiating logout for {}", principal);
        synchronized (KerberosLogin.class) {
            // register most recent relogin attempt
            lastLogin = currentElapsedTime();
            //clear up the kerberos state. But the tokens are not cleared! As per
            //the Java kerberos login module code, only the kerberos credentials
            //are cleared
            loginContext.logout();
            //login and also update the subject field of this instance to
            //have the new credentials (pass it to the LoginContext constructor)
            loginContext = new LoginContext(jaasContext().name(), subject, null, jaasContext().configuration());
            log.info("Initiating re-login for {}", principal);
            loginContext.login();
        }
    }

    private long currentElapsedTime() {
        return time.hiResClockMs();
    }

    private long currentWallTime() {
        return time.milliseconds();
    }

}
