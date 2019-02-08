# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from collections import defaultdict, namedtuple
import json
from threading import Thread
from select import select
import socket

MetricKey = namedtuple('MetricKey', ['host', 'client_id', 'name', 'group', 'tags'])
MetricValue = namedtuple('MetricValue', ['time', 'value'])

# Python's logging library doesn't define anything more detailed than DEBUG, but we'd like a finer-grained setting for
# for highly detailed messages, e.g. logging every single incoming request.
TRACE = 5


class HttpMetricsCollector(object):
    """
    HttpMetricsCollector enables collection of metrics from various Kafka clients instrumented with the
    PushHttpMetricsReporter. It starts a web server locally and provides the necessary configuration for clients
    to automatically report metrics data to this server. It also provides basic functionality for querying the
    recorded metrics. This class can be used either as a mixin or standalone object.
    """

    # The port to listen on on the worker node, which will be forwarded to the port listening on this driver node
    REMOTE_PORT = 6789

    def __init__(self, **kwargs):
        """
        Create a new HttpMetricsCollector
        :param period the period, in seconds, between updates that the metrics reporter configuration should define.
               defaults to reporting once per second
        :param args:
        :param kwargs:
        """
        self._http_metrics_period = kwargs.pop('period', 1)

        super(HttpMetricsCollector, self).__init__(**kwargs)

        # TODO: currently we maintain just a simple map from all key info -> value. However, some key fields are far
        # more common to filter on, so we'd want to index by them, e.g. host, client.id, metric name.
        self._http_metrics = defaultdict(list)

        self._httpd = HTTPServer(('', 0), _MetricsReceiver)
        self._httpd.parent = self
        self._httpd.metrics = self._http_metrics

        self._http_metrics_thread = Thread(target=self._run_http_metrics_httpd,
                                           name='http-metrics-thread[%s]' % str(self))
        self._http_metrics_thread.start()

        self._forwarders = {}

    @property
    def http_metrics_url(self):
        """
        :return: the URL to use when reporting metrics
        """
        return "http://%s:%d" % ("localhost", self.REMOTE_PORT)

    @property
    def http_metrics_client_configs(self):
        """
        Get client configurations that can be used to report data to this collector. Put these in a properties file for
        clients (e.g. console producer or consumer) to have them push metrics to this driver. Note that in some cases
        (e.g. streams, connect) these settings may need to be prefixed.
        :return: a dictionary of client configurations that will direct a client to report metrics to this collector
        """
        return {
            "metric.reporters": "org.apache.kafka.tools.PushHttpMetricsReporter",
            "metrics.url": self.http_metrics_url,
            "metrics.period": self._http_metrics_period,
        }

    def start_node(self, node):
        local_port = self._httpd.socket.getsockname()[1]
        self.logger.debug('HttpMetricsCollector listening on %s', local_port)
        self._forwarders[self.idx(node)] = _ReverseForwarder(self.logger, node, self.REMOTE_PORT, local_port)

        super(HttpMetricsCollector, self).start_node(node)

    def stop(self):
        super(HttpMetricsCollector, self).stop()

        if self._http_metrics_thread:
            self.logger.debug("Shutting down metrics httpd")
            self._httpd.shutdown()
            self._http_metrics_thread.join()
            self.logger.debug("Finished shutting down metrics httpd")

    def stop_node(self, node):
        super(HttpMetricsCollector, self).stop_node(node)

        idx = self.idx(node)
        self._forwarders[idx].stop()
        del self._forwarders[idx]

    def metrics(self, host=None, client_id=None, name=None, group=None, tags=None):
        """
        Get any collected metrics that match the specified parameters, yielding each as a tuple of
        (key, [<timestamp, value>, ...]) values.
        """
        for k, values in self._http_metrics.iteritems():
            if ((host is None or host == k.host) and
                    (client_id is None or client_id == k.client_id) and
                    (name is None or name == k.name) and
                    (group is None or group == k.group) and
                    (tags is None or tags == k.tags)):
                yield (k, values)

    def _run_http_metrics_httpd(self):
        self._httpd.serve_forever()


class _MetricsReceiver(BaseHTTPRequestHandler):
    """
    HTTP request handler that accepts requests from the PushHttpMetricsReporter and stores them back into the parent
    HttpMetricsCollector
    """

    def log_message(self, format, *args, **kwargs):
        # Don't do any logging here so we get rid of the mostly useless per-request Apache log-style info that spams
        # the debug log
        pass

    def do_POST(self):
        data = self.rfile.read(int(self.headers['Content-Length']))
        data = json.loads(data)
        self.server.parent.logger.log(TRACE, "POST %s\n\n%s\n%s", self.path, self.headers,
                                      json.dumps(data, indent=4, separators=(',', ': ')))
        self.send_response(204)
        self.end_headers()

        client = data['client']
        host = client['host']
        client_id = client['client_id']
        ts = client['time']
        metrics = data['metrics']
        for raw_metric in metrics:
            name = raw_metric['name']
            group = raw_metric['group']
            # Convert to tuple of pairs because dicts & lists are unhashable
            tags = tuple([(k, v) for k, v in raw_metric['tags'].iteritems()]),
            value = raw_metric['value']

            key = MetricKey(host=host, client_id=client_id, name=name, group=group, tags=tags)
            metric_value = MetricValue(time=ts, value=value)

            self.server.metrics[key].append(metric_value)


class _ReverseForwarder(object):
    """
    Runs reverse forwarding of a port on a node to a local port. This allows you to setup a server on the test driver
    that only assumes we have basic SSH access that ducktape guarantees is available for worker nodes.
    """

    def __init__(self, logger, node, remote_port, local_port):
        self.logger = logger
        self._node = node
        self._local_port = local_port
        self._remote_port = remote_port

        self.logger.debug('Forwarding %s port %d to driver port %d', node, remote_port, local_port)

        self._stopping = False

        self._transport = node.account.ssh_client.get_transport()
        self._transport.request_port_forward('', remote_port)

        self._accept_thread = Thread(target=self._accept)
        self._accept_thread.start()

    def stop(self):
        self._stopping = True
        self._accept_thread.join(30)
        if self._accept_thread.isAlive():
            raise RuntimeError("Failed to stop reverse forwarder on %s", self._node)
        self._transport.cancel_port_forward('', self._remote_port)

    def _accept(self):
        while not self._stopping:
            chan = self._transport.accept(1)
            if chan is None:
                continue
            thr = Thread(target=self._handler, args=(chan,))
            thr.setDaemon(True)
            thr.start()

    def _handler(self, chan):
        sock = socket.socket()
        try:
            sock.connect(("localhost", self._local_port))
        except Exception as e:
            self.logger.error('Forwarding request to port %d failed: %r', self._local_port, e)
            return

        self.logger.log(TRACE, 'Connected! Tunnel open %r -> %r -> %d', chan.origin_addr, chan.getpeername(),
                        self._local_port)
        while True:
            r, w, x = select([sock, chan], [], [])
            if sock in r:
                data = sock.recv(1024)
                if len(data) == 0:
                    break
                chan.send(data)
            if chan in r:
                data = chan.recv(1024)
                if len(data) == 0:
                    break
                sock.send(data)
        chan.close()
        sock.close()
        self.logger.log(TRACE, 'Tunnel closed from %r', chan.origin_addr)
