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

class ListenerSecurityConfig:

    SASL_MECHANISM_PREFIXED_CONFIGS = ["connections.max.reauth.ms", "sasl.jaas.config",
                                       "sasl.login.callback.handler.class", "sasl.login.class",
                                       "sasl.server.callback.handler.class"]

    def __init__(self, use_separate_interbroker_listener=False,
                 client_listener_overrides={}, interbroker_listener_overrides={}):
        """
        :param bool use_separate_interbroker_listener - if set, will use a separate interbroker listener,
        with security protocol set to interbroker_security_protocol value. If set, requires
        interbroker_security_protocol to be provided.
        Normally port name is the same as its security protocol, so setting security_protocol and
        interbroker_security_protocol to the same value will lead to a single port being open and both client
        and broker-to-broker communication will go over that port. This parameter allows
        you to add an interbroker listener with the same security protocol as a client listener, but running on a
        separate port.
        :param dict client_listener_overrides - non-prefixed listener config overrides for named client listener
        (for example 'sasl.jaas.config', 'ssl.keystore.location', 'sasl.login.callback.handler.class', etc).
        :param dict interbroker_listener_overrides - non-prefixed listener config overrides for named interbroker
        listener (for example 'sasl.jaas.config', 'ssl.keystore.location', 'sasl.login.callback.handler.class', etc).
        """
        self.use_separate_interbroker_listener = use_separate_interbroker_listener
        self.client_listener_overrides = client_listener_overrides
        self.interbroker_listener_overrides = interbroker_listener_overrides

    def requires_sasl_mechanism_prefix(self, config):
        return config in ListenerSecurityConfig.SASL_MECHANISM_PREFIXED_CONFIGS
