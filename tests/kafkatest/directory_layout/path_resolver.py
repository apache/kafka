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


class PathResolver(object):
    def __init__(self, context, project):
        self.context = context
        self.project = project

    def script(self, script_name, version=None):
        raise NotImplementedError("Subclasses must implement")

    def bin(self, version=None):
        raise NotImplementedError("Subclasses must implement")

    def home(self, version=None):
        raise NotImplementedError("Subclasses must implement")

    def jar(self, jar_name, version=None):
        raise NotImplementedError("Subclasses must implement")

    def scratch_space(self, service_instance):
        raise NotImplementedError("Subclasses must implement")
