# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#!/usr/bin/env python

# ===================================
# file: metrics.py
# ===================================

import inspect
import json
import logging
import os
import signal
import subprocess
import sys
import traceback

import csv
import time 
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from collections import namedtuple
import numpy

from pyh import *
import kafka_system_test_utils
import system_test_utils

logger     = logging.getLogger("namedLogger")
thisClassName = '(metrics)'
d = {'name_of_class': thisClassName}

attributeNameToNameInReportedFileMap = {
    'Min': 'min',
    'Max': 'max',
    'Mean': 'mean',
    '50thPercentile': 'median',
    'StdDev': 'stddev',
    '95thPercentile': '95%',
    '99thPercentile': '99%',
    '999thPercentile': '99.9%',
    'Count': 'count',
    'OneMinuteRate': '1 min rate',
    'MeanRate': 'mean rate',
    'FiveMinuteRate': '5 min rate',
    'FifteenMinuteRate': '15 min rate',
    'Value': 'value'
}

def getCSVFileNameFromMetricsMbeanName(mbeanName):
    return mbeanName.replace(":type=", ".").replace(",name=", ".") + ".csv"

def read_metrics_definition(metricsFile):
    metricsFileData = open(metricsFile, "r").read()
    metricsJsonData = json.loads(metricsFileData)
    allDashboards = metricsJsonData['dashboards']
    allGraphs = []
    for dashboard in allDashboards:
        dashboardName = dashboard['name']
        graphs = dashboard['graphs']
        for graph in graphs:
            bean = graph['bean_name']
            allGraphs.append(graph)
            attributes = graph['attributes']
            #print "Filtering on attributes " + attributes     
    return allGraphs
            
def get_dashboard_definition(metricsFile, role):
    metricsFileData = open(metricsFile, "r").read()
    metricsJsonData = json.loads(metricsFileData)
    allDashboards = metricsJsonData['dashboards']
    dashboardsForRole = []
    for dashboard in allDashboards:
        if dashboard['role'] == role:
            dashboardsForRole.append(dashboard) 
    return dashboardsForRole

def ensure_valid_headers(headers, attributes):
    if headers[0] != "# time":
        raise Exception("First column should be time")
    for header in headers:
        logger.debug(header, extra=d)
    # there should be exactly one column with a name that matches attributes
    try:
        attributeColumnIndex = headers.index(attributes)
        return attributeColumnIndex
    except ValueError as ve:
        #print "#### attributes : ", attributes
        #print "#### headers    : ", headers
        raise Exception("There should be exactly one column that matches attribute: {0} in".format(attributes) +  
                        " headers: {0}".format(",".join(headers)))
        
def plot_graphs(inputCsvFiles, labels, title, xLabel, yLabel, attribute, outputGraphFile):
    if not inputCsvFiles: return

    # create empty plot
    fig=plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax=fig.add_subplot(111)
    labelx = -0.3  # axes coords
    ax.set_xlabel(xLabel)
    ax.set_ylabel(yLabel)
    ax.grid()
    #ax.yaxis.set_label_coords(labelx, 0.5)
    Coordinates = namedtuple("Coordinates", 'x y')
    plots = []
    coordinates = []
    # read data for all files, organize by label in a dict
    for fileAndLabel in zip(inputCsvFiles, labels):
        inputCsvFile = fileAndLabel[0]
        label = fileAndLabel[1]
        csv_reader = list(csv.reader(open(inputCsvFile, "rb")))
        x,y = [],[]
        xticks_labels = []
        try:
            # read first line as the headers
            headers = csv_reader.pop(0)
            attributeColumnIndex = ensure_valid_headers(headers, attributeNameToNameInReportedFileMap[attribute])
            logger.debug("Column index for attribute {0} is {1}".format(attribute, attributeColumnIndex), extra=d)
            start_time = (int)(os.path.getctime(inputCsvFile) * 1000)
            int(csv_reader[0][0])
            for line in csv_reader:
                if(len(line) == 0):
                    continue
                yVal = float(line[attributeColumnIndex])                
                xVal = int(line[0])
                y.append(yVal)
                epoch= start_time + int(line[0])
                x.append(xVal)
                xticks_labels.append(time.strftime("%H:%M:%S", time.localtime(epoch)))
                coordinates.append(Coordinates(xVal, yVal))
            p1 = ax.plot(x,y)
            plots.append(p1)
        except Exception as e:
            logger.error("ERROR while plotting data for {0}: {1}".format(inputCsvFile, e), extra=d)
            traceback.print_exc()
    # find xmin, xmax, ymin, ymax from all csv files
    xmin = min(map(lambda coord: coord.x, coordinates))
    xmax = max(map(lambda coord: coord.x, coordinates))
    ymin = min(map(lambda coord: coord.y, coordinates))
    ymax = max(map(lambda coord: coord.y, coordinates))
    # set x and y axes limits
    plt.xlim(xmin, xmax)
    plt.ylim(ymin, ymax)
    # set ticks accordingly
    xticks = numpy.arange(xmin, xmax, 0.2*xmax)
#    yticks = numpy.arange(ymin, ymax)
    plt.xticks(xticks,xticks_labels,rotation=17)
#    plt.yticks(yticks)
    plt.legend(plots,labels, loc=2)
    plt.title(title)
    plt.savefig(outputGraphFile)

def draw_all_graphs(metricsDescriptionFile, testcaseEnv, clusterConfig):
    # go through each role and plot graphs for the role's metrics
    roles = set(map(lambda config: config['role'], clusterConfig))
    for role in roles:
        dashboards = get_dashboard_definition(metricsDescriptionFile, role)
        entities = kafka_system_test_utils.get_entities_for_role(clusterConfig, role)
        for dashboard in dashboards:
            graphs = dashboard['graphs']
            # draw each graph for all entities
            draw_graph_for_role(graphs, entities, role, testcaseEnv)
        
def draw_graph_for_role(graphs, entities, role, testcaseEnv):
    for graph in graphs:
        graphName = graph['graph_name'] 
        yLabel = graph['y_label']
        inputCsvFiles = []
        graphLegendLabels = []
        for entity in entities:
            entityMetricsDir = kafka_system_test_utils.get_testcase_config_log_dir_pathname(testcaseEnv, role, entity['entity_id'], "metrics")
            entityMetricCsvFile = entityMetricsDir + "/" + getCSVFileNameFromMetricsMbeanName(graph['bean_name'])
            if(not os.path.exists(entityMetricCsvFile)):
                logger.warn("The file {0} does not exist for plotting".format(entityMetricCsvFile), extra=d)
            else:
                inputCsvFiles.append(entityMetricCsvFile)
                graphLegendLabels.append(role + "-" + entity['entity_id'])
#            print "Plotting graph for metric {0} on entity {1}".format(graph['graph_name'], entity['entity_id'])
        try:
            # plot one graph per mbean attribute
            labels = graph['y_label'].split(',')
            fullyQualifiedAttributeNames = map(lambda attribute: graph['bean_name'] + ':' + attribute, 
                                           graph['attributes'].split(','))
            attributes = graph['attributes'].split(',')
            for labelAndAttribute in zip(labels, fullyQualifiedAttributeNames, attributes):            
                outputGraphFile = testcaseEnv.testCaseDashboardsDir + "/" + role + "/" + labelAndAttribute[1] + ".svg"            
                plot_graphs(inputCsvFiles, graphLegendLabels, graph['graph_name'] + '-' + labelAndAttribute[2], 
                            "time", labelAndAttribute[0], labelAndAttribute[2], outputGraphFile)
#            print "Finished plotting graph for metric {0} on entity {1}".format(graph['graph_name'], entity['entity_id'])
        except Exception as e:
            logger.error("ERROR while plotting graph {0}: {1}".format(outputGraphFile, e), extra=d)
            traceback.print_exc()

def build_all_dashboards(metricsDefinitionFile, testcaseDashboardsDir, clusterConfig):
    metricsHtmlFile = testcaseDashboardsDir + "/metrics.html"
    centralDashboard = PyH('Kafka Metrics Dashboard')
    centralDashboard << h1('Kafka Metrics Dashboard', cl='center')
    roles = set(map(lambda config: config['role'], clusterConfig))
    for role in roles:
        entities = kafka_system_test_utils.get_entities_for_role(clusterConfig, role)
        dashboardPagePath = build_dashboard_for_role(metricsDefinitionFile, role, 
                                                     entities, testcaseDashboardsDir)
        centralDashboard << a(role, href = dashboardPagePath)
        centralDashboard << br()
            
    centralDashboard.printOut(metricsHtmlFile)

def build_dashboard_for_role(metricsDefinitionFile, role, entities, testcaseDashboardsDir):
    # build all dashboards for the input entity's based on its role. It can be one of kafka, zookeeper, producer
    # consumer
    dashboards = get_dashboard_definition(metricsDefinitionFile, role)
    entityDashboard = PyH('Kafka Metrics Dashboard for ' + role)
    entityDashboard << h1('Kafka Metrics Dashboard for ' + role, cl='center')
    entityDashboardHtml = testcaseDashboardsDir + "/" + role + "-dashboards.html"
    for dashboard in dashboards:
        # place the graph svg files in this dashboard
        allGraphs = dashboard['graphs']
        for graph in allGraphs:
            attributes = map(lambda attribute: graph['bean_name'] + ':' + attribute, 
                                           graph['attributes'].split(','))
            for attribute in attributes:                
                graphFileLocation = testcaseDashboardsDir + "/" + role + "/" + attribute + ".svg"
                entityDashboard << embed(src = graphFileLocation, type = "image/svg+xml")
    entityDashboard.printOut(entityDashboardHtml)
    return entityDashboardHtml

def start_metrics_collection(jmxHost, jmxPort, role, entityId, systemTestEnv, testcaseEnv):
    logger.info("starting metrics collection on jmx port : " + jmxPort, extra=d)
    jmxUrl = "service:jmx:rmi:///jndi/rmi://" + jmxHost + ":" + jmxPort + "/jmxrmi"
    clusterConfig = systemTestEnv.clusterEntityConfigDictList
    metricsDefinitionFile = systemTestEnv.METRICS_PATHNAME
    entityMetricsDir = kafka_system_test_utils.get_testcase_config_log_dir_pathname(testcaseEnv, role, entityId, "metrics")
    dashboardsForRole = get_dashboard_definition(metricsDefinitionFile, role)
    mbeansForRole = get_mbeans_for_role(dashboardsForRole)
    
    kafkaHome = system_test_utils.get_data_by_lookup_keyval(clusterConfig, "entity_id", entityId, "kafka_home")
    javaHome  = system_test_utils.get_data_by_lookup_keyval(clusterConfig, "entity_id", entityId, "java_home")
    
    for mbean in mbeansForRole:
        outputCsvFile = entityMetricsDir + "/" + mbean + ".csv"
        startMetricsCmdList = ["ssh " + jmxHost,
                               "'JAVA_HOME=" + javaHome,
                               "JMX_PORT= " + kafkaHome + "/bin/kafka-run-class.sh kafka.tools.JmxTool",
                               "--jmx-url " + jmxUrl,
                               "--object-name " + mbean + " 1> ",
                                outputCsvFile + " & echo pid:$! > ",
                                entityMetricsDir + "/entity_pid'"]

        startMetricsCommand = " ".join(startMetricsCmdList) 
        logger.debug("executing command: [" + startMetricsCommand + "]", extra=d)
        system_test_utils.async_sys_call(startMetricsCommand)
        time.sleep(1)

        pidCmdStr = "ssh " + jmxHost + " 'cat " + entityMetricsDir + "/entity_pid' 2> /dev/null"
        logger.debug("executing command: [" + pidCmdStr + "]", extra=d)
        subproc = system_test_utils.sys_call_return_subproc(pidCmdStr)

        # keep track of JMX ppid in a dictionary of entity_id to list of JMX ppid
        # testcaseEnv.entityJmxParentPidDict:
        #   key: entity_id
        #   val: list of JMX ppid associated to that entity_id
        #   { 1: [1234, 1235, 1236], 2: [2234, 2235, 2236], ... }
        for line in subproc.stdout.readlines():
            line = line.rstrip('\n')
            logger.debug("line: [" + line + "]", extra=d)
            if line.startswith("pid"):
                logger.debug("found pid line: [" + line + "]", extra=d)
                tokens  = line.split(':')
                thisPid = tokens[1]
                if entityId not in testcaseEnv.entityJmxParentPidDict:
                    testcaseEnv.entityJmxParentPidDict[entityId] = []
                testcaseEnv.entityJmxParentPidDict[entityId].append(thisPid)
                #print "\n#### testcaseEnv.entityJmxParentPidDict ", testcaseEnv.entityJmxParentPidDict, "\n"


def stop_metrics_collection(jmxHost, jmxPort):
    logger.info("stopping metrics collection on " + jmxHost + ":" + jmxPort, extra=d)
    system_test_utils.sys_call("ps -ef | grep JmxTool | grep -v grep | grep " + jmxPort + " | awk '{print $2}' | xargs kill -9")

def get_mbeans_for_role(dashboardsForRole):
    graphs = reduce(lambda x,y: x+y, map(lambda dashboard: dashboard['graphs'], dashboardsForRole))
    return set(map(lambda metric: metric['bean_name'], graphs))
