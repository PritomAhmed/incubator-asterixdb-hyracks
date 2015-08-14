/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.control.cc;

import java.util.*;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.common.base.INodeController;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.common.controllers.NodeRegistration;
import org.apache.hyracks.control.common.heartbeat.HeartbeatData;
import org.apache.hyracks.control.common.heartbeat.HeartbeatSchema;
import org.apache.hyracks.control.common.heartbeat.HeartbeatSchema.GarbageCollectorInfo;

public class NodeControllerState {
    private static final int RRD_SIZE = 720;

    private final INodeController nodeController;

    private final NCConfig ncConfig;

    private final NetworkAddress dataPort;

    private final NetworkAddress datasetPort;

    private final Set<JobId> activeJobIds;

    private final String osName;

    private final String arch;

    private final String osVersion;

    private final int nProcessors;

    private final String vmName;

    private final String vmVersion;

    private final String vmVendor;

    private final String classpath;

    private final String libraryPath;

    private final String bootClasspath;

    private final List<String> inputArguments;

    private final Map<String, String> systemProperties;

    private final HeartbeatSchema hbSchema;

    private final long[] hbTime;

    private final long[] heapInitSize;

    private final long[] heapUsedSize;

    private final long[] heapCommittedSize;

    private final long[] heapMaxSize;

    private final long[] nonheapInitSize;

    private final long[] nonheapUsedSize;

    private final long[] nonheapCommittedSize;

    private final long[] nonheapMaxSize;

    private final int[] threadCount;

    private final int[] peakThreadCount;

    private final double[] systemLoadAverage;

    private final String[] gcNames;

    private final long[][] gcCollectionCounts;

    private final long[][] gcCollectionTimes;

    private final long[] netPayloadBytesRead;

    private final long[] netPayloadBytesWritten;

    private final long[] netSignalingBytesRead;

    private final long[] netSignalingBytesWritten;

    private final long[] datasetNetPayloadBytesRead;

    private final long[] datasetNetPayloadBytesWritten;

    private final long[] datasetNetSignalingBytesRead;

    private final long[] datasetNetSignalingBytesWritten;

    private final long[] ipcMessagesSent;

    private final long[] ipcMessageBytesSent;

    private final long[] ipcMessagesReceived;

    private final long[] ipcMessageBytesReceived;

    private final long[] diskReads;

    private final long[] diskWrites;

    private int rrdPtr;

    private int lastHeartbeatDuration;

    public NodeControllerState(INodeController nodeController, NodeRegistration reg) {
        this.nodeController = nodeController;
        ncConfig = reg.getNCConfig();
        dataPort = reg.getDataPort();
        datasetPort = reg.getDatasetPort();
        activeJobIds = new HashSet<JobId>();

        osName = reg.getOSName();
        arch = reg.getArch();
        osVersion = reg.getOSVersion();
        nProcessors = reg.getNProcessors();
        vmName = reg.getVmName();
        vmVersion = reg.getVmVersion();
        vmVendor = reg.getVmVendor();
        classpath = reg.getClasspath();
        libraryPath = reg.getLibraryPath();
        bootClasspath = reg.getBootClasspath();
        inputArguments = reg.getInputArguments();
        systemProperties = reg.getSystemProperties();

        hbSchema = reg.getHeartbeatSchema();

        hbTime = new long[RRD_SIZE];
        heapInitSize = new long[RRD_SIZE];
        heapUsedSize = new long[RRD_SIZE];
        heapCommittedSize = new long[RRD_SIZE];
        heapMaxSize = new long[RRD_SIZE];
        nonheapInitSize = new long[RRD_SIZE];
        nonheapUsedSize = new long[RRD_SIZE];
        nonheapCommittedSize = new long[RRD_SIZE];
        nonheapMaxSize = new long[RRD_SIZE];
        threadCount = new int[RRD_SIZE];
        peakThreadCount = new int[RRD_SIZE];
        systemLoadAverage = new double[RRD_SIZE];
        GarbageCollectorInfo[] gcInfos = hbSchema.getGarbageCollectorInfos();
        int gcN = gcInfos.length;
        gcNames = new String[gcN];
        for (int i = 0; i < gcN; ++i) {
            gcNames[i] = gcInfos[i].getName();
        }
        gcCollectionCounts = new long[gcN][RRD_SIZE];
        gcCollectionTimes = new long[gcN][RRD_SIZE];
        netPayloadBytesRead = new long[RRD_SIZE];
        netPayloadBytesWritten = new long[RRD_SIZE];
        netSignalingBytesRead = new long[RRD_SIZE];
        netSignalingBytesWritten = new long[RRD_SIZE];
        datasetNetPayloadBytesRead = new long[RRD_SIZE];
        datasetNetPayloadBytesWritten = new long[RRD_SIZE];
        datasetNetSignalingBytesRead = new long[RRD_SIZE];
        datasetNetSignalingBytesWritten = new long[RRD_SIZE];
        ipcMessagesSent = new long[RRD_SIZE];
        ipcMessageBytesSent = new long[RRD_SIZE];
        ipcMessagesReceived = new long[RRD_SIZE];
        ipcMessageBytesReceived = new long[RRD_SIZE];

        diskReads = new long[RRD_SIZE];
        diskWrites = new long[RRD_SIZE];

        rrdPtr = 0;
    }

    public void notifyHeartbeat(HeartbeatData hbData) {
        lastHeartbeatDuration = 0;
        hbTime[rrdPtr] = System.currentTimeMillis();
        if (hbData != null) {
            heapInitSize[rrdPtr] = hbData.heapInitSize;
            heapUsedSize[rrdPtr] = hbData.heapUsedSize;
            heapCommittedSize[rrdPtr] = hbData.heapCommittedSize;
            heapMaxSize[rrdPtr] = hbData.heapMaxSize;
            nonheapInitSize[rrdPtr] = hbData.nonheapInitSize;
            nonheapUsedSize[rrdPtr] = hbData.nonheapUsedSize;
            nonheapCommittedSize[rrdPtr] = hbData.nonheapCommittedSize;
            nonheapMaxSize[rrdPtr] = hbData.nonheapMaxSize;
            threadCount[rrdPtr] = hbData.threadCount;
            peakThreadCount[rrdPtr] = hbData.peakThreadCount;
            systemLoadAverage[rrdPtr] = hbData.systemLoadAverage;
            int gcN = hbSchema.getGarbageCollectorInfos().length;
            for (int i = 0; i < gcN; ++i) {
                gcCollectionCounts[i][rrdPtr] = hbData.gcCollectionCounts[i];
                gcCollectionTimes[i][rrdPtr] = hbData.gcCollectionTimes[i];
            }
            netPayloadBytesRead[rrdPtr] = hbData.netPayloadBytesRead;
            netPayloadBytesWritten[rrdPtr] = hbData.netPayloadBytesWritten;
            netSignalingBytesRead[rrdPtr] = hbData.netSignalingBytesRead;
            netSignalingBytesWritten[rrdPtr] = hbData.netSignalingBytesWritten;
            datasetNetPayloadBytesRead[rrdPtr] = hbData.datasetNetPayloadBytesRead;
            datasetNetPayloadBytesWritten[rrdPtr] = hbData.datasetNetPayloadBytesWritten;
            datasetNetSignalingBytesRead[rrdPtr] = hbData.datasetNetSignalingBytesRead;
            datasetNetSignalingBytesWritten[rrdPtr] = hbData.datasetNetSignalingBytesWritten;
            ipcMessagesSent[rrdPtr] = hbData.ipcMessagesSent;
            ipcMessageBytesSent[rrdPtr] = hbData.ipcMessageBytesSent;
            ipcMessagesReceived[rrdPtr] = hbData.ipcMessagesReceived;
            ipcMessageBytesReceived[rrdPtr] = hbData.ipcMessageBytesReceived;
            diskReads[rrdPtr] = hbData.diskReads;
            diskWrites[rrdPtr] = hbData.diskWrites;
            rrdPtr = (rrdPtr + 1) % RRD_SIZE;
        }
    }

    public int incrementLastHeartbeatDuration() {
        return lastHeartbeatDuration++;
    }

    public int getLastHeartbeatDuration() {
        return lastHeartbeatDuration;
    }

    public INodeController getNodeController() {
        return nodeController;
    }

    public NCConfig getNCConfig() {
        return ncConfig;
    }

    public Set<JobId> getActiveJobIds() {
        return activeJobIds;
    }

    public NetworkAddress getDataPort() {
        return dataPort;
    }

    public NetworkAddress getDatasetPort() {
        return datasetPort;
    }

    public JSONObject toSummaryJSON() throws JSONException {
        JSONObject o = new JSONObject();
        o.put("node-id", ncConfig.nodeId);
        o.put("heap-used", heapUsedSize[(rrdPtr + RRD_SIZE - 1) % RRD_SIZE]);
        o.put("system-load-average", systemLoadAverage[(rrdPtr + RRD_SIZE - 1) % RRD_SIZE]);

        return o;
    }

    public JSONObject toDetailedJSON() throws JSONException {
        JSONObject o = new JSONObject();

        o.put("node-id", ncConfig.nodeId);
        o.put("os-name", osName);
        o.put("arch", arch);
        o.put("os-version", osVersion);
        o.put("num-processors", nProcessors);
        o.put("vm-name", vmName);
        o.put("vm-version", vmVersion);
        o.put("vm-vendor", vmVendor);
        o.put("classpath", classpath);
        o.put("library-path", libraryPath);
        o.put("boot-classpath", bootClasspath);
        o.put("input-arguments", new JSONArray(inputArguments));
        o.put("rrd-ptr", rrdPtr);
        o.put("heartbeat-times", Arrays.copyOfRange(hbTime, 0, rrdPtr));
        o.put("heap-init-sizes", Arrays.copyOfRange(heapInitSize, 0, rrdPtr));
        o.put("heap-used-sizes", Arrays.copyOfRange(heapUsedSize, 0, rrdPtr));
        o.put("heap-committed-sizes", Arrays.copyOfRange(heapCommittedSize, 0, rrdPtr));
        o.put("heap-max-sizes", Arrays.copyOfRange(heapMaxSize, 0, rrdPtr));
        o.put("nonheap-init-sizes", Arrays.copyOfRange(nonheapInitSize, 0, rrdPtr));
        o.put("nonheap-used-sizes", Arrays.copyOfRange(nonheapUsedSize, 0, rrdPtr));
        o.put("nonheap-committed-sizes", Arrays.copyOfRange(nonheapCommittedSize, 0, rrdPtr));
        o.put("nonheap-max-sizes", Arrays.copyOfRange(nonheapMaxSize, 0, rrdPtr));
        o.put("thread-counts", Arrays.copyOfRange(threadCount, 0, rrdPtr));
        o.put("peak-thread-counts", Arrays.copyOfRange(peakThreadCount, 0, rrdPtr));
        o.put("system-load-averages", Arrays.copyOfRange(systemLoadAverage, 0, rrdPtr));
        o.put("gc-names", Arrays.copyOfRange(gcNames, 0, rrdPtr));



        int gcN = hbSchema.getGarbageCollectorInfos().length;
        long[][] tempGccCollectionCounts = new long[gcN][rrdPtr];
        long[][] tempGccCollectionTimes = new long[gcN][rrdPtr];
        for (int i = 0; i < gcN; ++i) {
            for (int j = 0; j< rrdPtr; j++) {
                tempGccCollectionCounts[i][j] = gcCollectionCounts[i][j];
                tempGccCollectionTimes[i][j] = gcCollectionTimes[i][j];
            }
        }
        o.put("gc-collection-counts", Arrays.copyOfRange(tempGccCollectionCounts, 0, gcN));
        o.put("gc-collection-times", Arrays.copyOfRange(tempGccCollectionTimes, 0, gcN));

        o.put("net-payload-bytes-read", Arrays.copyOfRange(netPayloadBytesRead, 0, rrdPtr));
        o.put("net-payload-bytes-written", Arrays.copyOfRange(netPayloadBytesWritten, 0, rrdPtr));
        o.put("net-signaling-bytes-read", Arrays.copyOfRange(netSignalingBytesRead, 0, rrdPtr));
        o.put("net-signaling-bytes-written", Arrays.copyOfRange(netSignalingBytesWritten, 0, rrdPtr));
        o.put("dataset-net-payload-bytes-read", Arrays.copyOfRange(datasetNetPayloadBytesRead, 0, rrdPtr));
        o.put("dataset-net-payload-bytes-written", Arrays.copyOfRange(datasetNetPayloadBytesWritten, 0, rrdPtr));
        o.put("dataset-net-signaling-bytes-read", Arrays.copyOfRange(datasetNetSignalingBytesRead, 0, rrdPtr));
        o.put("dataset-net-signaling-bytes-written", Arrays.copyOfRange(datasetNetSignalingBytesWritten, 0, rrdPtr));
        o.put("ipc-messages-sent", Arrays.copyOfRange(ipcMessagesSent, 0, rrdPtr));
        o.put("ipc-message-bytes-sent", Arrays.copyOfRange(ipcMessageBytesSent, 0, rrdPtr));
        o.put("ipc-messages-received", Arrays.copyOfRange(ipcMessagesReceived, 0, rrdPtr));
        o.put("ipc-message-bytes-received", Arrays.copyOfRange(ipcMessageBytesReceived, 0, rrdPtr));
        o.put("disk-reads", Arrays.copyOfRange(diskReads, 0, rrdPtr));
        o.put("disk-writes", Arrays.copyOfRange(diskWrites, 0, rrdPtr));

        return o;
    }
}