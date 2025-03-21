/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.worker.common;

import static org.apache.dolphinscheduler.common.Constants.DEFAULT_WORKER_GROUP;
import static org.apache.dolphinscheduler.common.Constants.REGISTRY_DOLPHINSCHEDULER_WORKERS;
import static org.apache.dolphinscheduler.common.Constants.SINGLE_SLASH;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.IStoppable;
import org.apache.dolphinscheduler.common.enums.NodeType;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.NetUtils;
import org.apache.dolphinscheduler.network.utils.NamedThreadFactory;
import org.apache.dolphinscheduler.registry.HeartBeatTask;
import org.apache.dolphinscheduler.service.registry.RegistryClient;

import org.apache.commons.lang.StringUtils;

import java.util.Date;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.Sets;

@Service
public class WorkerRegistryClient {

    private final Logger logger = LoggerFactory.getLogger(WorkerRegistryClient.class);
    @Autowired
    private WorkerConfig workerConfig;
    private ScheduledExecutorService heartBeatExecutor;
    private RegistryClient registryClient;
    private String startTime;
    private Set<String> workerGroups;

    @PostConstruct
    public void initWorkRegistry() {
        this.workerGroups = workerConfig.getWorkerGroups();
        this.startTime = DateUtils.dateToString(new Date());
        this.registryClient = RegistryClient.getInstance();
        this.heartBeatExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("HeartBeatExecutor"));
    }

    public void registry() {
        String address = NetUtils.getAddr(workerConfig.getListenPort());
        Set<String> workerZkPaths = getWorkerZkPaths();
        int workerHeartbeatInterval = workerConfig.getWorkerHeartbeatInterval();

        for (String workerZKPath : workerZkPaths) {
            registryClient.persistEphemeral(workerZKPath, "");
            logger.info("worker node : {} registry to ZK {} successfully", address, workerZKPath);
        }

        HeartBeatTask heartBeatTask = new HeartBeatTask(startTime,
                workerConfig.getWorkerMaxCpuloadAvg(),
                workerConfig.getWorkerReservedMemory(),
                workerConfig.getHostWeight(),
                workerZkPaths,
                Constants.WORKER_TYPE,
                registryClient);

        this.heartBeatExecutor.scheduleAtFixedRate(heartBeatTask, workerHeartbeatInterval, workerHeartbeatInterval, TimeUnit.SECONDS);
        logger.info("worker node : {} heartbeat interval {} s", address, workerHeartbeatInterval);
    }

    public void unRegistry() {
        String address = getLocalAddress();
        Set<String> workerZkPaths = getWorkerZkPaths();
        for (String workerZkPath : workerZkPaths) {
            registryClient.remove(workerZkPath);
            logger.info("worker node : {} unRegistry from ZK {}.", address, workerZkPath);
        }
        this.heartBeatExecutor.shutdownNow();
        logger.info("heartbeat executor shutdown");
        registryClient.close();
    }

    public Set<String> getWorkerZkPaths() {
        Set<String> workerPaths = Sets.newHashSet();
        String address = getLocalAddress();

        for (String workGroup : this.workerGroups) {
            StringJoiner workerPathJoiner = new StringJoiner(SINGLE_SLASH);
            workerPathJoiner.add(REGISTRY_DOLPHINSCHEDULER_WORKERS);
            if (StringUtils.isEmpty(workGroup)) {
                workGroup = DEFAULT_WORKER_GROUP;
            }
            // trim and lower case is need
            workerPathJoiner.add(workGroup.trim().toLowerCase());
            workerPathJoiner.add(address);
            workerPaths.add(workerPathJoiner.toString());
        }
        return workerPaths;
    }

    public void handleDeadServer(Set<String> nodeSet, NodeType nodeType, String opType) throws Exception {
        registryClient.handleDeadServer(nodeSet, nodeType, opType);
    }

    private String getLocalAddress() {
        return NetUtils.getAddr(workerConfig.getListenPort());
    }

    public void setRegistryStoppable(IStoppable stoppable) {
        registryClient.setStoppable(stoppable);
    }

}
