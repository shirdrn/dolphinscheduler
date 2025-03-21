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

package org.apache.dolphinscheduler.master.registry;

import static org.apache.dolphinscheduler.common.Constants.REGISTRY_DOLPHINSCHEDULER_MASTERS;
import static org.apache.dolphinscheduler.common.Constants.REGISTRY_DOLPHINSCHEDULER_WORKERS;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.NodeType;
import org.apache.dolphinscheduler.common.model.Server;
import org.apache.dolphinscheduler.common.utils.NetUtils;
import org.apache.dolphinscheduler.dao.AlertDao;
import org.apache.dolphinscheduler.dao.entity.WorkerGroup;
import org.apache.dolphinscheduler.dao.mapper.WorkerGroupMapper;
import org.apache.dolphinscheduler.network.utils.NamedThreadFactory;
import org.apache.dolphinscheduler.service.queue.MasterPriorityQueue;
import org.apache.dolphinscheduler.service.registry.RegistryClient;
import org.apache.dolphinscheduler.spi.register.DataChangeEvent;
import org.apache.dolphinscheduler.spi.register.SubscribeListener;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ServerNodeManager implements InitializingBean {

    private final Logger logger = LoggerFactory.getLogger(ServerNodeManager.class);
    private final Lock masterLock = new ReentrantLock();
    private final Lock workerGroupLock = new ReentrantLock();
    private final Lock workerNodeInfoLock = new ReentrantLock();
    private final ConcurrentHashMap<String, Set<String>> workerGroupNodes = new ConcurrentHashMap<>();
    private final Set<String> masterNodes = new HashSet<>();
    private final Map<String, String> workerNodeInfo = new HashMap<>();
    private ScheduledExecutorService executorService;
    private RegistryClient registryClient = RegistryClient.getInstance();

    /** eg : /node/worker/group/127.0.0.1:xxx */
    private static final int WORKER_LISTENER_CHECK_LENGTH = 5;
    @Autowired
    private WorkerGroupMapper workerGroupMapper;
    private MasterPriorityQueue masterPriorityQueue = new MasterPriorityQueue();
    @Autowired
    private AlertDao alertDao;
    public static volatile List<Integer> SLOT_LIST = new ArrayList<>();
    public static volatile Integer MASTER_SIZE = 0;

    public static Integer getSlot() {
        if (SLOT_LIST.size() > 0) {
            return SLOT_LIST.get(0);
        }
        return 0;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // load nodes from zookeeper
        load();
        executorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("ServerNodeManagerExecutor"));
        executorService.scheduleWithFixedDelay(new WorkerNodeInfoAndGroupDbSyncTask(), 0, 10, TimeUnit.SECONDS);
        registryClient.subscribe(REGISTRY_DOLPHINSCHEDULER_MASTERS, new MasterDataListener());
        registryClient.subscribe(REGISTRY_DOLPHINSCHEDULER_WORKERS, new WorkerDataListener());
    }

    public void load() {
        updateMasterNodes();
        Set<String> workerGroups = registryClient.getWorkerGroupDirectly();
        for (String workerGroup : workerGroups) {
            syncWorkerGroupNodes(workerGroup, registryClient.getWorkerGroupNodesDirectly(workerGroup));
        }
    }

    class WorkerNodeInfoAndGroupDbSyncTask implements Runnable {

        @Override
        public void run() {
            // sync worker node info
            Map<String, String> newWorkerNodeInfo = registryClient.getServerMaps(NodeType.WORKER, true);
            syncWorkerNodeInfo(newWorkerNodeInfo);

            // sync worker group nodes from database
            List<WorkerGroup> workerGroupList = workerGroupMapper.queryAllWorkerGroup();
            if (CollectionUtils.isNotEmpty(workerGroupList)) {
                for (WorkerGroup wg : workerGroupList) {
                    String workerGroup = wg.getName();
                    Set<String> nodes = new HashSet<>();
                    String[] addrs = wg.getAddrList().split(Constants.COMMA);
                    for (String addr : addrs) {
                        if (newWorkerNodeInfo.containsKey(addr)) {
                            nodes.add(addr);
                        }
                    }
                    if (!nodes.isEmpty()) {
                        syncWorkerGroupNodes(workerGroup, nodes);
                    }
                }
            }
        }
    }

    class WorkerDataListener implements SubscribeListener {

        @Override
        public void notify(String path, DataChangeEvent dataChangeEvent) {
            if (registryClient.isWorkerPath(path)) {
                try {
                    if (dataChangeEvent == DataChangeEvent.ADD) {
                        logger.info("worker group node : {} added.", path);
                        String group = parseGroup(path);
                        Set<String> currentNodes = registryClient.getWorkerGroupNodesDirectly(group);
                        logger.info("currentNodes : {}", currentNodes);
                        syncWorkerGroupNodes(group, currentNodes);
                    } else if (dataChangeEvent == DataChangeEvent.REMOVE) {
                        logger.info("worker group node : {} down.", path);
                        String group = parseGroup(path);
                        Set<String> currentNodes = registryClient.getWorkerGroupNodesDirectly(group);
                        syncWorkerGroupNodes(group, currentNodes);
                        alertDao.sendServerStopedAlert(1, path, "WORKER");
                    }
                } catch (IllegalArgumentException ex) {
                    logger.warn(ex.getMessage());
                } catch (Exception ex) {
                    logger.error("WorkerGroupListener capture data change and get data failed", ex);
                }

            }
        }

        private String parseGroup(String path) {
            String[] parts = path.split("/");
            if (parts.length < WORKER_LISTENER_CHECK_LENGTH) {
                throw new IllegalArgumentException(String.format("worker group path : %s is not valid, ignore", path));
            }
            return parts[parts.length - 2];
        }

    }

    class MasterDataListener implements SubscribeListener {
        @Override
        public void notify(String path, DataChangeEvent dataChangeEvent) {
            if (registryClient.isMasterPath(path)) {
                try {
                    if (dataChangeEvent.equals(DataChangeEvent.ADD)) {
                        logger.info("master node : {} added.", path);
                        updateMasterNodes();
                    }
                    if (dataChangeEvent.equals(DataChangeEvent.REMOVE)) {
                        logger.info("master node : {} down.", path);
                        updateMasterNodes();
                        alertDao.sendServerStopedAlert(1, path, "MASTER");
                    }
                } catch (Exception ex) {
                    logger.error("MasterNodeListener capture data change and get data failed.", ex);
                }
            }
        }
    }

    private void updateMasterNodes() {
        SLOT_LIST.clear();
        this.masterNodes.clear();
        String nodeLock = registryClient.getMasterLockPath();
        try {
            registryClient.getLock(nodeLock);
            Set<String> currentNodes = registryClient.getMasterNodesDirectly();
            List<Server> masterNodes = registryClient.getServerList(NodeType.MASTER);
            syncMasterNodes(currentNodes, masterNodes);
        } catch (Exception e) {
            logger.error("update master nodes error", e);
        } finally {
            registryClient.releaseLock(nodeLock);
        }

    }

    public Set<String> getMasterNodes() {
        masterLock.lock();
        try {
            return Collections.unmodifiableSet(masterNodes);
        } finally {
            masterLock.unlock();
        }
    }

    private void syncMasterNodes(Set<String> nodes, List<Server> masterNodes) {
        masterLock.lock();
        try {
            this.masterNodes.addAll(nodes);
            this.masterPriorityQueue.clear();
            this.masterPriorityQueue.putList(masterNodes);
            int index = masterPriorityQueue.getIndex(NetUtils.getHost());
            if (index >= 0) {
                MASTER_SIZE = nodes.size();
                SLOT_LIST.add(masterPriorityQueue.getIndex(NetUtils.getHost()));
            }
            logger.info("update master nodes, master size: {}, slot: {}",
                    MASTER_SIZE, SLOT_LIST.toString()
            );
        } finally {
            masterLock.unlock();
        }
    }

    private void syncWorkerGroupNodes(String workerGroup, Set<String> nodes) {
        workerGroupLock.lock();
        try {
            workerGroup = workerGroup.toLowerCase();
            Set<String> workerNodes = workerGroupNodes.getOrDefault(workerGroup, new HashSet<>());
            workerNodes.clear();
            workerNodes.addAll(nodes);
            workerGroupNodes.put(workerGroup, workerNodes);
        } finally {
            workerGroupLock.unlock();
        }
    }

    public Map<String, Set<String>> getWorkerGroupNodes() {
        return Collections.unmodifiableMap(workerGroupNodes);
    }

    public Set<String> getWorkerGroupNodes(String workerGroup) {
        workerGroupLock.lock();
        try {
            if (StringUtils.isEmpty(workerGroup)) {
                workerGroup = Constants.DEFAULT_WORKER_GROUP;
            }
            workerGroup = workerGroup.toLowerCase();
            Set<String> nodes = workerGroupNodes.get(workerGroup);
            if (CollectionUtils.isNotEmpty(nodes)) {
                return Collections.unmodifiableSet(nodes);
            }
            return nodes;
        } finally {
            workerGroupLock.unlock();
        }
    }

    public Map<String, String> getWorkerNodeInfo() {
        return Collections.unmodifiableMap(workerNodeInfo);
    }

    public String getWorkerNodeInfo(String workerNode) {
        workerNodeInfoLock.lock();
        try {
            return workerNodeInfo.getOrDefault(workerNode, null);
        } finally {
            workerNodeInfoLock.unlock();
        }
    }

    private void syncWorkerNodeInfo(Map<String, String> newWorkerNodeInfo) {
        workerNodeInfoLock.lock();
        try {
            workerNodeInfo.clear();
            workerNodeInfo.putAll(newWorkerNodeInfo);
        } finally {
            workerNodeInfoLock.unlock();
        }
    }

    @PreDestroy
    public void destroy() {
        executorService.shutdownNow();
    }

}
