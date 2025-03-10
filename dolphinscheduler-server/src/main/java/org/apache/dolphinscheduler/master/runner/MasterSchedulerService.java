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

package org.apache.dolphinscheduler.master.runner;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.NetUtils;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.master.common.MasterConfig;
import org.apache.dolphinscheduler.master.dispatch.executor.NettyExecutorManager;
import org.apache.dolphinscheduler.master.registry.MasterRegistryClient;
import org.apache.dolphinscheduler.master.registry.ServerNodeManager;
import org.apache.dolphinscheduler.network.NettyRpcClient;
import org.apache.dolphinscheduler.network.config.NettyClientConfig;
import org.apache.dolphinscheduler.service.alert.ProcessAlertManager;
import org.apache.dolphinscheduler.service.process.ProcessService;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MasterSchedulerService extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(MasterSchedulerService.class);
    @Autowired
    private ProcessService processService;
    @Autowired
    private MasterRegistryClient masterRegistryClient;
    @Autowired
    private MasterConfig masterConfig;
    @Autowired
    private ProcessAlertManager processAlertManager;
    private NettyRpcClient nettyRpcClient;
    @Autowired
    NettyExecutorManager nettyExecutorManager;
    private ThreadPoolExecutor masterExecService;
    private ConcurrentHashMap<Integer, WorkflowExecuteThread> processInstanceExecMaps;
    ConcurrentHashMap<Integer, ProcessInstance> processTimeoutCheckList = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, TaskInstance> taskTimeoutCheckList = new ConcurrentHashMap<>();
    private StateWheelExecuteThread stateWheelExecuteThread;

    public void init(ConcurrentHashMap<Integer, WorkflowExecuteThread> processInstanceExecMaps) {
        this.processInstanceExecMaps = processInstanceExecMaps;
        this.masterExecService = (ThreadPoolExecutor) ThreadUtils.newDaemonFixedThreadExecutor("Master-Exec-Thread", masterConfig.getMasterExecThreads());
        NettyClientConfig clientConfig = new NettyClientConfig();
        this.nettyRpcClient = new NettyRpcClient(clientConfig);

        stateWheelExecuteThread = new StateWheelExecuteThread(processTimeoutCheckList,
                taskTimeoutCheckList,
                this.processInstanceExecMaps,
                masterConfig.getStateWheelInterval() * Constants.SLEEP_TIME_MILLIS);

    }

    @Override
    public synchronized void start() {
        super.setName("MasterSchedulerService");
        super.start();
        this.stateWheelExecuteThread.start();
    }

    public void close() {
        masterExecService.shutdown();
        boolean terminated = false;
        try {
            terminated = masterExecService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        }
        if (!terminated) {
            logger.warn("masterExecService shutdown without terminated, increase await time");
        }
        nettyRpcClient.close();
        logger.info("master schedule service stopped...");
    }

    @Override
    public void run() {
        logger.info("master scheduler started");
        while (Stopper.isRunning()) {
            try {
                boolean runCheckFlag = OSUtils.checkResource(masterConfig.getMasterMaxCpuloadAvg(), masterConfig.getMasterReservedMemory());
                if (!runCheckFlag) {
                    Thread.sleep(Constants.SLEEP_TIME_MILLIS);
                    continue;
                }
                scheduleProcess();
            } catch (Exception e) {
                logger.error("master scheduler thread error", e);
            }
        }
    }

    /**
     * 1. get command by slot
     * 2. donot handle command if slot is empty
     *
     * @throws Exception
     */
    private void scheduleProcess() throws Exception {
        int activeCount = masterExecService.getActiveCount();
        // make sure to scan and delete command  table in one transaction
        Command command = findOneCommand();
        if (command != null) {
            logger.info("find one command: id: {}, type: {}", command.getId(), command.getCommandType());
            try {
                ProcessInstance processInstance = processService.handleCommand(logger,
                        getLocalAddress(),
                        this.masterConfig.getMasterExecThreads() - activeCount, command);
                if (processInstance != null) {
                    WorkflowExecuteThread workflowExecuteThread = new WorkflowExecuteThread(
                            processInstance
                            , processService
                            , nettyExecutorManager
                            , processAlertManager
                            , masterConfig
                            , taskTimeoutCheckList);

                    this.processInstanceExecMaps.put(processInstance.getId(), workflowExecuteThread);
                    if (processInstance.getTimeout() > 0) {
                        this.processTimeoutCheckList.put(processInstance.getId(), processInstance);
                    }
                    logger.info("command {} process {} start...",
                            command.getId(), processInstance.getId());
                    masterExecService.execute(workflowExecuteThread);
                }
            } catch (Exception e) {
                logger.error("scan command error ", e);
                processService.moveToErrorCommand(command, e.toString());
            }
        } else {
            //indicate that no command ,sleep for 1s
            Thread.sleep(Constants.SLEEP_TIME_MILLIS);
        }
    }

    private Command findOneCommand() {
        int pageNumber = 0;
        Command result = null;
        while (Stopper.isRunning()) {
            if (ServerNodeManager.MASTER_SIZE == 0) {
                return null;
            }
            List<Command> commandList = processService.findCommandPage(ServerNodeManager.MASTER_SIZE, pageNumber);
            if (commandList.size() == 0) {
                return null;
            }
            for (Command command : commandList) {
                int slot = ServerNodeManager.getSlot();
                if (ServerNodeManager.MASTER_SIZE != 0
                        && command.getId() % ServerNodeManager.MASTER_SIZE == slot) {
                    result = command;
                    break;
                }
            }
            if (result != null) {
                logger.info("find command {}, slot:{} :",
                        result.getId(),
                        ServerNodeManager.getSlot());
                break;
            }
            pageNumber += 1;
        }
        return result;
    }

    private String getLocalAddress() {
        return NetUtils.getAddr(masterConfig.getListenPort());
    }
}
