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

package org.apache.dolphinscheduler.master.dispatch.executor;

import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.master.common.ExecutionContext;
import org.apache.dolphinscheduler.remote.NettyRemotingClient;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.config.NettyClientConfig;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.apache.dolphinscheduler.master.dispatch.ExecutorType;
import org.apache.dolphinscheduler.master.common.ExecuteException;
import org.apache.dolphinscheduler.master.processor.TaskAckProcessor;
import org.apache.dolphinscheduler.master.processor.TaskKillResponseProcessor;
import org.apache.dolphinscheduler.master.processor.TaskResponseProcessor;
import org.apache.dolphinscheduler.master.registry.ServerNodeManager;

import org.apache.commons.collections.CollectionUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class NettyExecutorManager extends AbstractExecutorManager<Boolean>{

    private final Logger logger = LoggerFactory.getLogger(NettyExecutorManager.class);
    @Autowired
    private ServerNodeManager serverNodeManager;
    private final NettyRemotingClient nettyRemotingClient;

    public NettyExecutorManager(){
        final NettyClientConfig clientConfig = new NettyClientConfig();
        this.nettyRemotingClient = new NettyRemotingClient(clientConfig);
    }

    @PostConstruct
    public void init(){
        // register several processors
        this.nettyRemotingClient.registerProcessor(CommandType.TASK_EXECUTE_RESPONSE, new TaskResponseProcessor());
        this.nettyRemotingClient.registerProcessor(CommandType.TASK_EXECUTE_ACK, new TaskAckProcessor());
        this.nettyRemotingClient.registerProcessor(CommandType.TASK_KILL_RESPONSE, new TaskKillResponseProcessor());
    }

    @Override
    public Boolean execute(ExecutionContext context) throws ExecuteException {
        Set<String> allNodes = getAllNodes(context);
        Set<String> failNodeSet = new HashSet<>();
        Command command = context.getCommand();
        Host host = context.getHost();
        boolean success = false;
        while (!success) {
            try {
                doExecute(host,command);
                success = true;
                context.setHost(host);
            } catch (ExecuteException ex) {
                logger.error(String.format("execute command : %s error", command), ex);
                try {
                    failNodeSet.add(host.getAddress());
                    Set<String> tmpAllIps = new HashSet<>(allNodes);
                    Collection<String> remained = CollectionUtils.subtract(tmpAllIps, failNodeSet);
                    if (remained != null && remained.size() > 0) {
                        host = Host.of(remained.iterator().next());
                        logger.error("retry execute command : {} host : {}", command, host);
                    } else {
                        throw new ExecuteException("fail after try all nodes");
                    }
                } catch (Throwable t) {
                    throw new ExecuteException("fail after try all nodes");
                }
            }
        }
        return success;
    }

    @Override
    public void executeDirectly(ExecutionContext context) throws ExecuteException {
        Host host = context.getHost();
        doExecute(host, context.getCommand());
    }

    public void doExecute(final Host host, final Command command) throws ExecuteException {
        //  retry times，default 3
        int retryCount = 3;
        boolean success = false;
        do {
            try {
                nettyRemotingClient.send(host, command);
                success = true;
            } catch (Exception ex) {
                logger.error(String.format("send command : %s to %s error", command, host), ex);
                retryCount--;
                ThreadUtils.sleep(100);
            }
        } while (retryCount >= 0 && !success);

        if (!success) {
            throw new ExecuteException(String.format("send command : %s to %s error", command, host));
        }
    }

    private Set<String> getAllNodes(ExecutionContext context){
        Set<String> nodes = Collections.emptySet();
        ExecutorType executorType = context.getExecutorType();
        switch (executorType){
            case WORKER:
                nodes = serverNodeManager.getWorkerGroupNodes(context.getWorkerGroup());
                break;
            case CLIENT:
                break;
            default:
                throw new IllegalArgumentException("invalid executor type : " + executorType);

        }
        return nodes;
    }

    public NettyRemotingClient getNettyRemotingClient() {
        return nettyRemotingClient;
    }
}
