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

package org.apache.dolphinscheduler.master.dispatch;


import org.apache.dolphinscheduler.master.common.ExecuteException;
import org.apache.dolphinscheduler.master.common.ExecutionContext;
import org.apache.dolphinscheduler.master.dispatch.executor.ExecutorManager;
import org.apache.dolphinscheduler.master.dispatch.executor.NettyExecutorManager;
import org.apache.dolphinscheduler.master.dispatch.host.HostManager;
import org.apache.dolphinscheduler.network.utils.Host;

import org.apache.commons.lang.StringUtils;

import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ExecutorDispatcher implements InitializingBean {

    @Autowired
    private NettyExecutorManager nettyExecutorManager;

    /** Round-robin host manager */
    @Autowired
    private HostManager hostManager;
    private final ConcurrentHashMap<ExecutorType, ExecutorManager<Boolean>> executorManagers;
    public ExecutorDispatcher(){
        this.executorManagers = new ConcurrentHashMap<>();
    }

    public Boolean dispatch(final ExecutionContext context) throws ExecuteException {
        // get executor manager
        ExecutorManager<Boolean> executorManager = this.executorManagers.get(context.getExecutorType());
        if(executorManager == null){
            throw new ExecuteException("no ExecutorManager for type : " + context.getExecutorType());
        }

        // host select
        Host host = hostManager.select(context);
        if (StringUtils.isEmpty(host.getAddress())) {
            throw new ExecuteException(String.format("fail to execute : %s due to no suitable worker, "
                            + "current task needs worker group %s to execute",
                    context.getCommand(),context.getWorkerGroup()));
        }
        context.setHost(host);
        executorManager.beforeExecute(context);
        try {
            // task execute
            return executorManager.execute(context);
        } finally {
            executorManager.afterExecute(context);
        }
    }

    /**
     * Register executor managers.
     * @throws Exception if error throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        register(ExecutorType.WORKER, nettyExecutorManager);
        register(ExecutorType.CLIENT, nettyExecutorManager);
    }

    public void register(ExecutorType type, ExecutorManager executorManager){
        executorManagers.put(type, executorManager);
    }
}
