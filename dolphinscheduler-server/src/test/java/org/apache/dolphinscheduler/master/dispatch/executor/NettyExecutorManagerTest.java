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

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.utils.NetUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.master.common.ExecuteException;
import org.apache.dolphinscheduler.master.common.ExecutionContext;
import org.apache.dolphinscheduler.master.dispatch.ExecutorType;
import org.apache.dolphinscheduler.network.NettyRpcServer;
import org.apache.dolphinscheduler.network.config.NettyServerConfig;
import org.apache.dolphinscheduler.network.utils.Host;
import org.apache.dolphinscheduler.builder.TaskExecutionContextBuilder;
import org.apache.dolphinscheduler.entity.TaskExecutionContext;
import org.apache.dolphinscheduler.worker.processor.TaskExecuteProcessor;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * netty executor manager test
 */
@RunWith(SpringJUnit4ClassRunner.class)
@Ignore
public class NettyExecutorManagerTest {

    @Autowired
    private NettyExecutorManager nettyExecutorManager;

    @Test
    public void testExecute() throws ExecuteException {
        final NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(30000);
        NettyRpcServer nettyRpcServer = new NettyRpcServer(serverConfig);
        nettyRpcServer.registerProcessor(org.apache.dolphinscheduler.network.command.CommandType.TASK_EXECUTE_REQUEST, new TaskExecuteProcessor());
        nettyRpcServer.start();
        TaskInstance taskInstance = Mockito.mock(TaskInstance.class);
        ProcessDefinition processDefinition = Mockito.mock(ProcessDefinition.class);
        ProcessInstance processInstance = new ProcessInstance();
        processInstance.setCommandType(CommandType.COMPLEMENT_DATA);
        taskInstance.setProcessInstance(processInstance);
        TaskExecutionContext context = TaskExecutionContextBuilder.get()
                .buildTaskInstanceRelatedInfo(taskInstance)
                .buildProcessInstanceRelatedInfo(processInstance)
                .buildProcessDefinitionRelatedInfo(processDefinition)
                .create();
        ExecutionContext executionContext = new ExecutionContext(context.toCommand(), ExecutorType.WORKER);
        executionContext.setHost(Host.of(NetUtils.getAddr(serverConfig.getListenPort())));
        Boolean execute = nettyExecutorManager.execute(executionContext);
        Assert.assertTrue(execute);
        nettyRpcServer.close();
    }

    @Test(expected = ExecuteException.class)
    public void testExecuteWithException() throws ExecuteException {
        TaskInstance taskInstance = Mockito.mock(TaskInstance.class);
        ProcessDefinition processDefinition = Mockito.mock(ProcessDefinition.class);
        ProcessInstance processInstance = new ProcessInstance();
        processInstance.setCommandType(CommandType.COMPLEMENT_DATA);
        taskInstance.setProcessInstance(processInstance);
        TaskExecutionContext context = TaskExecutionContextBuilder.get()
                .buildTaskInstanceRelatedInfo(taskInstance)
                .buildProcessInstanceRelatedInfo(processInstance)
                .buildProcessDefinitionRelatedInfo(processDefinition)
                .create();
        ExecutionContext executionContext = new ExecutionContext(context.toCommand(), ExecutorType.WORKER);
        executionContext.setHost(Host.of(NetUtils.getAddr(4444)));
        nettyExecutorManager.execute(executionContext);

    }
}
