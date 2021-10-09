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
import org.apache.dolphinscheduler.network.NettyRpcServer;
import org.apache.dolphinscheduler.network.config.NettyServerConfig;
import org.apache.dolphinscheduler.utils.ExecutionContextTestUtils;
import org.apache.dolphinscheduler.worker.common.WorkerConfig;
import org.apache.dolphinscheduler.worker.processor.TaskExecuteProcessor;
import org.apache.dolphinscheduler.worker.common.WorkerRegistryClient;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * executor dispatch test
 */
@RunWith(SpringJUnit4ClassRunner.class)
@Ignore
public class ExecutorDispatcherTest {

    @Autowired
    private ExecutorDispatcher executorDispatcher;

    @Autowired
    private WorkerRegistryClient workerRegistryClient;

    @Autowired
    private WorkerConfig workerConfig;

    @Test(expected = ExecuteException.class)
    public void testDispatchWithException() throws ExecuteException {
        ExecutionContext executionContext = ExecutionContextTestUtils.getExecutionContext(10000);
        executorDispatcher.dispatch(executionContext);
    }

    @Test
    public void testDispatch() throws ExecuteException {
        int port = 30000;
        final NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(port);
        NettyRpcServer nettyRpcServer = new NettyRpcServer(serverConfig);
        nettyRpcServer.registerProcessor(org.apache.dolphinscheduler.network.command.CommandType.TASK_EXECUTE_REQUEST, Mockito.mock(TaskExecuteProcessor.class));
        nettyRpcServer.start();
        //
        workerConfig.setListenPort(port);
        workerRegistryClient.registry();

        ExecutionContext executionContext = ExecutionContextTestUtils.getExecutionContext(port);
        executorDispatcher.dispatch(executionContext);

        workerRegistryClient.unRegistry();
    }
}
