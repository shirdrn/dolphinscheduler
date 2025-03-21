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

package org.apache.dolphinscheduler.master;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.IStoppable;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.master.common.MasterConfig;
import org.apache.dolphinscheduler.master.processor.StateEventProcessor;
import org.apache.dolphinscheduler.master.processor.TaskAckProcessor;
import org.apache.dolphinscheduler.master.processor.TaskKillResponseProcessor;
import org.apache.dolphinscheduler.master.processor.TaskResponseProcessor;
import org.apache.dolphinscheduler.master.registry.MasterRegistryClient;
import org.apache.dolphinscheduler.master.runner.EventExecuteService;
import org.apache.dolphinscheduler.master.runner.MasterSchedulerService;
import org.apache.dolphinscheduler.master.runner.WorkflowExecuteThread;
import org.apache.dolphinscheduler.network.NettyRpcServer;
import org.apache.dolphinscheduler.network.command.CommandType;
import org.apache.dolphinscheduler.network.config.NettyServerConfig;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.quartz.QuartzExecutors;

import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@ComponentScan(value = "org.apache.dolphinscheduler", excludeFilters = {
        @ComponentScan.Filter(type = FilterType.REGEX, pattern = {
                "org.apache.dolphinscheduler.worker.*",
                "org.apache.dolphinscheduler.monitor.*",
                "org.apache.dolphinscheduler.log.*"
        })
})
@EnableTransactionManagement
public class MasterServer implements IStoppable {

    private static final Logger logger = LoggerFactory.getLogger(MasterServer.class);
    @Autowired
    private MasterConfig masterConfig;
    @Autowired
    private SpringApplicationContext springApplicationContext;
    private NettyRpcServer nettyRpcServer;

    /**
     * zk master client
     */
    @Autowired
    private MasterRegistryClient masterRegistryClient;

    @Autowired
    private MasterSchedulerService masterSchedulerService;
    @Autowired
    private EventExecuteService eventExecuteService;
    private ConcurrentHashMap<Integer, WorkflowExecuteThread> processInstanceExecMaps = new ConcurrentHashMap<>();

    @PostConstruct
    public void run() {
        // initialize & start remoting server
        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(masterConfig.getListenPort());
        nettyRpcServer = new NettyRpcServer(serverConfig);
        TaskAckProcessor ackProcessor = new TaskAckProcessor();
        ackProcessor.init(processInstanceExecMaps);
        TaskResponseProcessor taskResponseProcessor = new TaskResponseProcessor();
        taskResponseProcessor.init(processInstanceExecMaps);
        StateEventProcessor stateEventProcessor = new StateEventProcessor();
        stateEventProcessor.init(processInstanceExecMaps);
        nettyRpcServer.registerProcessor(CommandType.TASK_EXECUTE_RESPONSE, taskResponseProcessor);
        nettyRpcServer.registerProcessor(CommandType.TASK_EXECUTE_ACK, ackProcessor);
        nettyRpcServer.registerProcessor(CommandType.TASK_KILL_RESPONSE, new TaskKillResponseProcessor());
        nettyRpcServer.registerProcessor(CommandType.STATE_EVENT_REQUEST, stateEventProcessor);
        nettyRpcServer.start();

        // self tolerant
        masterRegistryClient.init(this.processInstanceExecMaps);
        masterRegistryClient.start();
        masterRegistryClient.setRegistryStoppable(this);

        eventExecuteService.init(this.processInstanceExecMaps);
        eventExecuteService.start();
        // start scheduler
        masterSchedulerService.init(this.processInstanceExecMaps);

        masterSchedulerService.start();

        // start QuartzExecutors
        // what system should do if exception
        try {
            logger.info("start Quartz server...");
            QuartzExecutors.getInstance().start();
        } catch (Exception e) {
            try {
                QuartzExecutors.getInstance().shutdown();
            } catch (SchedulerException e1) {
                logger.error("QuartzExecutors shutdown failed : " + e1.getMessage(), e1);
            }
            logger.error("start Quartz failed", e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (Stopper.isRunning()) {
                close("shutdownHook");
            }
        }));

    }

    public void close(String cause) {
        try {
            // execute only once
            if (Stopper.isStopped()) {
                return;
            }

            logger.info("master server is stopping ..., cause : {}", cause);
            // set stop signal is true
            Stopper.stop();

            try {
                // thread sleep 3 seconds for thread quietly stop
                Thread.sleep(3000L);
            } catch (Exception e) {
                logger.warn("thread sleep exception ", e);
            }
            // close
            this.masterSchedulerService.close();
            nettyRpcServer.close();
            masterRegistryClient.closeRegistry();
            // close quartz
            try {
                QuartzExecutors.getInstance().shutdown();
                logger.info("Quartz service stopped");
            } catch (Exception e) {
                logger.warn("Quartz service stopped exception:{}", e.getMessage());
            }
            // close spring Context and will invoke method with @PreDestroy annotation to destory beans. like ServerNodeManager,HostManager,TaskResponseService,CuratorZookeeperClient,etc
            springApplicationContext.close();
        } catch (Exception e) {
            logger.error("master server stop exception ", e);
        }
    }

    @Override
    public void stop(String cause) {
        close(cause);
    }

    public static void main(String[] args) {
        Thread.currentThread().setName(Constants.THREAD_NAME_MASTER_SERVER);
        new SpringApplicationBuilder(MasterServer.class).web(WebApplicationType.NONE).run(args);
    }
}
