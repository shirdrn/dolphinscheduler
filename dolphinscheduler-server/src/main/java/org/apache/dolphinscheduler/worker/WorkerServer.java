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

package org.apache.dolphinscheduler.worker;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.IStoppable;
import org.apache.dolphinscheduler.common.enums.NodeType;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.network.NettyRpcServer;
import org.apache.dolphinscheduler.network.command.CommandType;
import org.apache.dolphinscheduler.network.config.NettyServerConfig;
import org.apache.dolphinscheduler.worker.common.WorkerConfig;
import org.apache.dolphinscheduler.worker.common.TaskPluginManager;
import org.apache.dolphinscheduler.worker.processor.DBTaskAckProcessor;
import org.apache.dolphinscheduler.worker.processor.DBTaskResponseProcessor;
import org.apache.dolphinscheduler.worker.processor.TaskExecuteProcessor;
import org.apache.dolphinscheduler.worker.processor.TaskKillProcessor;
import org.apache.dolphinscheduler.worker.common.WorkerRegistryClient;
import org.apache.dolphinscheduler.worker.runner.RetryReportTaskStatusThread;
import org.apache.dolphinscheduler.worker.runner.WorkerManagerThread;
import org.apache.dolphinscheduler.service.alert.AlertClientService;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.spi.exception.PluginNotFoundException;
import org.apache.dolphinscheduler.spi.plugin.DolphinPluginLoader;
import org.apache.dolphinscheduler.spi.plugin.DolphinPluginManagerConfig;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import org.apache.commons.collections4.MapUtils;

import java.util.Set;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.facebook.presto.jdbc.internal.guava.collect.ImmutableList;

@ComponentScan(value = "org.apache.dolphinscheduler", excludeFilters = {
        @ComponentScan.Filter(type = FilterType.REGEX, pattern = {
                "org.apache.dolphinscheduler.server.master.*",
                "org.apache.dolphinscheduler.server.monitor.*",
                "org.apache.dolphinscheduler.server.log.*"
        })
})
@EnableTransactionManagement
public class WorkerServer implements IStoppable {

    private static final Logger logger = LoggerFactory.getLogger(WorkerServer.class);
    private NettyRpcServer nettyRpcServer;
    @Autowired
    private WorkerRegistryClient workerRegistryClient;
    @Autowired
    private WorkerConfig workerConfig;
    @Autowired
    private SpringApplicationContext springApplicationContext;
    private AlertClientService alertClientService;
    @Autowired
    private RetryReportTaskStatusThread retryReportTaskStatusThread;
    @Autowired
    private WorkerManagerThread workerManagerThread;
    private TaskPluginManager taskPluginManager;

    @PostConstruct
    public void run() {
        // alert-server client registry
        alertClientService = new AlertClientService(workerConfig.getAlertListenHost(), Constants.ALERT_RPC_PORT);

        // init task plugin
        initTaskPlugin();
        // init remoting server
        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(workerConfig.getListenPort());
        this.nettyRpcServer = new NettyRpcServer(serverConfig);
        this.nettyRpcServer.registerProcessor(CommandType.TASK_EXECUTE_REQUEST, new TaskExecuteProcessor(alertClientService, taskPluginManager));
        this.nettyRpcServer.registerProcessor(CommandType.TASK_KILL_REQUEST, new TaskKillProcessor());
        this.nettyRpcServer.registerProcessor(CommandType.DB_TASK_ACK, new DBTaskAckProcessor());
        this.nettyRpcServer.registerProcessor(CommandType.DB_TASK_RESPONSE, new DBTaskResponseProcessor());
        this.nettyRpcServer.start();

        // worker registry
        try {
            this.workerRegistryClient.registry();
            this.workerRegistryClient.setRegistryStoppable(this);
            Set<String> workerZkPaths = this.workerRegistryClient.getWorkerZkPaths();

            this.workerRegistryClient.handleDeadServer(workerZkPaths, NodeType.WORKER, Constants.DELETE_OP);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        // task execute manager
        this.workerManagerThread.start();

        // retry report task status
        this.retryReportTaskStatusThread.start();

        /**
         * registry hooks, which are called before the process exits
         */
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (Stopper.isRunning()) {
                close("shutdownHook");
            }
        }));
    }

    // todo better
    private void initTaskPlugin() {
        taskPluginManager = new TaskPluginManager();
        DolphinPluginManagerConfig taskPluginManagerConfig = new DolphinPluginManagerConfig();
        taskPluginManagerConfig.setPlugins(workerConfig.getTaskPluginBinding());
        if (StringUtils.isNotBlank(workerConfig.getTaskPluginDir())) {
            taskPluginManagerConfig.setInstalledPluginsDir(workerConfig.getTaskPluginDir().trim());
        }

        if (StringUtils.isNotBlank(workerConfig.getMavenLocalRepository())) {
            taskPluginManagerConfig.setMavenLocalRepository(workerConfig.getMavenLocalRepository().trim());
        }

        DolphinPluginLoader taskPluginLoader = new DolphinPluginLoader(taskPluginManagerConfig, ImmutableList.of(taskPluginManager));
        try {
            taskPluginLoader.loadPlugins();
        } catch (Exception e) {
            throw new RuntimeException("Load Task Plugin Failed !", e);
        }
        if (MapUtils.isEmpty(taskPluginManager.getTaskChannelMap())) {
            throw new PluginNotFoundException("Task Plugin Not Found,Please Check Config File");
        }
    }

    public void close(String cause) {
        try {
            // execute only once
            if (Stopper.isStopped()) {
                return;
            }

            logger.info("worker server is stopping ..., cause : {}", cause);
            // set stop signal is true
            Stopper.stop();

            try {
                // thread sleep 3 seconds for thread quitely stop
                Thread.sleep(3000L);
            } catch (Exception e) {
                logger.warn("thread sleep exception", e);
            }

            // close
            this.nettyRpcServer.close();
            this.workerRegistryClient.unRegistry();
            this.alertClientService.close();
            this.springApplicationContext.close();
        } catch (Exception e) {
            logger.error("worker server stop exception ", e);
        }
    }

    @Override
    public void stop(String cause) {
        close(cause);
    }

    public static void main(String[] args) {
        Thread.currentThread().setName(Constants.THREAD_NAME_WORKER_SERVER);
        new SpringApplicationBuilder(WorkerServer.class).web(WebApplicationType.NONE).run(args);
    }
}
