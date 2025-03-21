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

package org.apache.dolphinscheduler.log;


import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.network.NettyRpcServer;
import org.apache.dolphinscheduler.network.command.CommandType;
import org.apache.dolphinscheduler.network.config.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerServer {

    private static  final Logger logger = LoggerFactory.getLogger(LoggerServer.class);
    private final NettyRpcServer server;
    private final NettyServerConfig serverConfig;
    private final LoggerRequestProcessor requestProcessor;

    public LoggerServer(){
        this.serverConfig = new NettyServerConfig();
        this.serverConfig.setListenPort(Constants.RPC_PORT);
        this.server = new NettyRpcServer(serverConfig);
        this.requestProcessor = new LoggerRequestProcessor();
        this.server.registerProcessor(CommandType.GET_LOG_BYTES_REQUEST, requestProcessor, requestProcessor.getExecutor());
        this.server.registerProcessor(CommandType.ROLL_VIEW_LOG_REQUEST, requestProcessor, requestProcessor.getExecutor());
        this.server.registerProcessor(CommandType.VIEW_WHOLE_LOG_REQUEST, requestProcessor, requestProcessor.getExecutor());
        this.server.registerProcessor(CommandType.REMOVE_TAK_LOG_REQUEST, requestProcessor, requestProcessor.getExecutor());
    }

    public void start()  {
        this.server.start();
        logger.info("logger server started, listening on port : {}" , Constants.RPC_PORT);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LoggerServer.this.stop();
            }
        });
    }

    public void stop() {
        this.server.close();
        logger.info("logger server shut down");
    }

    public static void main(String[] args)  {
        final LoggerServer server = new LoggerServer();
        server.start();
    }

}
