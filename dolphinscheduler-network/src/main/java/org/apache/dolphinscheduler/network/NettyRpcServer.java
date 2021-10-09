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

package org.apache.dolphinscheduler.network;

import org.apache.dolphinscheduler.network.codec.NettyDecoder;
import org.apache.dolphinscheduler.network.codec.NettyEncoder;
import org.apache.dolphinscheduler.network.command.CommandType;
import org.apache.dolphinscheduler.network.config.NettyServerConfig;
import org.apache.dolphinscheduler.network.exceptions.RemoteException;
import org.apache.dolphinscheduler.network.handler.NettyServerHandler;
import org.apache.dolphinscheduler.network.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.network.utils.Constants;
import org.apache.dolphinscheduler.network.utils.NettyUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

public class NettyRpcServer extends AbstractRpcService {

    private final Logger logger = LoggerFactory.getLogger(NettyRpcServer.class);
    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    private final NettyEncoder encoder = new NettyEncoder();
    private final ExecutorService defaultExecutor = Executors.newFixedThreadPool(Constants.CPUS);
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final NettyServerConfig serverConfig;
    private final NettyServerHandler serverHandler = new NettyServerHandler(this);
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private static final String NETTY_BIND_FAILURE_MSG = "NettyRemotingServer bind %s fail";

    public NettyRpcServer(final NettyServerConfig serverConfig) {
        this.serverConfig = serverConfig;
        if (NettyUtils.useEpoll()) {
            bossGroup = newEpollEventLoopGroup(1, "NettyServerBossThread_");
            workerGroup = newEpollEventLoopGroup(serverConfig.getWorkerThread(), "NettyServerWorkerThread_");
        } else {
            bossGroup = newEpollEventLoopGroup(1, "NettyServerBossThread_");
            workerGroup = newNioEventLoopGroup(serverConfig.getWorkerThread(), "NettyServerWorkerThread_");
        }
    }

    public void start() {
        if (isStarted.compareAndSet(false, true)) {
            serverBootstrap
                    .group(this.bossGroup, this.workerGroup)
                    .channel(NettyUtils.getServerSocketChannelClass())
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.SO_BACKLOG, serverConfig.getSoBacklog())
                    .childOption(ChannelOption.SO_KEEPALIVE, serverConfig.isSoKeepalive())
                    .childOption(ChannelOption.TCP_NODELAY, serverConfig.isTcpNoDelay())
                    .childOption(ChannelOption.SO_SNDBUF, serverConfig.getSendBufferSize())
                    .childOption(ChannelOption.SO_RCVBUF, serverConfig.getReceiveBufferSize())
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        protected void initChannel(SocketChannel ch) {
                            initNettyChannel(ch);
                        }
                    });

            ChannelFuture future;
            try {
                future = serverBootstrap.bind(serverConfig.getListenPort()).sync();
            } catch (Exception e) {
                logger.error("NettyServer bind fail {}, exit", e.getMessage(), e);
                throw new RemoteException(String.format(NETTY_BIND_FAILURE_MSG, serverConfig.getListenPort()));
            }
            if (future.isSuccess()) {
                logger.info("NettyServer bind success at port : {}", serverConfig.getListenPort());
            } else if (future.cause() != null) {
                throw new RemoteException(String.format(NETTY_BIND_FAILURE_MSG, serverConfig.getListenPort()), future.cause());
            } else {
                throw new RemoteException(String.format(NETTY_BIND_FAILURE_MSG, serverConfig.getListenPort()));
            }
        }
    }

    private void initNettyChannel(SocketChannel ch) {
        ch.pipeline()
                .addLast("encoder", encoder)
                .addLast("decoder", new NettyDecoder())
                .addLast("server-idle-handle", new IdleStateHandler(0, 0, Constants.NETTY_SERVER_HEART_BEAT_TIME, TimeUnit.MILLISECONDS))
                .addLast("handler", serverHandler);
    }

    public void registerProcessor(final CommandType commandType, final NettyRequestProcessor processor) {
        this.registerProcessor(commandType, processor, null);
    }

    public void registerProcessor(final CommandType commandType, final NettyRequestProcessor processor, final ExecutorService executor) {
        serverHandler.registerProcessor(commandType, processor, executor);
    }

    public ExecutorService getDefaultExecutor() {
        return defaultExecutor;
    }

    public void close() {
        if (isStarted.compareAndSet(true, false)) {
            try {
                if (bossGroup != null) {
                    bossGroup.shutdownGracefully();
                }
                if (workerGroup != null) {
                    workerGroup.shutdownGracefully();
                }
                defaultExecutor.shutdown();
            } catch (Exception ex) {
                logger.error("netty server close exception", ex);
            }
            logger.info("netty server closed");
        }
    }
}
