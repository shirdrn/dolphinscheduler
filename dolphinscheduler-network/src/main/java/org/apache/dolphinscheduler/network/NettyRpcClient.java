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
import org.apache.dolphinscheduler.network.command.Command;
import org.apache.dolphinscheduler.network.command.CommandType;
import org.apache.dolphinscheduler.network.config.NettyClientConfig;
import org.apache.dolphinscheduler.network.exceptions.RemotingException;
import org.apache.dolphinscheduler.network.exceptions.RemotingTimeoutException;
import org.apache.dolphinscheduler.network.exceptions.RemotingTooMuchRequestException;
import org.apache.dolphinscheduler.network.future.InvokeCallback;
import org.apache.dolphinscheduler.network.future.ReleaseSemaphore;
import org.apache.dolphinscheduler.network.future.ResponseFuture;
import org.apache.dolphinscheduler.network.handler.NettyClientHandler;
import org.apache.dolphinscheduler.network.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.network.utils.CallerThreadExecutePolicy;
import org.apache.dolphinscheduler.network.utils.Constants;
import org.apache.dolphinscheduler.network.utils.Host;
import org.apache.dolphinscheduler.network.utils.NamedThreadFactory;
import org.apache.dolphinscheduler.network.utils.NettyUtils;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

public class NettyRpcClient extends AbstractRpcService {

    private final Logger logger = LoggerFactory.getLogger(NettyRpcClient.class);
    private final Bootstrap bootstrap = new Bootstrap();
    private final NettyEncoder encoder = new NettyEncoder();
    private final ConcurrentHashMap<Host, Channel> channels = new ConcurrentHashMap<>(128);
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final EventLoopGroup workerGroup;
    private final NettyClientConfig clientConfig;
    private final Semaphore asyncSemaphore = new Semaphore(200, true);
    private final ExecutorService callbackExecutor;
    private final NettyClientHandler clientHandler;
    private final ScheduledExecutorService responseFutureExecutor;

    public NettyRpcClient(final NettyClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        if (NettyUtils.useEpoll()) {
            workerGroup = newEpollEventLoopGroup(clientConfig.getWorkerThreads(), "NettyClient_");
        } else {
            workerGroup = newEpollEventLoopGroup(clientConfig.getWorkerThreads(), "NettyClient_");
        }

        callbackExecutor = new ThreadPoolExecutor(5, 10, 1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(1000), new NamedThreadFactory("CallbackExecutor", 10),
                new CallerThreadExecutePolicy());
        clientHandler = new NettyClientHandler(this, callbackExecutor);
        responseFutureExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("ResponseFutureExecutor"));
        start();
    }

    private void start() {
        bootstrap.group(workerGroup)
                .channel(NettyUtils.getSocketChannelClass())
                .option(ChannelOption.SO_KEEPALIVE, clientConfig.isSoKeepalive())
                .option(ChannelOption.TCP_NODELAY, clientConfig.isTcpNoDelay())
                .option(ChannelOption.SO_SNDBUF, clientConfig.getSendBufferSize())
                .option(ChannelOption.SO_RCVBUF, clientConfig.getReceiveBufferSize())
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, clientConfig.getConnectTimeoutMillis())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast("client-idle-handler", new IdleStateHandler(Constants.NETTY_CLIENT_HEART_BEAT_TIME, 0, 0, TimeUnit.MILLISECONDS))
                                .addLast(new NettyDecoder(), clientHandler, encoder);
                    }
                });
        responseFutureExecutor.scheduleAtFixedRate(ResponseFuture::scanFutureTable, 5000, 1000, TimeUnit.MILLISECONDS);
        isStarted.compareAndSet(false, true);
    }

    public void sendAsync(final Host host, final Command command,
                          final long timeoutMillis,
                          final InvokeCallback invokeCallback) throws InterruptedException, RemotingException {
        final Channel channel = getChannel(host);
        if (channel == null) {
            throw new RemotingException("network error");
        }
        // request unique identification
        final long opaque = command.getOpaque();
        // control concurrency number
        boolean acquired = asyncSemaphore.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final ReleaseSemaphore releaseSemaphore = new ReleaseSemaphore(asyncSemaphore);
            final ResponseFuture responseFuture = new ResponseFuture(opaque,
                    timeoutMillis,
                    invokeCallback,
                    releaseSemaphore);
            try {
                channel.writeAndFlush(command).addListener(future -> {
                    if (future.isSuccess()) {
                        responseFuture.setSendOk(true);
                        return;
                    } else {
                        responseFuture.setSendOk(false);
                    }
                    responseFuture.setCause(future.cause());
                    responseFuture.putResponse(null);
                    try {
                        responseFuture.executeInvokeCallback();
                    } catch (Exception ex) {
                        logger.error("execute callback error", ex);
                    } finally {
                        responseFuture.release();
                    }
                });
            } catch (Exception ex) {
                responseFuture.release();
                throw new RemotingException(String.format("send command to host: %s failed", host), ex);
            }
        } else {
            String message = String.format("try to acquire async semaphore timeout: %d, waiting thread num: %d, total permits: %d",
                    timeoutMillis, asyncSemaphore.getQueueLength(), asyncSemaphore.availablePermits());
            throw new RemotingTooMuchRequestException(message);
        }
    }

    public Command sendSync(final Host host, final Command command, final long timeoutMillis) throws InterruptedException, RemotingException {
        final Channel channel = getChannel(host);
        if (channel == null) {
            throw new RemotingException(String.format("connect to : %s fail", host));
        }
        final long opaque = command.getOpaque();
        final ResponseFuture responseFuture = new ResponseFuture(opaque, timeoutMillis, null, null);
        channel.writeAndFlush(command).addListener(future -> {
            if (future.isSuccess()) {
                responseFuture.setSendOk(true);
                return;
            } else {
                responseFuture.setSendOk(false);
            }
            responseFuture.setCause(future.cause());
            responseFuture.putResponse(null);
            logger.error("send command {} to host {} failed", command, host);
        });
        /*
         * sync wait for result
         */
        Command result = responseFuture.waitResponse();
        if (result == null) {
            if (responseFuture.isSendOK()) {
                throw new RemotingTimeoutException(host.toString(), timeoutMillis, responseFuture.getCause());
            } else {
                throw new RemotingException(host.toString(), responseFuture.getCause());
            }
        }
        return result;
    }

    public void send(final Host host, final Command command) throws RemotingException {
        Channel channel = getChannel(host);
        if (channel == null) {
            throw new RemotingException(String.format("connect to : %s fail", host));
        }
        try {
            ChannelFuture future = channel.writeAndFlush(command).await();
            if (future.isSuccess()) {
                logger.debug("send command : {} , to : {} successfully.", command, host.getAddress());
            } else {
                String msg = String.format("send command : %s , to :%s failed", command, host.getAddress());
                logger.error(msg, future.cause());
                throw new RemotingException(msg);
            }
        } catch (Exception e) {
            logger.error("Send command {} to address {} encounter error.", command, host.getAddress());
            throw new RemotingException(String.format("Send command : %s , to :%s encounter error", command, host.getAddress()), e);
        }
    }

    public void registerProcessor(final CommandType commandType, final NettyRequestProcessor processor) {
        registerProcessor(commandType, processor, null);
    }

    public void registerProcessor(final CommandType commandType, final NettyRequestProcessor processor, final ExecutorService executor) {
        clientHandler.registerProcessor(commandType, processor, executor);
    }

    public Channel getChannel(Host host) {
        Channel channel = channels.get(host);
        if (channel != null && channel.isActive()) {
            return channel;
        }
        return createChannel(host, true);
    }

    public Channel createChannel(Host host, boolean isSync) {
        ChannelFuture future;
        try {
            synchronized (bootstrap) {
                future = bootstrap.connect(new InetSocketAddress(host.getIp(), host.getPort()));
            }
            if (isSync) {
                future.sync();
            }
            if (future.isSuccess()) {
                Channel channel = future.channel();
                channels.put(host, channel);
                return channel;
            }
        } catch (Exception ex) {
            logger.warn(String.format("connect to %s error", host), ex);
        }
        return null;
    }

    public void close() {
        if (isStarted.compareAndSet(true, false)) {
            try {
                closeChannels();
                if (workerGroup != null) {
                    workerGroup.shutdownGracefully();
                }
                if (callbackExecutor != null) {
                    callbackExecutor.shutdownNow();
                }
                if (responseFutureExecutor != null) {
                    responseFutureExecutor.shutdownNow();
                }
            } catch (Exception ex) {
                logger.error("netty client close exception", ex);
            }
            logger.info("netty client closed");
        }
    }

    private void closeChannels() {
        for (Channel channel : channels.values()) {
            channel.close();
        }
        channels.clear();
    }

    public void closeChannel(Host host) {
        Channel channel = channels.remove(host);
        if (channel != null) {
            channel.close();
        }
    }
}
