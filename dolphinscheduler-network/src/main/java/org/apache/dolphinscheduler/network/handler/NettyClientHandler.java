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

package org.apache.dolphinscheduler.network.handler;

import org.apache.dolphinscheduler.network.NettyRpcClient;
import org.apache.dolphinscheduler.network.command.Command;
import org.apache.dolphinscheduler.network.command.CommandType;
import org.apache.dolphinscheduler.network.future.ResponseFuture;
import org.apache.dolphinscheduler.network.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.network.utils.ChannelUtils;
import org.apache.dolphinscheduler.network.utils.Constants;
import org.apache.dolphinscheduler.network.utils.Pair;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;

@ChannelHandler.Sharable
public class NettyClientHandler extends ChannelInboundHandlerAdapter {

    private final Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);
    private final NettyRpcClient nettyRpcClient;
    private static byte[] heartBeatData = "heart_beat".getBytes();
    private final ExecutorService callbackExecutor;
    private final ConcurrentHashMap<CommandType, Pair<NettyRequestProcessor, ExecutorService>> processors;
    private final ExecutorService defaultExecutor = Executors.newFixedThreadPool(Constants.CPUS);

    public NettyClientHandler(NettyRpcClient nettyRpcClient, ExecutorService callbackExecutor) {
        this.nettyRpcClient = nettyRpcClient;
        this.callbackExecutor = callbackExecutor;
        this.processors = new ConcurrentHashMap<>();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        nettyRpcClient.closeChannel(ChannelUtils.toAddress(ctx.channel()));
        ctx.channel().close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        processReceived(ctx.channel(), (Command) msg);
    }

    public void registerProcessor(final CommandType commandType, final NettyRequestProcessor processor) {
        this.registerProcessor(commandType, processor, null);
    }

    public void registerProcessor(final CommandType commandType, final NettyRequestProcessor processor, final ExecutorService executor) {
        ExecutorService executorRef = executor;
        if (executorRef == null) {
            executorRef = defaultExecutor;
        }
        this.processors.putIfAbsent(commandType, new Pair<>(processor, executorRef));
    }

    private void processReceived(final Channel channel, final Command command) {
        ResponseFuture future = ResponseFuture.getFuture(command.getOpaque());
        if (future != null) {
            future.setResponseCommand(command);
            future.release();
            if (future.getInvokeCallback() != null) {
                this.callbackExecutor.submit(future::executeInvokeCallback);
            } else {
                future.putResponse(command);
            }
        } else {
            processByCommandType(channel, command);
        }
    }

    public void processByCommandType(final Channel channel, final Command command) {
        final Pair<NettyRequestProcessor, ExecutorService> pair = processors.get(command.getType());
        if (pair != null) {
            Runnable run = () -> {
                try {
                    pair.getLeft().process(channel, command);
                } catch (Exception e) {
                    logger.error(String.format("process command %s exception", command), e);
                }
            };
            try {
                pair.getRight().submit(run);
            } catch (RejectedExecutionException e) {
                logger.warn("thread pool is full, discard command {} from {}", command, ChannelUtils.getRemoteAddress(channel));
            }
        } else {
            logger.warn("receive response {}, but not matched any request ", command);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("exceptionCaught : {}", cause.getMessage(), cause);
        nettyRpcClient.closeChannel(ChannelUtils.toAddress(ctx.channel()));
        ctx.channel().close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            Command heartBeat = new Command();
            heartBeat.setType(CommandType.HEART_BEAT);
            heartBeat.setBody(heartBeatData);
            ctx.channel().writeAndFlush(heartBeat)
                    .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
            if (logger.isDebugEnabled()) {
                logger.debug("Client send heart beat to: {}", ChannelUtils.getRemoteAddress(ctx.channel()));
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

}
