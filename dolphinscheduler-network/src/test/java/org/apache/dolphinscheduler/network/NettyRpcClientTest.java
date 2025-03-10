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

import org.apache.dolphinscheduler.network.command.Command;
import org.apache.dolphinscheduler.network.command.CommandType;
import org.apache.dolphinscheduler.network.command.Ping;
import org.apache.dolphinscheduler.network.command.Pong;
import org.apache.dolphinscheduler.network.config.NettyClientConfig;
import org.apache.dolphinscheduler.network.config.NettyServerConfig;
import org.apache.dolphinscheduler.network.future.InvokeCallback;
import org.apache.dolphinscheduler.network.future.ResponseFuture;
import org.apache.dolphinscheduler.network.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.network.utils.Host;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import io.netty.channel.Channel;

/**
 *  netty remote client test
 */
public class NettyRpcClientTest {

    /**
     *  test send sync
     */
    @Test
    public void testSendSync() {
        NettyServerConfig serverConfig = new NettyServerConfig();

        NettyRpcServer server = new NettyRpcServer(serverConfig);
        server.registerProcessor(CommandType.PING, new NettyRequestProcessor() {
            @Override
            public void process(Channel channel, Command command) {
                channel.writeAndFlush(Pong.create(command.getOpaque()));
            }
        });


        server.start();
        //
        final NettyClientConfig clientConfig = new NettyClientConfig();
        NettyRpcClient client = new NettyRpcClient(clientConfig);
        Command commandPing = Ping.create();
        try {
            Command response = client.sendSync(new Host("127.0.0.1", serverConfig.getListenPort()), commandPing, 2000);
            Assert.assertEquals(commandPing.getOpaque(), response.getOpaque());
        } catch (Exception e) {
            e.printStackTrace();
        }
        server.close();
        client.close();
    }

    /**
     *  test sned async
     */
    @Test
    public void testSendAsync(){
        NettyServerConfig serverConfig = new NettyServerConfig();

        NettyRpcServer server = new NettyRpcServer(serverConfig);
        server.registerProcessor(CommandType.PING, new NettyRequestProcessor() {
            @Override
            public void process(Channel channel, Command command) {
                channel.writeAndFlush(Pong.create(command.getOpaque()));
            }
        });
        server.start();
        //
        final NettyClientConfig clientConfig = new NettyClientConfig();
        NettyRpcClient client = new NettyRpcClient(clientConfig);
        CountDownLatch latch = new CountDownLatch(1);
        Command commandPing = Ping.create();
        try {
            final AtomicLong opaque = new AtomicLong(0);
            client.sendAsync(new Host("127.0.0.1", serverConfig.getListenPort()), commandPing, 2000, new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {
                    opaque.set(responseFuture.getOpaque());
                    latch.countDown();
                }
            });
            latch.await();
            Assert.assertEquals(commandPing.getOpaque(), opaque.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
        server.close();
        client.close();
    }
}
