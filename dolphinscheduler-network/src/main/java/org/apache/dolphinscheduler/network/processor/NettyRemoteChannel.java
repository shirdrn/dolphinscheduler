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

package org.apache.dolphinscheduler.network.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.dolphinscheduler.network.command.Command;
import org.apache.dolphinscheduler.network.utils.ChannelUtils;
import org.apache.dolphinscheduler.network.utils.Host;

public class NettyRemoteChannel {

    private final Channel channel;
    private final long opaque;
    private final Host masterHost;


    public NettyRemoteChannel(Channel channel, long opaque) {
        this.channel = channel;
        this.masterHost = ChannelUtils.toAddress(channel);
        this.opaque = opaque;
    }

    public NettyRemoteChannel(Channel channel) {
        this.channel = channel;
        this.masterHost = ChannelUtils.toAddress(channel);
        this.opaque = -1;
    }

    public Channel getChannel() {
        return channel;
    }

    public long getOpaque() {
        return opaque;
    }

    public Host getMasterHost() {
        return masterHost;
    }

    public boolean isActive(){
        return this.channel.isActive();
    }

    public ChannelFuture writeAndFlush(Command command){
        return this.channel.writeAndFlush(command);
    }

    public void close(){
        this.channel.close();
    }
}
