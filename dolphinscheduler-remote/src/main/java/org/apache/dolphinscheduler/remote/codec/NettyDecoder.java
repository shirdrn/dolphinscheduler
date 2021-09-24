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

package org.apache.dolphinscheduler.remote.codec;

import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandContext;
import org.apache.dolphinscheduler.remote.command.CommandHeader;
import org.apache.dolphinscheduler.remote.command.CommandType;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

public class NettyDecoder extends ReplayingDecoder<NettyDecoder.State> {

    private static final Logger logger = LoggerFactory.getLogger(NettyDecoder.class);
    private final CommandHeader commandHeader = new CommandHeader();

    public NettyDecoder() {
        super(State.MAGIC);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state()) {
            case MAGIC:
                checkMagic(in.readByte());
                checkpoint(State.VERSION);
                // fallthru
            case VERSION:
                checkVersion(in.readByte());
                checkpoint(State.COMMAND);
                // fallthru
            case COMMAND:
                commandHeader.setType(in.readByte());
                checkpoint(State.OPAQUE);
                // fallthru
            case OPAQUE:
                commandHeader.setOpaque(in.readLong());
                checkpoint(State.CONTEXT_LENGTH);
                // fallthru
            case CONTEXT_LENGTH:
                commandHeader.setContextLength(in.readInt());
                checkpoint(State.CONTEXT);
                // fallthru
            case CONTEXT:
                byte[] context = new byte[commandHeader.getContextLength()];
                in.readBytes(context);
                commandHeader.setContext(context);
                checkpoint(State.BODY_LENGTH);
                // fallthru
            case BODY_LENGTH:
                commandHeader.setBodyLength(in.readInt());
                checkpoint(State.BODY);
                // fallthru
            case BODY:
                byte[] body = new byte[commandHeader.getBodyLength()];
                in.readBytes(body);
                //
                Command packet = new Command();
                packet.setType(commandType(commandHeader.getType()));
                packet.setOpaque(commandHeader.getOpaque());
                packet.setContext(CommandContext.valueOf(commandHeader.getContext()));
                packet.setBody(body);
                out.add(packet);
                //
                checkpoint(State.MAGIC);
                break;
            default:
                logger.warn("unknown decoder state {}", state());
        }
    }

    private CommandType commandType(byte type) {
        for (CommandType ct : CommandType.values()) {
            if (ct.ordinal() == type) {
                return ct;
            }
        }
        return null;
    }

    private void checkMagic(byte magic) {
        if (magic != Command.MAGIC) {
            throw new IllegalArgumentException("illegal packet [magic]" + magic);
        }
    }

    private void checkVersion(byte version) {
        if (version != Command.VERSION) {
            throw new IllegalArgumentException("illegal protocol [version]" + version);
        }
    }

    enum State {
        MAGIC,
        VERSION,
        COMMAND,
        OPAQUE,
        CONTEXT_LENGTH,
        CONTEXT,
        BODY_LENGTH,
        BODY;
    }
}
