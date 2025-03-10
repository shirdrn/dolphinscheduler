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

package org.apache.dolphinscheduler.master.processor.queue;

import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.enums.StateEvent;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.network.command.StateEventResponseCommand;
import org.apache.dolphinscheduler.master.runner.WorkflowExecuteThread;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.netty.channel.Channel;

@Component
public class StateEventResponseService {

    private final Logger logger = LoggerFactory.getLogger(StateEventResponseService.class);
    private final BlockingQueue<StateEvent> eventQueue = new LinkedBlockingQueue<>(5000);
    private Thread responseWorker;
    private ConcurrentHashMap<Integer, WorkflowExecuteThread> processInstanceMapper;

    public void init(ConcurrentHashMap<Integer, WorkflowExecuteThread> processInstanceMapper) {
        if (this.processInstanceMapper == null) {
            this.processInstanceMapper = processInstanceMapper;
        }
    }

    @PostConstruct
    public void start() {
        this.responseWorker = new StateEventResponseWorker();
        this.responseWorker.setName("StateEventResponseWorker");
        this.responseWorker.start();
    }

    @PreDestroy
    public void stop() {
        this.responseWorker.interrupt();
        if (!eventQueue.isEmpty()) {
            List<StateEvent> remainEvents = new ArrayList<>(eventQueue.size());
            eventQueue.drainTo(remainEvents);
            for (StateEvent event : remainEvents) {
                this.persist(event);
            }
        }
    }

    public void addResponse(StateEvent stateEvent) {
        try {
            eventQueue.put(stateEvent);
        } catch (InterruptedException e) {
            logger.error("put state event : {} error :{}", stateEvent, e);
            Thread.currentThread().interrupt();
        }
    }

    class StateEventResponseWorker extends Thread {

        @Override
        public void run() {

            while (Stopper.isRunning()) {
                try {
                    // if not task , blocking here
                    StateEvent stateEvent = eventQueue.take();
                    persist(stateEvent);
                } catch (InterruptedException e) {
                    logger.warn("persist task error", e);
                    Thread.currentThread().interrupt();
                }
            }
            logger.info("StateEventResponseWorker stopped");
        }
    }

    private void writeResponse(StateEvent stateEvent, ExecutionStatus status) {
        Channel channel = stateEvent.getChannel();
        if (channel != null) {
            StateEventResponseCommand command = new StateEventResponseCommand(status.getCode(), stateEvent.getKey());
            channel.writeAndFlush(command.convert2Command());
        }
    }

    private void persist(StateEvent stateEvent) {
        try {
            if (!this.processInstanceMapper.containsKey(stateEvent.getProcessInstanceId())) {
                writeResponse(stateEvent, ExecutionStatus.FAILURE);
                return;
            }

            WorkflowExecuteThread workflowExecuteThread = this.processInstanceMapper.get(stateEvent.getProcessInstanceId());
            workflowExecuteThread.addStateEvent(stateEvent);
            writeResponse(stateEvent, ExecutionStatus.SUCCESS);
        } catch (Exception e) {
            logger.error("persist event queue error:", stateEvent.toString(), e);
        }
    }

    public BlockingQueue<StateEvent> getEventQueue() {
        return eventQueue;
    }
}
