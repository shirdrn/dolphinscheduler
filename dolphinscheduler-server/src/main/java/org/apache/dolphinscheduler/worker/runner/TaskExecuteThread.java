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

package org.apache.dolphinscheduler.worker.runner;

import static java.util.Calendar.DAY_OF_MONTH;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.Event;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.enums.TaskType;
import org.apache.dolphinscheduler.common.process.Property;
import org.apache.dolphinscheduler.common.utils.CommonUtils;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.HadoopUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.common.utils.RetryerUtils;
import org.apache.dolphinscheduler.network.command.Command;
import org.apache.dolphinscheduler.network.command.TaskExecuteAckCommand;
import org.apache.dolphinscheduler.network.command.TaskExecuteResponseCommand;
import org.apache.dolphinscheduler.entity.TaskExecutionContext;
import org.apache.dolphinscheduler.worker.common.ResponceCache;
import org.apache.dolphinscheduler.worker.common.TaskPluginManager;
import org.apache.dolphinscheduler.worker.processor.TaskCallbackService;
import org.apache.dolphinscheduler.service.alert.AlertClientService;
import org.apache.dolphinscheduler.spi.exception.PluginNotFoundException;
import org.apache.dolphinscheduler.spi.task.AbstractTask;
import org.apache.dolphinscheduler.spi.task.TaskAlertInfo;
import org.apache.dolphinscheduler.spi.task.TaskChannel;
import org.apache.dolphinscheduler.spi.task.TaskExecutionContextCacheManager;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rholder.retry.RetryException;

public class TaskExecuteThread implements Runnable, Delayed {

    private final Logger logger = LoggerFactory.getLogger(TaskExecuteThread.class);
    private TaskExecutionContext taskExecutionContext;
    private AbstractTask task;
    private TaskCallbackService taskCallbackService;
    private TaskExecutionContextCacheManager taskExecutionContextCacheManager;
    private Logger taskLogger;
    private AlertClientService alertClientService;
    private TaskPluginManager taskPluginManager;

    public TaskExecuteThread(TaskExecutionContext taskExecutionContext,
                             TaskCallbackService taskCallbackService,
                             AlertClientService alertClientService) {
        this.taskExecutionContext = taskExecutionContext;
        this.taskCallbackService = taskCallbackService;
        this.alertClientService = alertClientService;
    }

    public TaskExecuteThread(TaskExecutionContext taskExecutionContext,
                             TaskCallbackService taskCallbackService,
                             AlertClientService alertClientService,
                             TaskPluginManager taskPluginManager) {
        this.taskExecutionContext = taskExecutionContext;
        this.taskCallbackService = taskCallbackService;
        this.alertClientService = alertClientService;
        this.taskPluginManager = taskPluginManager;
    }

    @Override
    public void run() {

        TaskExecuteResponseCommand responseCommand = new TaskExecuteResponseCommand(taskExecutionContext.getTaskInstanceId(), taskExecutionContext.getProcessInstanceId());
        try {
            logger.info("script path : {}", taskExecutionContext.getExecutePath());
            // check if the OS user exists
            if (!OSUtils.getUserList().contains(taskExecutionContext.getTenantCode())) {
                String errorLog = String.format("tenantCode: %s does not exist", taskExecutionContext.getTenantCode());
                taskLogger.error(errorLog);
                responseCommand.setStatus(ExecutionStatus.FAILURE.getCode());
                responseCommand.setEndTime(new Date());
                return;
            }

            if (taskExecutionContext.getStartTime() == null) {
                taskExecutionContext.setStartTime(new Date());
            }
            if (taskExecutionContext.getCurrentExecutionStatus() != ExecutionStatus.RUNNING_EXECUTION) {
                changeTaskExecutionStatusToRunning();
            }
            logger.info("the task begins to execute. task instance id: {}", taskExecutionContext.getTaskInstanceId());

            // copy hdfs/minio file to local
            downloadResource(taskExecutionContext.getExecutePath(),
                    taskExecutionContext.getResources(),
                    logger);

            taskExecutionContext.setEnvFile(CommonUtils.getSystemEnvPath());
            taskExecutionContext.setDefinedParams(getGlobalParamsMap());

            taskExecutionContext.setTaskAppId(String.format("%s_%s",
                    taskExecutionContext.getProcessInstanceId(),
                    taskExecutionContext.getTaskInstanceId()));

            preBuildBusinessParams();

            TaskChannel taskChannel = taskPluginManager.getTaskChannelMap().get(taskExecutionContext.getTaskType());
            if (null == taskChannel) {
                throw new PluginNotFoundException(String.format("%s Task Plugin Not Found,Please Check Config File.", taskExecutionContext.getTaskType()));
            }
            TaskRequest taskRequest = JSONUtils.parseObject(JSONUtils.toJsonString(taskExecutionContext), TaskRequest.class);

            task = taskChannel.createTask(taskRequest);
            // task init
            this.task.init();
            //init varPool
            this.task.getParameters().setVarPool(taskExecutionContext.getVarPool());
            // task handle
            this.task.handle();

            // task result process
            if (this.task.getNeedAlert()) {
                sendAlert(this.task.getTaskAlertInfo());
            }
            responseCommand.setStatus(this.task.getExitStatus().getCode());
            responseCommand.setEndTime(new Date());
            responseCommand.setProcessId(this.task.getProcessId());
            responseCommand.setAppIds(this.task.getAppIds());
            responseCommand.setVarPool(JSONUtils.toJsonString(this.task.getParameters().getVarPool()));
            logger.info("task instance id : {},task final status : {}", taskExecutionContext.getTaskInstanceId(), this.task.getExitStatus());
        } catch (Throwable e) {
            logger.error("task scheduler failure", e);
            kill();
            responseCommand.setStatus(ExecutionStatus.FAILURE.getCode());
            responseCommand.setEndTime(new Date());
            responseCommand.setProcessId(task.getProcessId());
            responseCommand.setAppIds(task.getAppIds());
        } finally {
            TaskExecutionContextCacheManager.removeByTaskInstanceId(taskExecutionContext.getTaskInstanceId());
            ResponceCache.get().cache(taskExecutionContext.getTaskInstanceId(), responseCommand.convert2Command(), Event.RESULT);
            taskCallbackService.sendResult(taskExecutionContext.getTaskInstanceId(), responseCommand.convert2Command());
            clearTaskExecPath();
        }
    }

    private void sendAlert(TaskAlertInfo taskAlertInfo) {
        alertClientService.sendAlert(taskAlertInfo.getAlertGroupId(), taskAlertInfo.getTitle(), taskAlertInfo.getContent());
    }

    /**
     * After task finished, clear execute path.
     */
    private void clearTaskExecPath() {
        logger.info("develop mode is: {}", CommonUtils.isDevelopMode());

        if (!CommonUtils.isDevelopMode()) {
            // get exec dir
            String execLocalPath = taskExecutionContext.getExecutePath();

            if (StringUtils.isEmpty(execLocalPath)) {
                logger.warn("task: {} exec local path is empty.", taskExecutionContext.getTaskName());
                return;
            }

            if ("/".equals(execLocalPath)) {
                logger.warn("task: {} exec local path is '/', direct deletion is not allowed", taskExecutionContext.getTaskName());
                return;
            }

            try {
                org.apache.commons.io.FileUtils.deleteDirectory(new File(execLocalPath));
                logger.info("exec local path: {} cleared.", execLocalPath);
            } catch (IOException e) {
                logger.error("delete exec dir failed : {}", e.getMessage(), e);
            }
        }
    }

    private Map<String, String> getGlobalParamsMap() {
        Map<String, String> globalParamsMap = new HashMap<>(16);

        // global params string
        String globalParamsStr = taskExecutionContext.getGlobalParams();
        if (globalParamsStr != null) {
            List<Property> globalParamsList = JSONUtils.toList(globalParamsStr, Property.class);
            globalParamsMap.putAll(globalParamsList.stream().collect(Collectors.toMap(Property::getProp, Property::getValue)));
        }
        return globalParamsMap;
    }

    public void kill() {
        if (task != null) {
            try {
                task.cancelApplication(true);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private void downloadResource(String execLocalPath, Map<String, String> projectRes, Logger logger) {
        if (MapUtils.isEmpty(projectRes)) {
            return;
        }

        Set<Map.Entry<String, String>> resEntries = projectRes.entrySet();

        for (Map.Entry<String, String> resource : resEntries) {
            String fullName = resource.getKey();
            String tenantCode = resource.getValue();
            File resFile = new File(execLocalPath, fullName);
            if (!resFile.exists()) {
                try {
                    // query the tenant code of the resource according to the name of the resource
                    String resHdfsPath = HadoopUtils.getHdfsResourceFileName(tenantCode, fullName);

                    logger.info("get resource file from hdfs :{}", resHdfsPath);
                    HadoopUtils.getInstance().copyHdfsToLocal(resHdfsPath, execLocalPath + File.separator + fullName, false, true);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e.getMessage());
                }
            } else {
                logger.info("file : {} exists ", resFile.getName());
            }
        }
    }

    private void changeTaskExecutionStatusToRunning() {
        taskExecutionContext.setCurrentExecutionStatus(ExecutionStatus.RUNNING_EXECUTION);
        Command ackCommand = buildAckCommand().convert2Command();
        try {
            RetryerUtils.retryCall(() -> {
                taskCallbackService.sendAck(taskExecutionContext.getTaskInstanceId(), ackCommand);
                return Boolean.TRUE;
            });
        } catch (ExecutionException | RetryException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private TaskExecuteAckCommand buildAckCommand() {
        TaskExecuteAckCommand ackCommand = new TaskExecuteAckCommand();
        ackCommand.setTaskInstanceId(taskExecutionContext.getTaskInstanceId());
        ackCommand.setStatus(taskExecutionContext.getCurrentExecutionStatus().getCode());
        ackCommand.setStartTime(taskExecutionContext.getStartTime());
        ackCommand.setLogPath(taskExecutionContext.getLogPath());
        ackCommand.setHost(taskExecutionContext.getHost());
        if (TaskType.SQL.getDesc().equalsIgnoreCase(taskExecutionContext.getTaskType()) || TaskType.PROCEDURE.getDesc().equalsIgnoreCase(taskExecutionContext.getTaskType())) {
            ackCommand.setExecutePath(null);
        } else {
            ackCommand.setExecutePath(taskExecutionContext.getExecutePath());
        }
        return ackCommand;
    }

    public TaskExecutionContext getTaskExecutionContext() {
        return this.taskExecutionContext;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(DateUtils.getRemainTime(taskExecutionContext.getFirstSubmitTime(),
                taskExecutionContext.getDelayTime() * 60L), TimeUnit.SECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        if (o == null) {
            return 1;
        }
        return Long.compare(this.getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
    }

    private void preBuildBusinessParams() {
        Map<String, Property> paramsMap = new HashMap<>();
        // replace variable TIME with $[YYYYmmddd...] in shell file when history run job and batch complement job
        if (taskExecutionContext.getScheduleTime() != null) {
            Date date = taskExecutionContext.getScheduleTime();
            if (CommandType.COMPLEMENT_DATA.getCode() == taskExecutionContext.getCmdTypeIfComplement()) {
                date = DateUtils.add(taskExecutionContext.getScheduleTime(), DAY_OF_MONTH, 1);
            }
            String dateTime = DateUtils.format(date, Constants.PARAMETER_FORMAT_TIME);
            Property p = new Property();
            p.setValue(dateTime);
            p.setProp(Constants.PARAMETER_DATETIME);
            paramsMap.put(Constants.PARAMETER_DATETIME, p);
        }
        taskExecutionContext.setParamsMap(paramsMap);
    }
}
