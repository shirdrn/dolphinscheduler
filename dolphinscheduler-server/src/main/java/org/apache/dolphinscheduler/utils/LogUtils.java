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

package org.apache.dolphinscheduler.utils;

import org.apache.dolphinscheduler.entity.TaskExecutionContext;
import org.apache.dolphinscheduler.log.TaskLogDiscriminator;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.sift.SiftingAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.spi.AppenderAttachable;

public class LogUtils {

    private LogUtils() throws IllegalStateException {
        throw new IllegalStateException("Utility class");
    }

    public static String getTaskLogPath(Long processDefineCode, int processDefineVersion, int processInstanceId, int taskInstanceId) {
        // Optional.map will be skipped if null
        return Optional.of(LoggerFactory.getILoggerFactory())
                .map(e -> (AppenderAttachable<ILoggingEvent>) (e.getLogger("ROOT")))
                .map(e -> (SiftingAppender) (e.getAppender("TASKLOGFILE")))
                .map(e -> ((TaskLogDiscriminator) (e.getDiscriminator())))
                .map(TaskLogDiscriminator::getLogBase)
                .map(e -> Paths.get(e)
                        .toAbsolutePath()
                        .resolve(processDefineCode + "_" + processDefineVersion)
                        .resolve(String.valueOf(processInstanceId))
                        .resolve(taskInstanceId + ".log"))
                .map(Path::toString)
                .orElse("");
    }

    public static String getTaskLogPath(TaskExecutionContext taskExecutionContext) {
        return getTaskLogPath(taskExecutionContext.getProcessDefineCode(),
                taskExecutionContext.getProcessDefineVersion(),
                taskExecutionContext.getProcessInstanceId(),
                taskExecutionContext.getTaskInstanceId());
    }

}
