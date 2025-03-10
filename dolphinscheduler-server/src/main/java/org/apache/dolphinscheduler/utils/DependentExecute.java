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

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.DependResult;
import org.apache.dolphinscheduler.common.enums.DependentRelation;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.model.DateInterval;
import org.apache.dolphinscheduler.common.model.DependentItem;
import org.apache.dolphinscheduler.common.utils.DependentUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.process.ProcessService;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DependentExecute {

    private Logger logger = LoggerFactory.getLogger(DependentExecute.class);
    private final ProcessService processService = SpringApplicationContext.getBean(ProcessService.class);
    private List<DependentItem> dependItemList;
    private DependentRelation relation;
    private DependResult modelDependResult = DependResult.WAITING;
    private Map<String, DependResult> dependResultMap = new HashMap<>();

    public DependentExecute(List<DependentItem> itemList, DependentRelation relation) {
        this.dependItemList = itemList;
        this.relation = relation;
    }

    private DependResult getDependentResultForItem(DependentItem dependentItem, Date currentTime) {
        List<DateInterval> dateIntervals = DependentUtils.getDateIntervalList(currentTime, dependentItem.getDateValue());
        return calculateResultForTasks(dependentItem, dateIntervals);
    }

    private DependResult calculateResultForTasks(DependentItem dependentItem,
                                                 List<DateInterval> dateIntervals) {
        DependResult result = DependResult.FAILED;
        for (DateInterval dateInterval : dateIntervals) {
            ProcessInstance processInstance = findLastProcessInterval(dependentItem.getDefinitionCode(),
                    dateInterval);
            if (processInstance == null) {
                return DependResult.WAITING;
            }
            // need to check workflow for updates, so get all task and check the task state
            if (dependentItem.getDepTasks().equals(Constants.DEPENDENT_ALL)) {
                result = dependResultByProcessInstance(processInstance);
            } else {
                result = getDependTaskResult(dependentItem.getDepTasks(), processInstance);
            }
            if (result != DependResult.SUCCESS) {
                break;
            }
        }
        return result;
    }

    /**
     * depend type = depend_all
     */
    private DependResult dependResultByProcessInstance(ProcessInstance processInstance) {
        if (!processInstance.getState().typeIsFinished()) {
            return DependResult.WAITING;
        }
        if (processInstance.getState().typeIsSuccess()) {
            return DependResult.SUCCESS;
        }
        return DependResult.FAILED;
    }

    private DependResult getDependTaskResult(String taskName, ProcessInstance processInstance) {
        DependResult result;
        TaskInstance taskInstance = null;
        List<TaskInstance> taskInstanceList = processService.findValidTaskListByProcessId(processInstance.getId());

        for (TaskInstance task : taskInstanceList) {
            if (task.getName().equals(taskName)) {
                taskInstance = task;
                break;
            }
        }
        if (taskInstance == null) {
            // cannot find task in the process instance
            // maybe because process instance is running or failed.
            if (processInstance.getState().typeIsFinished()) {
                result = DependResult.FAILED;
            } else {
                return DependResult.WAITING;
            }
        } else {
            result = getDependResultByState(taskInstance.getState());
        }
        return result;
    }

    /**
     * find the last one process instance that :
     * 1. manual run and finish between the interval
     * 2. schedule run and schedule time between the interval
     *
     * @param definitionCode definition code
     * @param dateInterval   date interval
     * @return ProcessInstance
     */
    private ProcessInstance findLastProcessInterval(Long definitionCode, DateInterval dateInterval) {

        ProcessInstance runningProcess = processService.findLastRunningProcess(definitionCode, dateInterval.getStartTime(), dateInterval.getEndTime());
        if (runningProcess != null) {
            return runningProcess;
        }

        ProcessInstance lastSchedulerProcess = processService.findLastSchedulerProcessInterval(definitionCode, dateInterval);

        ProcessInstance lastManualProcess = processService.findLastManualProcessInterval(definitionCode, dateInterval);

        if (lastManualProcess == null) {
            return lastSchedulerProcess;
        }
        if (lastSchedulerProcess == null) {
            return lastManualProcess;
        }

        return (lastManualProcess.getEndTime().after(lastSchedulerProcess.getEndTime())) ? lastManualProcess : lastSchedulerProcess;
    }

    private DependResult getDependResultByState(ExecutionStatus state) {

        if (!state.typeIsFinished()) {
            return DependResult.WAITING;
        } else if (state.typeIsSuccess()) {
            return DependResult.SUCCESS;
        } else {
            return DependResult.FAILED;
        }
    }

    private DependResult getDependResultByProcessStateWhenTaskNull(ExecutionStatus state) {

        if (state.typeIsRunning()
                || state == ExecutionStatus.SUBMITTED_SUCCESS
                || state == ExecutionStatus.WAITING_THREAD) {
            return DependResult.WAITING;
        } else {
            return DependResult.FAILED;
        }
    }

    public boolean finish(Date currentTime) {
        if (modelDependResult == DependResult.WAITING) {
            modelDependResult = getModelDependResult(currentTime);
            return false;
        }
        return true;
    }

    public DependResult getModelDependResult(Date currentTime) {
        List<DependResult> dependResultList = new ArrayList<>();
        for (DependentItem dependentItem : dependItemList) {
            DependResult dependResult = getDependResultForItem(dependentItem, currentTime);
            if (dependResult != DependResult.WAITING) {
                dependResultMap.put(dependentItem.getKey(), dependResult);
            }
            dependResultList.add(dependResult);
        }
        modelDependResult = DependentUtils.getDependResultForRelation(this.relation, dependResultList);
        return modelDependResult;
    }

    private DependResult getDependResultForItem(DependentItem item, Date currentTime) {
        String key = item.getKey();
        if (dependResultMap.containsKey(key)) {
            return dependResultMap.get(key);
        }
        return getDependentResultForItem(item, currentTime);
    }

    public Map<String, DependResult> getDependResultMap() {
        return dependResultMap;
    }

}
