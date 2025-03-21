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

package org.apache.dolphinscheduler.plugin.task.python;

import org.apache.dolphinscheduler.plugin.task.api.AbstractTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.util.MapUtils;
import org.apache.dolphinscheduler.spi.task.AbstractParameters;
import org.apache.dolphinscheduler.spi.task.Property;
import org.apache.dolphinscheduler.spi.task.TaskConstants;
import org.apache.dolphinscheduler.spi.task.paramparser.ParamUtils;
import org.apache.dolphinscheduler.spi.task.paramparser.ParameterUtils;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * python task
 */
public class PythonTask extends AbstractTaskExecutor {

    /**
     * python parameters
     */
    private PythonParameters pythonParameters;

    /**
     * task dir
     */
    private String taskDir;

    /**
     * python command executor
     */
    private PythonCommandExecutor pythonCommandExecutor;

    private TaskRequest taskRequest;

    /**
     * constructor
     *
     * @param taskRequest taskRequest
     */
    public PythonTask(TaskRequest taskRequest) {
        super(taskRequest);
        this.taskRequest = taskRequest;

        this.pythonCommandExecutor = new PythonCommandExecutor(this::logHandle,
                taskRequest,
                logger);
    }

    @Override
    public void init() {
        logger.info("python task params {}", taskRequest.getTaskParams());

        pythonParameters = JSONUtils.parseObject(taskRequest.getTaskParams(), PythonParameters.class);

        if (!pythonParameters.checkParameters()) {
            throw new TaskException("python task params is not valid");
        }
    }

    @Override
    public String getPreScript() {
        String rawPythonScript = pythonParameters.getRawScript().replaceAll("\\r\\n", "\n");
        try {
            rawPythonScript = convertPythonScriptPlaceholders(rawPythonScript);
        } catch (StringIndexOutOfBoundsException e) {
            logger.error("setShareVar field format error, raw python script : {}", rawPythonScript);
        }
        return rawPythonScript;
    }

    @Override
    public void handle() throws Exception {
        try {
            // construct process
            String command = buildCommand();
            TaskResponse taskResponse = pythonCommandExecutor.run(command);
            setExitStatusCode(taskResponse.getExitStatusCode());
            setAppIds(taskResponse.getAppIds());
            setProcessId(taskResponse.getProcessId());
            setVarPool(pythonCommandExecutor.getVarPool());
        } catch (Exception e) {
            logger.error("python task failure", e);
            setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
            throw new TaskException("run python task error", e);
        }
    }

    @Override
    public void cancelApplication(boolean cancelApplication) throws Exception {
        // cancel process
        pythonCommandExecutor.cancelApplication();
    }

    @Override
    public AbstractParameters getParameters() {
        return pythonParameters;
    }

    /**
     * convertPythonScriptPlaceholders
     *
     * @param rawScript rawScript
     * @return String
     * @throws StringIndexOutOfBoundsException StringIndexOutOfBoundsException
     */
    private static String convertPythonScriptPlaceholders(String rawScript) throws StringIndexOutOfBoundsException {
        int len = "${setShareVar(${".length();
        int scriptStart = 0;
        while ((scriptStart = rawScript.indexOf("${setShareVar(${", scriptStart)) != -1) {
            int start = -1;
            int end = rawScript.indexOf('}', scriptStart + len);
            String prop = rawScript.substring(scriptStart + len, end);

            start = rawScript.indexOf(',', end);
            end = rawScript.indexOf(')', start);

            String value = rawScript.substring(start + 1, end);

            start = rawScript.indexOf('}', start) + 1;
            end = rawScript.length();

            String replaceScript = String.format("print(\"${{setValue({},{})}}\".format(\"%s\",%s))", prop, value);

            rawScript = rawScript.substring(0, scriptStart) + replaceScript + rawScript.substring(start, end);

            scriptStart += replaceScript.length();
        }
        return rawScript;
    }

    /**
     * build command
     *
     * @return raw python script
     * @throws Exception exception
     */
    private String buildCommand() throws Exception {
        String rawPythonScript = pythonParameters.getRawScript().replaceAll("\\r\\n", "\n");

        // replace placeholder
        Map<String, Property> paramsMap = ParamUtils.convert(taskRequest, pythonParameters);
        if (MapUtils.isEmpty(paramsMap)) {
            paramsMap = new HashMap<>();
        }
        if (MapUtils.isNotEmpty(taskRequest.getParamsMap())) {
            paramsMap.putAll(taskRequest.getParamsMap());
        }
        rawPythonScript = ParameterUtils.convertParameterPlaceholders(rawPythonScript, ParamUtils.convert(paramsMap));

        logger.info("raw python script : {}", pythonParameters.getRawScript());
        logger.info("task dir : {}", taskDir);

        return rawPythonScript;
    }

}
