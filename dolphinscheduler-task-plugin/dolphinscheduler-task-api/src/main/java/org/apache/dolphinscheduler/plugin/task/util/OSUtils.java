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

package org.apache.dolphinscheduler.plugin.task.util;

import org.apache.dolphinscheduler.plugin.task.api.ShellExecutor;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import java.io.IOException;
import java.util.StringTokenizer;

public class OSUtils {

    private OSUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static String getSudoCmd(String tenantCode, String command) {
        return StringUtils.isEmpty(tenantCode) ? command : "sudo -u " + tenantCode + " " + command;
    }

    public static boolean isMacOS() {
        return getOSName().startsWith("Mac");
    }

    public static boolean isWindows() {
        return getOSName().startsWith("Windows");
    }

    public static String exeCmd(String command) throws IOException {
        StringTokenizer st = new StringTokenizer(command);
        String[] cmdArray = new String[st.countTokens()];
        for (int i = 0; st.hasMoreTokens(); i++) {
            cmdArray[i] = st.nextToken();
        }
        return exeShell(cmdArray);
    }

    public static String exeShell(String[] command) throws IOException {
        return ShellExecutor.execCommand(command);
    }

    public static String getOSName() {
        return System.getProperty("os.name");
    }
}
