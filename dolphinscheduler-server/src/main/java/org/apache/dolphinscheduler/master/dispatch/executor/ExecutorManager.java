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

package org.apache.dolphinscheduler.master.dispatch.executor;

import org.apache.dolphinscheduler.master.common.ExecutionContext;
import org.apache.dolphinscheduler.master.common.ExecuteException;

public interface ExecutorManager<T> {

    void beforeExecute(ExecutionContext executeContext) throws ExecuteException;

    T execute(ExecutionContext context) throws ExecuteException;

    /**
     * Execute task directly without retry operation.
     * @param context context
     * @throws ExecuteException if error throws ExecuteException
     */
    void executeDirectly(ExecutionContext context) throws ExecuteException;

    void afterExecute(ExecutionContext context) throws ExecuteException;
}
