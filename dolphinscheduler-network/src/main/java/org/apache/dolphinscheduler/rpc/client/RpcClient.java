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

package org.apache.dolphinscheduler.rpc.client;

import static net.bytebuddy.matcher.ElementMatchers.isDeclaredBy;

import org.apache.dolphinscheduler.network.utils.Host;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.implementation.MethodDelegation;

public class RpcClient implements IRpcClient {

    @Override
    public <T> T create(Class<T> clazz, Host host) throws Exception {
        return new ByteBuddy()
                .subclass(clazz)
                .method(isDeclaredBy(clazz)).intercept(MethodDelegation.to(new ConsumerInterceptor(host)))
                .make()
                .load(getClass().getClassLoader())
                .getLoaded()
                .getDeclaredConstructor().newInstance();
    }
}
