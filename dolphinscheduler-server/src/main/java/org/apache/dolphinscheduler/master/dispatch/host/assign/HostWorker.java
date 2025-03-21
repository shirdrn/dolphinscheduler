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

package org.apache.dolphinscheduler.master.dispatch.host.assign;

import org.apache.dolphinscheduler.network.utils.Host;

public class HostWorker extends Host {

    private int hostWeight;
    private String workerGroup;

    public HostWorker(String ip, int port, int hostWeight, String workerGroup) {
        super(ip, port);
        this.hostWeight = hostWeight;
        this.workerGroup = workerGroup;
    }

    public HostWorker(String address, int hostWeight, String workerGroup) {
        super(address);
        this.hostWeight = hostWeight;
        this.workerGroup = workerGroup;
    }

    public int getHostWeight() {
        return hostWeight;
    }

    public void setHostWeight(int hostWeight) {
        this.hostWeight = hostWeight;
    }

    public String getWorkerGroup() {
        return workerGroup;
    }

    public void setWorkerGroup(String workerGroup) {
        this.workerGroup = workerGroup;
    }

    public static HostWorker of(String address, int hostWeight, String workerGroup) {
        return new HostWorker(address, hostWeight, workerGroup);
    }

    @Override
    public String toString() {
        return "Host{"
                + "hostWeight=" + hostWeight
                + ", workerGroup='" + workerGroup + '\''
                + '}';
    }

}
