/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wenyu.clustertools;

import com.wenyu.utils.AsyncTask;
import com.wenyu.utils.ClusterToolNodeProbe;
import com.wenyu.utils.Constants;
import com.wenyu.utils.OutTextStyle;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Command(name = "truncatehints", description = "Truncate all hints on the cluster")
public class TruncateHints extends ClusterToolCmd
{
    @Override
    public void execute() {
        ClusterToolNodeProbe probe = getNodeProbe();
        probe.truncateHints();
        System.out.println("Hints are all truncated");
    }

    private ClusterToolNodeProbe getNodeProbe() {
        int nodeCount = nodes.size();
        int randomNode = new Random().nextInt(nodeCount);
        Node nodeToBeConnect = nodes.get(randomNode);
        ClusterToolNodeProbe nodeProbe = connect(nodeToBeConnect);
        return nodeProbe;
    }
}