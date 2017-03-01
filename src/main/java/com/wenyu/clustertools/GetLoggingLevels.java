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

import com.wenyu.utils.*;
import io.airlift.command.Command;
import io.airlift.command.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Command(name = "getlogginglevels", description = "Get the runtime logging levels")
public class GetLoggingLevels extends ClusterToolCmd
{
    @Option(title = "parallel executor", name = {"-p", "--par-jobs"}, description = "Number of threads to get timeout of all nodes.")
    private int parallel = 1;

    @Override
    public void execute() {
        ExecutorService executor = Executors.newFixedThreadPool(parallel);

        System.out.println("BBBBBBBBBBBBB");
        Map<ClusterToolCmd.Node, Future<String>> futures = new HashMap<>();
        for (ClusterToolCmd.Node node : nodes) {
            futures.put(node, executor.submit(new Executor(node)));
        }
        System.out.println("AAAAAAAAAA");
        for (Map.Entry<ClusterToolCmd.Node, Future<String>> future : futures.entrySet()) {
            try {
                System.out.println("CCCCCCCCC");
                System.out.println(future.getValue().get(Constants.MAX_PARALLEL_WAIT_IN_SEC, TimeUnit.SECONDS));
            } catch (Exception ex) {
                System.out.println(String.format("%s failed with error: %s", future.getKey().server, ex.toString()));
                ex.printStackTrace();
            }
        }
    }

    private class Executor extends AsyncTask<String> {
        private ClusterToolCmd.Node node;

        public Executor(ClusterToolCmd.Node node) {
            this.node = node;
        }

        @Override
        public String execute() {
            ClusterToolNodeProbe probe = connect(node);

            // what if some one set a very long logger name? 50 space may not be enough...
            String result = "";
            for (Map.Entry<String, String> entry : probe.getLoggingLevels().entrySet())
                result += String.format("%s:%-50s%10s%n", node.server, entry.getKey(), entry.getValue());
            return result;
        }
    }

}