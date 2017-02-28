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
import com.wenyu.utils.Constants;
import io.airlift.command.Command;
import io.airlift.command.Option;
import com.wenyu.utils.ClusterToolNodeProbe;;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

@Command(name = "getcompactionthroughput", description = "Print the MB/s throughput cap for compaction in the system")
public class GetCompactionThroughput extends ClusterToolCmd
{
    @Option(title = "parallel executor", name = {"-p", "--par-jobs"}, description = "Number of threads to get timeout of all nodes.")
    private int parallel = 1;

    @Override
    public void execute()
    {
        ExecutorService executor = Executors.newFixedThreadPool(parallel);

        Map<Node, Future<String>> futures = new HashMap<>();
        for (Node node : nodes) {
            futures.put(node, executor.submit(new Executor(node)));
        }

        for (Map.Entry<Node, Future<String>> future : futures.entrySet()) {
            try {
                System.out.println(future.getValue().get(Constants.MAX_PARALLEL_WAIT_IN_SEC, TimeUnit.SECONDS));
            } catch (Exception ex) {
                System.out.println(String.format("%s failed with error: %s", future.getKey().server, ex.toString()));
                ex.printStackTrace();
            }
        }
    }

    private class Executor extends AsyncTask<String> {
        private Node node;

        public Executor(Node node) {
            this.node = node;
        }

        @Override
        public String execute() {
            ClusterToolNodeProbe probe = connect(node);

            try {
                String template = "Current %s's compaction throughput: %s MB/s";
                String result = String.format(template, node.server, probe.getCompactionThroughput());
                return result;
            } catch (Exception e) {
                String error = String.format("%s failed with error: %s", node.server, e.toString());
                return error;
            }
        }
    }
}