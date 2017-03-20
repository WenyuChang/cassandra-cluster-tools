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
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

;

@Command(name = "compactpartition", description = "User defined compaction on partition")
public class CompactPartition extends ClusterToolCmd {
    @Arguments(usage = "<keyspace> <cfname> <key>", description = "The keyspace, the column family, and the partition key for which we need to compact")
    private List<String> args = new ArrayList<>();

    @Option(title = "parallel executor", name = {"-p", "--par-jobs"}, description = "Number of threads to get timeout of all nodes.")
    private int parallel = 1;

    @Override
    public void execute() {
        checkArgument(args.size() == 3, "getendpoints requires ks, cf and key args");
        ClusterToolNodeProbe probe = getNodeProbe();

        String ks = args.get(0);
        String cf = args.get(1);
        String key = args.get(2);
        List<InetAddress> endpoints = probe.getEndpoints(ks, cf, key);

        ExecutorService executor = Executors.newFixedThreadPool(parallel);
        HashMap<InetAddress, Node> nodeMap = new HashMap<>();
        for (Node node : nodes) {
            try {
                InetAddress address = InetAddress.getByName(node.server);
                nodeMap.put(address, node);
            } catch (Exception ex) {}
        }

        Map<ClusterToolCmd.Node, Future<String>> futures = new HashMap<>();
        for (InetAddress endpoint : endpoints) {
            if (!nodeMap.containsKey(endpoint)) {
                System.out.println("Cannot find endpoint (" + endpoint.getHostAddress() + ") in nodes file.");
                continue;
            }
            Node node = nodeMap.get(endpoint);
            futures.put(node, executor.submit(new Executor(node, ks, cf, key)));
        }

        for (Map.Entry<ClusterToolCmd.Node, Future<String>> future : futures.entrySet()) {
            try {
                System.out.println(future.getValue().get(Constants.MAX_PARALLEL_WAIT_IN_SEC, TimeUnit.SECONDS));
            } catch (Exception ex) {
                System.out.println(String.format("%s failed with error: %s", future.getKey().server, ex.toString()));
                ex.printStackTrace();
            }
        }
    }

    private ClusterToolNodeProbe getNodeProbe() {
        int nodeCount = nodes.size();
        int randomNode = new Random().nextInt(nodeCount);
        Node nodeToBeConnect = nodes.get(randomNode);
        ClusterToolNodeProbe nodeProbe = connect(nodeToBeConnect);
        return nodeProbe;
    }

    private class Executor extends AsyncTask<String> {
        private Node node;
        private String ks;
        private String cf;
        private String key;

        public Executor(Node node, String ks, String cf, String key) {
            this.node = node;
            this.ks = ks;
            this.cf = cf;
            this.key = key;
        }

        @Override
        public String execute() {
            ClusterToolNodeProbe probe = connect(node);

            try {
                List<String> sstables = probe.getSSTables(ks, cf, key, false);
                probe.forceUserDefinedCompaction(String.join(",", sstables));

                StringBuilder builder = new StringBuilder();
                builder.append("Compacted " + node.server + ": [" + String.join(",", sstables) + "]\n");
                return builder.toString();
            } catch (Exception e) {
                String error = String.format("%s failed with error: %s", node.server, e.toString());
                return error;
            }
        }
    }
}
