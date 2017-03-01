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
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Command(name = "garbagecollect", description = "Remove deleted data from one or more tables")
public class GarbageCollect extends ClusterToolCmd
{
    @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    @Option(title = "granularity",
        name = {"-g", "--granularity"},
        allowedValues = {"ROW", "CELL"},
        description = "Granularity of garbage removal. ROW (default) removes deleted partitions and rows, CELL also removes overwritten or deleted cells.")
    private String tombstoneOption = "ROW";

    @Option(title = "jobs",
            name = {"-j", "--jobs"},
            description = "Number of sstables to cleanup simultanously, set to 0 to use all available compaction threads")
    private int jobs = 2;

    @Option(title = "parallel executor", name = {"-p", "--par-jobs"}, description = "Number of threads to get timeout of all nodes.")
    private int parallel = 1;

    @Override
    public void execute() {
        ExecutorService executor = Executors.newFixedThreadPool(parallel);

        Map<ClusterToolCmd.Node, Future<Void>> futures = new HashMap<>();
        for (ClusterToolCmd.Node node : nodes) {
            futures.put(node, executor.submit(new Executor(node)));
        }

        for (Map.Entry<ClusterToolCmd.Node, Future<Void>> future : futures.entrySet()) {
            try {
                future.getValue().get(Constants.MAX_PARALLEL_WAIT_IN_SEC, TimeUnit.SECONDS);
            } catch (Exception ex) {
                System.out.println(String.format("%s failed with error: %s", future.getKey().server, ex.toString()));
                ex.printStackTrace();
            }
        }
    }

    private class Executor extends AsyncTask<Void> {
        private ClusterToolCmd.Node node;

        public Executor(ClusterToolCmd.Node node) {
            this.node = node;
        }

        @Override
        public boolean preExecute() {
            System.out.println("Start to GC " + node.server);
            return true;
        }

        @Override
        public Void execute() {
            ClusterToolNodeProbe probe = connect(node);

            List<String> keyspaces = Utilities.parseOptionalKeyspace(args, probe, KeyspaceSet.NON_SYSTEM);
            String[] tableNames = Utilities.parseOptionalTables(args);

            for (String keyspace : keyspaces)
            {
                try
                {
                    probe.garbageCollect(System.out, tombstoneOption, jobs, keyspace, tableNames);
                } catch (Exception e)
                {
                    throw new RuntimeException("Error occurred during garbage collection", e);
                }
            }

            return null;
        }

        @Override
        public boolean postExecute() {
            System.out.println("Finish GCing " + node.server);
            return true;
        }
    }
}