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
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;
import org.apache.cassandra.tools.NodeProbe;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterables.toArray;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;

@Command(name = "flush", description = "Flush one or more tables")
public class Flush extends ClusterToolCmd {
    @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

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
            System.out.println("Start to flush " + node.server);
            return true;
        }

        @Override
        public Void execute() {
            NodeProbe probe = connect(node);

            List<String> keyspaces = parseOptionalKeyspace(args, probe);
            String[] tableNames = parseOptionalTables(args);

            for (String keyspace : keyspaces) {
                try {
                    probe.forceKeyspaceFlush(keyspace, tableNames);
                } catch (Exception e) {
                    throw new RuntimeException("Error occurred during flushing", e);
                }
            }

            return null;
        }

        @Override
        public boolean postExecute() {
            System.out.println("Finish flushing " + node.server);
            return true;
        }
    }

    private List<String> parseOptionalKeyspace(List<String> cmdArgs, NodeProbe nodeProbe) {
        List<String> keyspaces = new ArrayList<>();


        if (cmdArgs == null || cmdArgs.isEmpty()) {
            keyspaces.addAll(keyspaces = nodeProbe.getNonSystemKeyspaces());
        } else {
            keyspaces.add(cmdArgs.get(0));
        }

        for (String keyspace : keyspaces) {
            if (!nodeProbe.getKeyspaces().contains(keyspace))
                throw new IllegalArgumentException("Keyspace [" + keyspace + "] does not exist.");
        }

        return Collections.unmodifiableList(keyspaces);
    }

    private String[] parseOptionalTables(List<String> cmdArgs) {
        return cmdArgs.size() <= 1 ? EMPTY_STRING_ARRAY : toArray(cmdArgs.subList(1, cmdArgs.size()), String.class);
    }
}