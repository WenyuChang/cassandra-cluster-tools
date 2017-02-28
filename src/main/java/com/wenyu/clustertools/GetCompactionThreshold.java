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
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.tools.NodeProbe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "getcompactionthreshold", description = "Print min and max compaction thresholds for a given table")
public class GetCompactionThreshold extends ClusterToolCmd
{
    @Arguments(usage = "<keyspace> <table>", description = "The keyspace with a table")
    private List<String> args = new ArrayList<>();

    @Option(title = "parallel executor", name = {"-p", "--par-jobs"}, description = "Number of threads to get timeout of all nodes.")
    private int parallel = 1;

    @Override
    public void execute()
    {
        ExecutorService executor = Executors.newFixedThreadPool(parallel);

        Map<ClusterToolCmd.Node, Future<String>> futures = new HashMap<>();
        for (ClusterToolCmd.Node node : nodes) {
            futures.put(node, executor.submit(new Executor(node)));
        }

        for (Map.Entry<ClusterToolCmd.Node, Future<String>> future : futures.entrySet()) {
            try {
                System.out.println(future.getValue().get(Constants.MAX_PARALLEL_WAIT_IN_SEC, TimeUnit.SECONDS));
            } catch (Exception ex) {
                System.out.println(String.format("%s failed with error: %s", future.getKey().server, ex.toString()));
            }
        }
    }

    private class Executor extends AsyncTask<String> {
        private Node node;

        public Executor(Node node) {
            this.node = node;
        }

        @Override
        public String execute()
        {
            checkArgument(args.size() == 2, "getcompactionthreshold requires ks and cf args");
            String ks = args.get(0);
            String cf = args.get(1);

            NodeProbe probe = connect(node);
            ColumnFamilyStoreMBean cfsProxy = probe.getCfsProxy(ks, cf);
            String result = "Current " + node.server + "'s compaction thresholds for " + ks + "/" + cf + ": " +
                    " min = " + cfsProxy.getMinimumCompactionThreshold() + ", " +
                    " max = " + cfsProxy.getMaximumCompactionThreshold();
            return result;
        }
    }
}