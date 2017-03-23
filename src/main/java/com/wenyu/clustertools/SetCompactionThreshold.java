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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.parseInt;

@Command(name = "setcompactionthreshold", description = "Set min and max compaction thresholds for a given table")
public class SetCompactionThreshold extends ClusterToolCmd
{
    @Arguments(title = "<keyspace> <table> <minthreshold> <maxthreshold>", usage = "<keyspace> <table> <minthreshold> <maxthreshold>", description = "The keyspace, the table, min and max threshold", required = true)
    private List<String> args = new ArrayList<>();

    @Option(title = "parallel executor", name = {"-p", "--par-jobs"}, description = "Number of threads to get timeout of all nodes.")
    private int parallel = 1;

    @Override
    public void execute() {
        checkArgument(args.size() == 4, "setcompactionthreshold requires ks, cf, min, and max threshold args.");

        int minthreshold = parseInt(args.get(2));
        int maxthreshold = parseInt(args.get(3));
        checkArgument(minthreshold >= 0 && maxthreshold >= 0, "Thresholds must be positive integers");
        checkArgument(minthreshold <= maxthreshold, "Min threshold cannot be greater than max.");
        checkArgument(minthreshold >= 2 || maxthreshold == 0, "Min threshold must be at least 2");

        ExecutorService executor = Executors.newFixedThreadPool(parallel);

        Map<Node, Future<String>> futures = new HashMap<>();
        for (ClusterToolCmd.Node node : nodes) {
            futures.put(node, executor.submit(new Executor(node)));
        }

        for (Map.Entry<ClusterToolCmd.Node, Future<String>> future : futures.entrySet()) {
            try {
                String result = future.getValue().get(Constants.MAX_PARALLEL_WAIT_IN_SEC, TimeUnit.SECONDS);
                System.out.println(result);
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
            int minThreshold = parseInt(args.get(2));
            int maxThreshold = parseInt(args.get(3));
            probe.setCompactionThreshold(args.get(0), args.get(1), minThreshold, maxThreshold);
            return "Finished set compaction threshold on " + node.server + ":" + args.get(0) + "/" + args.get(1) + " to {min:" + minThreshold + "/max:" + maxThreshold + "]";
        }
    }
}