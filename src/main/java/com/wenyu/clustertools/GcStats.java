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
import com.wenyu.utils.TableGenerator;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Command(name = "gcstats", description = "Print GC Statistics")
public class GcStats extends ClusterToolCmd
{
    @Option(title = "parallel executor", name = {"-p", "--par-jobs"}, description = "Number of threads to get timeout of all nodes.")
    private int parallel = 1;

    @Override
    public void execute() {
        ExecutorService executor = Executors.newFixedThreadPool(parallel);

        Map<ClusterToolCmd.Node, Future<List<String>>> futures = new HashMap<>();
        for (ClusterToolCmd.Node node : nodes) {
            futures.put(node, executor.submit(new Executor(node)));
        }

        List<String> header = new ArrayList<>();
        header.add("Server");
        header.add("Interval (ms)");
        header.add("Max GC Elapsed (ms)");
        header.add("Total GC Elapsed (ms)");
        header.add("Stdev GC Elapsed (ms)");
        header.add("GC Reclaimed (MB)");
        header.add("Collections");
        header.add("Direct Memory Bytes");

        List<List<String>> rows = new ArrayList<>();
        for (Map.Entry<ClusterToolCmd.Node, Future<List<String>>> future : futures.entrySet()) {
            try {
                List<String> row = future.getValue().get(Constants.MAX_PARALLEL_WAIT_IN_SEC, TimeUnit.SECONDS);
                rows.add(row);
            } catch (Exception ex) {
                System.out.println(String.format("%s failed with error: %s", future.getKey().server, ex.toString()));
                ex.printStackTrace();
            }
        }

        System.out.println(TableGenerator.generateTable(header, rows));
    }

    private class Executor extends AsyncTask<List<String>> {
        private ClusterToolCmd.Node node;

        public Executor(ClusterToolCmd.Node node) {
            this.node = node;
        }

        @Override
        public List<String> execute() {
            ClusterToolNodeProbe probe = connect(node);

            double[] stats = probe.getAndResetGCStats();
            double mean = stats[2] / stats[5];
            double stdev = Math.sqrt((stats[3] / stats[5]) - (mean * mean));

            List<String> result = new ArrayList<>();
            result.add(node.server);
            result.add(String.valueOf(stats[0]));
            result.add(String.valueOf(stats[1]));
            result.add(String.valueOf(stats[2]));
            result.add(String.valueOf(stdev));
            result.add(String.valueOf(stats[4]));
            result.add(String.valueOf(stats[5]));
            result.add(String.valueOf((long)stats[6]));
            return result;
        }
    }
}