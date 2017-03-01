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
import com.wenyu.utils.ClusterToolNodeProbe;;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterables.toArray;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.join;

@Command(name = "clearsnapshot", description = "Remove the snapshot with the given name from the given keyspaces. If no snapshotName is specified we will remove all snapshots")
public class ClearSnapshot extends ClusterToolCmd
{
    @Arguments(usage = "[<keyspaces>...] ", description = "Remove snapshots from the given keyspaces")
    private List<String> keyspaces = new ArrayList<>();

    @Option(title = "snapshot_name", name = "-t", description = "Remove the snapshot with a given name")
    private String snapshotName = EMPTY;

    @Option(title = "parallel executor", name = {"-p", "--par-jobs"}, description = "Number of threads to get timeout of all nodes.")
    private int parallel = 1;

    @Override
    public void execute() {
        ExecutorService executor = Executors.newFixedThreadPool(parallel);

        Map<Node, Future<Void>> futures = new HashMap<>();
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
            StringBuilder sb = new StringBuilder();
            sb.append("Requested clearing snapshot(s) for ");

            if (keyspaces.isEmpty())
                sb.append("[all keyspaces]");
            else
                sb.append("[").append(join(keyspaces, ", ")).append("]");

            if (!snapshotName.isEmpty())
                sb.append(" with snapshot name [").append(snapshotName).append("]");

            System.out.println(sb.toString());
            return true;
        }

        @Override
        public Void execute() {
            ClusterToolNodeProbe probe = connect(node);
            try {
                probe.clearSnapshot(snapshotName, toArray(keyspaces, String.class));
            } catch (IOException e) {
                throw new RuntimeException("Error during clearing snapshots", e);
            }
            return null;
        }

        @Override
        public boolean postExecute() {
            System.out.println("Finish flushing " + node.server);
            return true;
        }
    }
}