package com.wenyu.clustertools;

import com.wenyu.utils.*;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by wenyu on 3/21/17.
 */

@Command(name = "disableautocompaction", description = "Disable autocompaction for the given keyspace and table")
public class DisableAutoCompaction extends ClusterToolCmd {
    @Arguments(usage = "[<keyspace> <cfnames>...]", description = "The keyspace followed by one or many column families")
    private List<String> args = new ArrayList<>();

    @Option(title = "parallel executor", name = {"-p", "--par-jobs"}, description = "Number of threads to get timeout of all nodes.")
    private int parallel = 1;

    @Override
    public void execute() {
        ExecutorService executor = Executors.newFixedThreadPool(parallel);

        Map<Node, Future<Void>> futures = new HashMap<>();
        for (Node node : nodes) {
            futures.put(node, executor.submit(new Executor(node)));
        }

        for (Map.Entry<Node, Future<Void>> future : futures.entrySet()) {
            try {
                future.getValue().get(Constants.MAX_PARALLEL_WAIT_IN_SEC, TimeUnit.SECONDS);
            } catch (Exception ex) {
                System.out.println(String.format("%s failed with error: %s", future.getKey().server, ex.toString()));
                ex.printStackTrace();
            }
        }
    }

    private class Executor extends AsyncTask<Void> {
        private Node node;

        public Executor(Node node) {
            this.node = node;
        }

        @Override
        public Void execute() {
            ClusterToolNodeProbe probe = connect(node);

            List<String> keyspaces = Utilities.parseOptionalKeyspace(args, probe, KeyspaceSet.NON_SYSTEM);
            String[] tableNames = Utilities.parseOptionalTables(args);

            for (String keyspace : keyspaces) {
                try {
                    probe.disableAutoCompaction(keyspace, tableNames);
                } catch (IOException e) {
                    throw new RuntimeException("Error occurred during enabling auto-compaction", e);
                }
            }
            return null;
        }

        @Override
        public boolean postExecute() {
            if (args.size() == 0) {
                System.out.println("Disabled auto compaction on " + node.server + " ([all keyspaces and tables])");
            } else {
                System.out.println("Disabled auto compaction on " + node.server + " (" + args + ")");
            }
            return true;
        }
    }
}
