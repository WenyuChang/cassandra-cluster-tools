package com.wenyu.clustertools;

import com.wenyu.utils.AsyncTask;
import com.wenyu.utils.ClusterToolNodeProbe;
import com.wenyu.utils.Constants;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by wenyu on 3/21/17.
 */
@Command(name = "version", description = "Print cassandra version")
public class Version extends ClusterToolCmd
{
    @Option(title = "parallel executor", name = {"-p", "--par-jobs"}, description = "Number of threads to get timeout of all nodes.")
    private int parallel = 1;

    @Override
    public void execute() {
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
                String result = "ReleaseVersion of " + node.server + ": " + probe.getReleaseVersion();
                return result;
            } catch (Exception e) {
                String error = String.format("%s failed with error: %s", node.server, e.toString());
                return error;
            }
        }
    }
}