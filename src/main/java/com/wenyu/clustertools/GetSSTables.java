package com.wenyu.clustertools;

import com.wenyu.utils.AsyncTask;
import com.wenyu.utils.ClusterToolNodeProbe;
import com.wenyu.utils.Constants;
import com.wenyu.utils.TableGenerator;
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

/**
 * Created by wenyu on 3/21/17.
 */
@Command(name = "getsstables", description = "Print the server and sstable filenames that own the key")
public class GetSSTables extends ClusterToolCmd {
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

        Map<Node, Future<List<List<String>>>> futures = new HashMap<>();
        for (InetAddress endpoint : endpoints) {
            if (!nodeMap.containsKey(endpoint)) {
                System.out.println(endpoint.getHostAddress() + ": ? (Don't have current node JMX information)");
                continue;
            }
            Node node = nodeMap.get(endpoint);
            futures.put(node, executor.submit(new Executor(node, ks, cf, key)));
        }

        List<String> header = new ArrayList<String>() {{
            add("Server");
            add("SSTables");
        }};

        List<List<String>> rows = new ArrayList<>();
        for (Map.Entry<ClusterToolCmd.Node, Future<List<List<String>>>> future : futures.entrySet()) {
            try {
                rows.addAll(future.getValue().get(Constants.MAX_PARALLEL_WAIT_IN_SEC, TimeUnit.SECONDS));
            } catch (Exception ex) {
                System.out.println(String.format("%s failed with error: %s", future.getKey().server, ex.toString()));
                ex.printStackTrace();
            }
        }

        System.out.println(TableGenerator.generateTable(header, rows));
    }

    private ClusterToolNodeProbe getNodeProbe() {
        int nodeCount = nodes.size();
        int randomNode = new Random().nextInt(nodeCount);
        Node nodeToBeConnect = nodes.get(randomNode);
        ClusterToolNodeProbe nodeProbe = connect(nodeToBeConnect);
        return nodeProbe;
    }

    private class Executor extends AsyncTask<List<List<String>>> {
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
        public List<List<String>> execute() {
            ClusterToolNodeProbe probe = connect(node);

            try {
                List<String> sstables = probe.getSSTables(ks, cf, key, false);
                List<List<String>> rows = new ArrayList<>();

                for (String sstable : sstables) {
                    List<String> row = new ArrayList<String>() {{
                        add(node.server);
                    }};
                    row.add(sstable);
                    rows.add(row);
                }

                return rows;
            } catch (Exception e) {
                String error = String.format("%s failed with error: %s", node.server, e.toString());
                throw e;
            }
        }
    }
}
