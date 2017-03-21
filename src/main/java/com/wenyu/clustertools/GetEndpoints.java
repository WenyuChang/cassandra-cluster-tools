package com.wenyu.clustertools;

import com.wenyu.utils.ClusterToolNodeProbe;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by wenyu on 3/21/17.
 */
@Command(name = "getendpoints", description = "Print the end points that owns the key")
public class GetEndpoints extends ClusterToolCmd {
    @Arguments(usage = "<keyspace> <cfname> <key>", description = "The keyspace, the column family, and the partition key for which we need to find the endpoint")
    private List<String> args = new ArrayList<>();

    @Option(title = "parallel executor", name = {"-p", "--par-jobs"}, description = "Number of threads to get timeout of all nodes.")
    private int parallel = 1;

    @Override
    public void execute() {
        ClusterToolNodeProbe probe = getNodeProbe();

        checkArgument(args.size() == 3, "getendpoints requires ks, cf and key args");
        String ks = args.get(0);
        String cf = args.get(1);
        String key = args.get(2);

        List<InetAddress> endpoints = probe.getEndpoints(ks, cf, key);
        for (InetAddress endpoint : endpoints) {
            System.out.println(endpoint.getHostAddress());
        }
    }

    private ClusterToolNodeProbe getNodeProbe() {
        int nodeCount = nodes.size();
        int randomNode = new Random().nextInt(nodeCount);
        Node nodeToBeConnect = nodes.get(randomNode);
        ClusterToolNodeProbe nodeProbe = connect(nodeToBeConnect);
        return nodeProbe;
    }
}
