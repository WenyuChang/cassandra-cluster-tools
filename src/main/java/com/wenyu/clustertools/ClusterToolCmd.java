package com.wenyu.clustertools;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;

import com.google.common.base.Throwables;
import io.airlift.command.*;
import com.wenyu.utils.ClusterToolNodeProbe;;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by wenyu on 2/24/17.
 */
public abstract class ClusterToolCmd implements Runnable {
    @Option(type = OptionType.GLOBAL, name = {"-nf", "--nodes-file"},
            description = "Path to the nodes connection information file")
    private String nodesConnectionFile = EMPTY;

    @Option(type = OptionType.GLOBAL, name = {"-ni", "--node-info"},
            description = "The host to connect to. eg: cass01/user:password")
    private String host = EMPTY;

    protected List<Node> nodes;

    private void parseNodesConnectionFile() {
        nodes = new ArrayList<Node>();

        if (!StringUtils.isEmpty(host)) {
            // Read single host from pass-in value
            connectToNode();
        } else if (!StringUtils.isEmpty(nodesConnectionFile)) {
            // No single host is specified, then read nodes connection file
            readConnectionsFile();
        } else {
            // Set default, and connection to local
            nodes.add(new Node());
            return;
        }
    }

    private void connectToNode() {
        try {
            String jmxRole = host;
            String serverRole = jmxRole.split("/")[0];
            String authRole = EMPTY;
            if (jmxRole.split("/").length > 1) {
                authRole = jmxRole.split("/")[1];
            }

            // Make sure server information is correctly formatted
            assert serverRole.length() > 0;

            Node node = new Node();
            node.server = serverRole.split(":")[0];
            if (serverRole.split(":").length > 1) {
                node.port = serverRole.split(":")[1];
            }

            if (!EMPTY.equals(authRole)) {
                node.username = authRole.split(":")[0];
                node.password = authRole.split(":")[1];
            }

            nodes.add(node);
        } catch (AssertionError e) {
            throw e;
        }
    }

    private void readConnectionsFile() {
        try {
            Set<InetAddress> nodeSet = new HashSet<>();
            File nodesFile = new File(nodesConnectionFile);
            Scanner scanner = new Scanner(nodesFile).useDelimiter(System.lineSeparator());
            while (scanner.hasNext())
            {
                String jmxRole = scanner.next();
                String serverRole = jmxRole.split("/")[0];
                String authRole = EMPTY;
                if (jmxRole.split("/").length > 1) {
                    authRole = jmxRole.split("/")[1];
                }

                // Make sure server information is correctly formatted
                assert serverRole.length() > 0;

                InetAddress add = InetAddress.getByName(serverRole.split(":")[0]);
                if (nodeSet.contains(add)) {
                    continue;
                } else {
                    nodeSet.add(add);
                }

                Node node = new Node();
                node.server = serverRole.split(":")[0];
                if (serverRole.split(":").length > 1) {
                    node.port = serverRole.split(":")[1];
                }

                if (!EMPTY.equals(authRole)) {
                    node.username = authRole.split(":")[0];
                    node.password = authRole.split(":")[1];
                }

                nodes.add(node);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (AssertionError e) {
            throw e;
        } catch (UnknownHostException e) {
            // TODO Unknow Host Exception handling
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            parseNodesConnectionFile();
            if (nodes == null || nodes.size() <= 0) {
                return;
            }

            execute();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public abstract void execute();

    protected ClusterToolNodeProbe connect(Node node)
    {
        ClusterToolNodeProbe nodeClient = null;

        try {
            if (node.username.isEmpty())
                nodeClient = new ClusterToolNodeProbe(node.server, parseInt(node.port));
            else
                nodeClient = new ClusterToolNodeProbe(node.server, parseInt(node.port), node.username, node.password);

        } catch (IOException | SecurityException e) {
            Throwable rootCause = Throwables.getRootCause(e);
            System.err.println(format("nodetool: Failed to connect to '%s:%s' - %s: '%s'.", node.server, node.port, rootCause.getClass().getSimpleName(), rootCause.getMessage()));
            System.exit(1);
        }

        return nodeClient;
    }

    public static class Node {
        protected String server = "127.0.0.1";
        protected String port = "7199";
        protected String username = EMPTY;
        protected String password = EMPTY;
    }
}
