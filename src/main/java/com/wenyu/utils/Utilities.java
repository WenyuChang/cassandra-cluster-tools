package com.wenyu.utils;

import org.apache.cassandra.tools.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Iterables.toArray;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;

/**
 * Created by wenyu on 2/28/17.
 */
public class Utilities {

    public static List<String> parseOptionalKeyspace(List<String> cmdArgs, ClusterToolNodeProbe nodeProbe, KeyspaceSet defaultKeyspaceSet) {
        List<String> keyspaces = new ArrayList<>();


        if (cmdArgs == null || cmdArgs.isEmpty()) {
            if (defaultKeyspaceSet == KeyspaceSet.NON_LOCAL_STRATEGY)
                keyspaces.addAll(keyspaces = nodeProbe.getNonLocalStrategyKeyspaces());
            else if (defaultKeyspaceSet == KeyspaceSet.NON_SYSTEM)
                keyspaces.addAll(keyspaces = nodeProbe.getNonSystemKeyspaces());
            else
                keyspaces.addAll(nodeProbe.getKeyspaces());
        } else {
            keyspaces.add(cmdArgs.get(0));
        }

        for (String keyspace : keyspaces) {
            if (!nodeProbe.getKeyspaces().contains(keyspace))
                throw new IllegalArgumentException("Keyspace [" + keyspace + "] does not exist.");
        }

        return Collections.unmodifiableList(keyspaces);
    }

    public static String[] parseOptionalTables(List<String> cmdArgs) {
        return cmdArgs.size() <= 1 ? EMPTY_STRING_ARRAY : toArray(cmdArgs.subList(1, cmdArgs.size()), String.class);
    }
}
