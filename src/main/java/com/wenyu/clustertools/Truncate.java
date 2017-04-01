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

import com.wenyu.utils.ClusterToolNodeProbe;
import io.airlift.command.Arguments;
import io.airlift.command.Command;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "truncate", description = "Truncates (deletes) the given table from the provided keyspace")
public class Truncate extends ClusterToolCmd {
    @Arguments(usage = "<keyspace> <cfname>", description = "The keyspace, the column family for which we need to truncate")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute() {
        checkArgument(args.size() == 2, "truncate requires ks and cf args");

        String ks = args.get(0);
        String cf = args.get(1);
        ClusterToolNodeProbe probe = getNodeProbe();
        probe.truncate(ks, cf);
        System.out.println("Finished truncating " + ks + "." + cf);
    }

    private ClusterToolNodeProbe getNodeProbe() {
        int nodeCount = nodes.size();
        int randomNode = new Random().nextInt(nodeCount);
        Node nodeToBeConnect = nodes.get(randomNode);
        ClusterToolNodeProbe nodeProbe = connect(nodeToBeConnect);
        return nodeProbe;
    }
}
