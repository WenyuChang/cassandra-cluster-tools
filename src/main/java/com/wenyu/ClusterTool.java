package com.wenyu;

import com.wenyu.clustertools.*;
import io.airlift.command.*;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;


/**
 * Created by wenyu on 2/24/17.
 */
public class ClusterTool {

    private static List<Class<? extends Runnable>> commands() {
        List<Class<? extends Runnable>> commands = new ArrayList<>();

        commands.add(Help.class);
        commands.add(Status.class);
        commands.add(Ring.class);
        commands.add(GetTimeout.class);
        commands.add(GetCompactionThroughput.class);
        commands.add(GetCompactionThreshold.class);
        commands.add(GetConcurrentCompactors.class);
        commands.add(Flush.class);
        commands.add(ClearSnapshot.class);
        commands.add(Cleanup.class);

        commands.add(Drain.class);
        commands.add(GarbageCollect.class);
        commands.add(Snapshot.class);
        commands.add(SetLoggingLevel.class);
        commands.add(GetLoggingLevels.class);

        commands.add(CompactPartition.class);
        commands.add(GetEndpoints.class);
        commands.add(GetSSTables.class);
        commands.add(Version.class);
        commands.add(EnableAutoCompaction.class);
        commands.add(DisableAutoCompaction.class);

        commands.add(CompactionStats.class);
        commands.add(DescribeCluster.class);
        commands.add(GcStats.class);

        return commands;
    }

    public static void main(String[] args) {
        Cli.CliBuilder<Runnable> builder = Cli.builder("clustertool");

        builder.withDescription("Manage your Cassandra cluster")
                .withDefaultCommand(Help.class)
                .withCommands(commands());
        Cli<Runnable> parser = builder.build();

        int status = 0;
        try
        {
            Runnable parse = parser.parse(args);
            parse.run();
        } catch (IllegalArgumentException |
                IllegalStateException |
                ParseArgumentsMissingException |
                ParseArgumentsUnexpectedException |
                ParseOptionConversionException |
                ParseOptionMissingException |
                ParseOptionMissingValueException |
                ParseCommandMissingException |
                ParseCommandUnrecognizedException e)
        {
            status = 1;
            e.printStackTrace();
        } catch (Throwable throwable)
        {
            status = 2;
            throwable.printStackTrace();
        }

        System.exit(status);
    }
}
