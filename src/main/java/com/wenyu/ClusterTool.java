package com.wenyu;

import com.wenyu.clustertools.GetTimeout;
import com.wenyu.clustertools.Ring;
import com.wenyu.clustertools.Status;
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
        } catch (Throwable throwable)
        {
            status = 2;
        }

        System.exit(status);
    }
}
