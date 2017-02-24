package com.wenyu;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import io.airlift.command.*;

/**
 * Created by wenyu on 2/24/17.
 */
public class ClusterToolCmd implements Runnable {
    @Option(type = OptionType.GLOBAL, name = {"-h", "--host"}, description = "Node hostname or ip address")
    private String host = "127.0.0.1";

    @Option(type = OptionType.GLOBAL, name = {"-p", "--port"}, description = "Remote jmx agent port number")
    private String port = "7199";

    @Option(type = OptionType.GLOBAL, name = {"-u", "--username"}, description = "Remote jmx agent username")
    private String username = EMPTY;

    @Option(type = OptionType.GLOBAL, name = {"-pw", "--password"}, description = "Remote jmx agent password")
    private String password = EMPTY;

    @Option(type = OptionType.GLOBAL, name = {"-pwf", "--password-file"}, description = "Path to the JMX password file")
    private String passwordFilePath = EMPTY;

    public void run() {

    }
}
