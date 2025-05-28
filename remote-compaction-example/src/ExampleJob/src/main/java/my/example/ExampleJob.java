package my.example;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class ExampleJob {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleJob.class);

    private static Configuration getConfiguration(ParameterTool params) {
        Configuration configuration = new Configuration();
        configuration.addAll(params.getConfiguration());

        return configuration;
    }

    public static final ConfigOption<Boolean> EMBED_COMPACTION_SERVICE =
            ConfigOptions.key("embedCompactionService").booleanType().defaultValue(false);
    public static final ConfigOption<String> COMPACTION_SERVICE_ADDRESS =
            ConfigOptions.key("compactionServiceAddress").stringType().defaultValue("");

    public static void main(String[] args) throws Exception {
        String statebackend = "forst";
        Configuration configuration = new Configuration();
        String filePath = "sql_scripts/example.sql";
        InputStream inputStream = ExampleJob.class.getClassLoader().getResourceAsStream(filePath);
        if (inputStream == null) {
            System.out.println("File Not Found: " + filePath);
            return;
        }

        List<String> lines = new ArrayList<>();
        try (Scanner scanner = new Scanner(inputStream)) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                lines.add(line);
            }
        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        ParameterTool params = ParameterTool.fromArgs(args);
        Configuration configurationFromArgs = getConfiguration(params);
        String compactionServiceAddress = configurationFromArgs.get(COMPACTION_SERVICE_ADDRESS);
        env.getConfig()
                .setCompactionServiceAddress(compactionServiceAddress);
        LOG.info("compactionServiceAddress: {}", compactionServiceAddress);

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        executeSql(String.join("\n", lines), tEnv);
    }

    public static void executeSql(String sql, StreamTableEnvironment tableEnvironment) throws ExecutionException, InterruptedException {
        StatementSet statementSet = tableEnvironment.createStatementSet();
        Arrays.stream(sql.split(";\n"))
                .map(s -> s.replace('\n', ' '))
                .map(String::trim)
                .forEach(
                        s -> {
                            if (s.startsWith("END") || s.startsWith("BEGIN") || s.startsWith("begin") || s.startsWith("end")) {
                                return;
                            }
                            if (s.startsWith("INSERT") || s.startsWith("insert")) {
                                statementSet.addInsertSql(s);
                            } else {
                                tableEnvironment.executeSql(s);
                            }
                        });
        LOG.info(statementSet.explain());
        statementSet.execute().await();
    }
}
