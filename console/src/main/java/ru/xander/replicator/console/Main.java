package ru.xander.replicator.console;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.xander.replicator.DumpOptions;
import ru.xander.replicator.Replicator;
import ru.xander.replicator.Schema;
import ru.xander.replicator.SchemaFactory;
import ru.xander.replicator.SchemaOptions;

import java.io.File;
import java.io.FileOutputStream;

@SuppressWarnings("DuplicatedCode")
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final String PARAM_CMD = "cmd";
    private static final String PARAM_SRC_DRIVER = "srcDriver";
    private static final String PARAM_SRC_URL = "srcUrl";
    private static final String PARAM_SRC_USER = "srcUser";
    private static final String PARAM_SRC_PWD = "srcPwd";
    private static final String PARAM_SRC_SCHEMA = "srcSchema";
    private static final String PARAM_TRG_DRIVER = "trgDriver";
    private static final String PARAM_TRG_URL = "trgUrl";
    private static final String PARAM_TRG_USER = "trgUser";
    private static final String PARAM_TRG_PWD = "trgPwd";
    private static final String PARAM_TRG_SCHEMA = "trgSchema";
    private static final String PARAM_DUMP_TABLES = "dumpTables";

    private static final Options options;

    static {
        options = new Options();
        options.addOption(Option.builder(PARAM_CMD).required().hasArg()
                .argName("Команда").desc("Возможные значения: dump - снятие дампа").build());
        options.addOption(Option.builder(PARAM_SRC_DRIVER).required().hasArg()
                .argName("Драйвер источника").desc("Класс JDBC драйвера для схемы источника").build());
        options.addOption(Option.builder(PARAM_SRC_URL).required().hasArg()
                .argName("Адрес источника").desc("Строка подключения к схеме источника").build());
        options.addOption(Option.builder(PARAM_SRC_USER).required().hasArg()
                .argName("Пользователь источника").desc("Имя пользователя для подключения к схеме источника").build());
        options.addOption(Option.builder(PARAM_SRC_PWD).required().hasArg()
                .argName("Пароль источника").desc("Пароль для подключения к схеме источника").build());
        options.addOption(Option.builder(PARAM_SRC_SCHEMA).required().hasArg()
                .argName("Рабочая схема источника").desc("Рабочая схема источника").build());
        options.addOption(Option.builder(PARAM_TRG_DRIVER).required().hasArg()
                .argName("Драйвер приемника").desc("Класс JDBC драйвера для схемы приемника").build());
        options.addOption(Option.builder(PARAM_TRG_URL).required().hasArg()
                .argName("Адрес приемника").desc("Строка подключения к схеме приемника").build());
        options.addOption(Option.builder(PARAM_TRG_USER).required().hasArg()
                .argName("Пользователь приемника").desc("Имя пользователя для подключения к схеме приемника").build());
        options.addOption(Option.builder(PARAM_TRG_PWD).required().hasArg()
                .argName("Пароль приемника").desc("Пароль для подключения к схеме приемника").build());
        options.addOption(Option.builder(PARAM_TRG_SCHEMA).required().hasArg()
                .argName("Рабочая схема приемника").desc("Рабочая схема приемника").build());
        options.addOption(Option.builder(PARAM_DUMP_TABLES).hasArg()
                .argName("Таблицы для дампа").desc("Список таблицы, для которых будет выполняться команда dump (через запятую)").build());
    }

    private final CommandLine commandLine;

    private Main(String[] args) throws ParseException {
        commandLine = new DefaultParser().parse(options, args);
    }

    private void execute() throws Exception {
        String cmd = commandLine.getOptionValue(PARAM_CMD);
        if ("dump".equals(cmd)) {
            requireArgs(PARAM_DUMP_TABLES);
            dump();
        } else {
            throw new ParseException("Неизвестная операция: " + cmd);
        }
    }

    private void dump() throws Exception {
        SchemaOptions sourceSchemaOptions = createSourceSchemaOption();
        try (Schema sourceSchema = SchemaFactory.create(sourceSchemaOptions)) {
            Replicator replicator = new Replicator(sourceSchema, sourceSchema, new ConsoleListener(null));

            File outPath = new File(System.getProperty("user.dir"), "dumps");
            //noinspection ResultOfMethodCallIgnored
            outPath.mkdirs();

            String[] tables = commandLine.getOptionValue(PARAM_DUMP_TABLES).split(",");
            for (String table : tables) {
                String tableName = table.trim().toUpperCase();
                File dumpFile = new File(outPath, tableName + ".sql");
                try (FileOutputStream outputStream = new FileOutputStream(dumpFile)) {
                    log.info("Dump table {}", tableName);
                    replicator.dump(tableName, outputStream, new DumpOptions());
                    log.info("Dump saved to {}", dumpFile.getAbsolutePath());
                } catch (Exception e) {
                    throw new RuntimeException("Ошибка при создании дампа для таблицы " + tableName, e);
                }
            }
        }
    }

    private SchemaOptions createSourceSchemaOption() {
        SchemaOptions schemaOptions = new SchemaOptions();
        schemaOptions.setJdbcDriver(commandLine.getOptionValue(PARAM_SRC_DRIVER));
        schemaOptions.setJdbcUrl(commandLine.getOptionValue(PARAM_SRC_URL));
        schemaOptions.setUsername(commandLine.getOptionValue(PARAM_SRC_USER));
        schemaOptions.setPassword(commandLine.getOptionValue(PARAM_SRC_PWD));
        schemaOptions.setWorkSchema(commandLine.getOptionValue(PARAM_SRC_SCHEMA));
        schemaOptions.setListener(new ConsoleListener("SOURCE"));
        return schemaOptions;
    }

    private SchemaOptions createTargetSchemaOption() {
        SchemaOptions schemaOptions = new SchemaOptions();
        schemaOptions.setJdbcDriver(commandLine.getOptionValue(PARAM_TRG_DRIVER));
        schemaOptions.setJdbcUrl(commandLine.getOptionValue(PARAM_TRG_URL));
        schemaOptions.setUsername(commandLine.getOptionValue(PARAM_TRG_USER));
        schemaOptions.setPassword(commandLine.getOptionValue(PARAM_TRG_PWD));
        schemaOptions.setWorkSchema(commandLine.getOptionValue(PARAM_TRG_SCHEMA));
        schemaOptions.setListener(new ConsoleListener("TARGET"));
        return schemaOptions;
    }

    private void requireArgs(String... args) throws ParseException {
        for (String paramName : args) {
            if (!commandLine.hasOption(paramName)) {
                throw new ParseException("Необходимо указать параметр: " + paramName);
            }
        }
    }

    public static void main(String[] args) {
        try {
            log.info("---------------------------------------------------------------");
            new Main(args).execute();
        } catch (ParseException e) {
            printUsage();
            log.error("Неверные аргументы командной строки", e);
            e.printStackTrace(System.err);
            System.exit(1);
        } catch (Exception e) {
            log.error("Ошибка утилиты", e);
            System.exit(1);
        }
    }

    private static void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);
        formatter.setSyntaxPrefix("");
        formatter.printHelp("Параметры:", null, options, null);
    }
}
