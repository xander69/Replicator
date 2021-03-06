package ru.xander.replicator.action;

import ru.xander.replicator.exception.ReplicatorException;
import ru.xander.replicator.schema.BatchExecutor;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.SchemaConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Objects;

/**
 * @author Alexander Shakhov
 */
public class PumpAction implements Action {

    private final SchemaConfig schemaConfig;
    private final File[] scriptFiles;

    public PumpAction(SchemaConfig schemaConfig, File[] scriptFiles) {
        Objects.requireNonNull(schemaConfig, "Configure schema");
        Objects.requireNonNull(scriptFiles, "Script files");
        if (scriptFiles.length == 0) {
            throw new IllegalArgumentException("At least one script file must be specified for pump");
        }
        this.schemaConfig = schemaConfig;
        this.scriptFiles = scriptFiles;
    }

    public void execute() {
        withSchema(schemaConfig, schema -> {
            for (File scriptFile : scriptFiles) {
                pumpScript(schema, scriptFile);
            }
        });
    }

    private void pumpScript(Schema target, File scriptFile) {
        long fileSize = scriptFile.length();
        long verboseStep = 1024L * 1024L;//(long) (fileSize / 1000.0d);
        int lineNumber = 0;
        long readBytes = 0;
        long readBatch = 0;
        try (
                BufferedReader bufferedReader = new BufferedReader(new FileReader(scriptFile));
                BatchExecutor batchExecutor = target.createBatchExecutor()
        ) {
            String line;
            StringBuilder statement = new StringBuilder();
            boolean script = false;
            while ((line = bufferedReader.readLine()) != null) {
                int lineSize = line.length();
                readBytes += (lineSize + 1);
                readBatch += (lineSize + 1);
                lineNumber++;
                if (lineSize == 0) {
                    continue;
                }

                if (!script) {
                    script = line.startsWith("BEGIN");
                }

                boolean endStatement;
                if (script) {
                    endStatement = line.endsWith("END;");
                } else {
                    endStatement = line.charAt(lineSize - 1) == ';';
                }

                if (endStatement) {
                    if (script) {
                        statement.append(line);
                    } else {
                        statement.append(line, 0, line.length() - 1);
                    }
                    batchExecutor.execute(statement.toString());
                    statement.setLength(0);
                    script = false;
                } else {
                    statement.append(line).append('\n');
                }

                if (readBatch >= verboseStep) {
                    //TODO:
//                    listener.progress(new Progress(readBytes, fileSize, "Pump script " + scriptFile.getName() + " to target"));
                    readBatch = 0;
                }
            }
            batchExecutor.finish();
        } catch (Exception e) {
            String errorMessage = "Failed to pump script " + scriptFile.getAbsolutePath() + " at line " + lineNumber + ": " + e.getMessage();
            throw new ReplicatorException(errorMessage, e);
        }
    }
}
