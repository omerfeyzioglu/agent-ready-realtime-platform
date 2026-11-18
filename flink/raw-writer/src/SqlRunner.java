import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Runs a Flink SQL file as one streaming job. DDL statements run one by
 * one; all INSERT statements go into a single statement set, so the source
 * is read once and feeds every sink.
 */
public class SqlRunner {
    public static void main(String[] args) throws Exception {
        String script = Files.readString(Path.of(args[0]));
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        StatementSet inserts = env.createStatementSet();

        for (String statement : split(script)) {
            if (statement.regionMatches(true, 0, "INSERT", 0, 6)) {
                inserts.addInsertSql(statement);
            } else {
                env.executeSql(statement);
            }
        }
        inserts.execute();
    }

    /** Splits on ';' at the end of a line, dropping full-line comments. */
    static List<String> split(String script) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (String line : script.split("\n")) {
            if (line.strip().startsWith("--")) {
                continue;
            }
            current.append(line).append('\n');
            if (line.strip().endsWith(";")) {
                String s = current.toString().strip();
                statements.add(s.substring(0, s.length() - 1));
                current.setLength(0);
            }
        }
        if (!current.toString().isBlank()) {
            statements.add(current.toString().strip());
        }
        return statements;
    }
}
