package org.test;

import org.apache.shardingsphere.sql.parser.api.CacheOption;
import org.apache.shardingsphere.sql.parser.api.SQLParserEngine;
import org.apache.shardingsphere.sql.parser.api.SQLVisitorEngine;
import org.apache.shardingsphere.sql.parser.core.ParseContext;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.stream.Collectors;

public class MySqlTest {
    
    protected Long passed = 0L;
    
    protected Long total = 0L;
    
    private String delimiter = ";";
    
    private boolean isPerlSkip = false;
    
    private final StringBuilder multiLineSql = new StringBuilder();
    
    private final CacheOption cacheOption = new CacheOption(128, 1024L, 4);
    private final SQLParserEngine parserEngine = new SQLParserEngine("MySQL", cacheOption, false);
    private final SQLVisitorEngine visitorEngine = new SQLVisitorEngine("MySQL", "STATEMENT", new Properties());
    
    void test() throws IOException {
        testSQLParse(Paths.get("src/main/resources/mysql"));
//        textSqlParseEachFile(Paths.get("src/main/resources/mysql/events_restart.test"));
        System.out.printf("MySQL Test passed amount %d, total amount %d, Passing rate is %s", passed, total, new BigDecimal(passed).divide(new BigDecimal(total), 5, RoundingMode.HALF_UP));
    }
    
    protected void testSQLParse(final Path path) throws IOException {
        for (Path each : Files.list(path).collect(Collectors.toList())) {
            if (each.getFileName().toString().endsWith(".test")) {
                textSqlParseEachFile(each);
            }
        }
    }
    
    private void textSqlParseEachFile(final Path each) throws IOException {
        try {
            BufferedReader reader = Files.newBufferedReader(each);
            String line;
            while (null != (line = reader.readLine())) {
                line = line.trim();
                if (handlePerl(line) || handleComment(line) || handleEmptyLine(line) || handleMySqlTestCommand(line)) continue;
                handleThisLine(line);
                if (line.startsWith("delimiter") || line.startsWith("DELIMITER")) {
                    delimiter = line.substring("delimiter".length() + 1, "delimiter".length() + 2);
                }
            }
        } catch (Exception e) {
            System.out.println("wrong charset" + each.getFileName().toString());
        }
    }
    
    private void handleThisLine(final String line) throws IOException {
        if (line.endsWith(delimiter)) {
            if (multiLineSql.length() == 0) {
                testParse(fixDelimiter(line.substring(0, line.lastIndexOf(delimiter))));
            } else {
                multiLineSql.append(" ").append(line);
                testParse(fixDelimiter(multiLineSql.substring(0, multiLineSql.lastIndexOf(delimiter))));
                multiLineSql.setLength(0);
            }
        } else {
            if (multiLineSql.length() == 0) {
                multiLineSql.append(line);
            } else {
                multiLineSql.append(" ").append(line);
            }
        }
    }
    
    private boolean handleMySqlTestCommand(final String line) {
        return line.startsWith("connection") || line.startsWith("let") || line.startsWith("eval");
    }
    
    private boolean handleEmptyLine(final String line) {
        return line.equals("");
    }
    
    private boolean handleComment(final String line) {
        return line.startsWith("--") || line.startsWith("#");
    }
    
    private boolean handlePerl(final String line) {
        if (line.startsWith("--perl")) {
            isPerlSkip = true;
        }
        if (line.startsWith("EOF")) {
            isPerlSkip = false;
            return true;
        }
        return isPerlSkip;
    }
    
    private String fixDelimiter(final String line) {
        if ((!line.endsWith("*/") && !line.endsWith("//")) 
                && (line.endsWith("\\") || line.endsWith("$") || line.endsWith("/"))) {
            return line.substring(0, line.length() - 1);
        }
        return line;
    }
    
    private void testParse(final String line) throws IOException {
        total++;
        try {
            ParseContext parseContext = parserEngine.parse(line, false);
            SQLStatement sqlStatement = visitorEngine.visit(parseContext);
            passed++;
        } catch (Exception e) {
            writeErrorSqlIntoFile(line);
        }
    }
    
    protected void writeErrorSqlIntoFile(final String sql) throws IOException {
        Path path = Paths.get("src/main/resources/result/mySql.txt");
        Files.write(path, (sql.trim() + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
    }
}
