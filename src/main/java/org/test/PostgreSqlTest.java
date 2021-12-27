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

public class PostgreSqlTest {
    
    protected Long passed = 0L;
    
    protected Long total = 0L;
    
    final private CacheOption cacheOption = new CacheOption(128, 1024L, 4);
    final private SQLParserEngine parserEngine = new SQLParserEngine("PostgreSQL", cacheOption, false);
    final private SQLVisitorEngine visitorEngine = new SQLVisitorEngine("PostgreSQL", "STATEMENT", new Properties());
    
    void test() throws IOException {
        testSQLParse(Paths.get("src/main/resources/postgresql"));
        System.out.printf("PostgreSql Test passed amount %d, total amount %d, Passing rate is %s", passed, total, new BigDecimal(passed).divide(new BigDecimal(total), 5, RoundingMode.HALF_UP));
    }
    
    protected void testSQLParse(final Path path) throws IOException {
        for (Path each : Files.list(path).collect(Collectors.toList())) {
            if (each.getFileName().toString().endsWith(".sql")) {
                testPerFile(each);
            }
        }
    }
    
    private void testPerFile(final Path each) throws IOException {
        BufferedReader bufferedReader = Files.newBufferedReader(each);
        String line;
        StringBuilder stringBuilder = new StringBuilder();
        boolean needSkip = false;
        boolean isDoubleDollar = false;
        while ((line = bufferedReader.readLine()) != null) {
            if (line.startsWith("DO $x$")) {
                needSkip = true;
            }
            if (line.startsWith("$x$")) {
                needSkip = false;
                continue;
            }
            if (needSkip) {
                continue;
            }
            if (line.trim().startsWith("--") || line.trim().startsWith("#") || line.trim().equals("") || line.trim().startsWith("\\d+") || line.trim().startsWith("\\d") || line.trim().startsWith("\\!")) {
                continue;
            }
            if (line.contains("$$") && line.indexOf("$$") == line.lastIndexOf("$$") && !isDoubleDollar) {
                isDoubleDollar = true;
                stringBuilder.append(line);
                continue;
            }
            if (line.contains("$$") && line.indexOf("$$") == line.lastIndexOf("$$") && isDoubleDollar) {
                isDoubleDollar = false;
            }
            if (isDoubleDollar) {
                stringBuilder.append(" ").append(line);
                continue;
            }
            if (line.endsWith(";")) {
                if (stringBuilder.length() == 0) {
                    testParse(line);
                } else {
                    stringBuilder.append(" ").append(line);
                    testParse(stringBuilder.toString());
                    stringBuilder.setLength(0);
                }
            } else {
                if (stringBuilder.length() == 0) {
                    stringBuilder.append(line);
                } else {
                    stringBuilder.append(" ").append(line);
                }

            }
        }
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
        Path path = Paths.get("src/main/resources/result/postgreSql.txt");
        Files.write(path,(sql+"\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
    }
}
