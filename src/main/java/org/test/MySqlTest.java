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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class MySqlTest {
    
    protected Long passed = 0L;
    
    protected Long total = 0L;
    
    private final CacheOption cacheOption = new CacheOption(128, 1024L, 4);
    private final SQLParserEngine parserEngine = new SQLParserEngine("MySQL", cacheOption, false);
    private final SQLVisitorEngine visitorEngine = new SQLVisitorEngine("MySQL", "STATEMENT", new Properties());
    
    void test() throws IOException {
        testSQLParse(Paths.get("src/main/resources/mysql"));
//        textSqlParseEachFile(Paths.get("src/main/resources/mysql/partition_not_supported.test"));
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
        String delimiter = ";";
        boolean isPerlSkip = false;
        try {
            BufferedReader reader = Files.newBufferedReader(each);
            String line;
            StringBuilder stringBuilder = new StringBuilder();
            while (null != (line = reader.readLine())) {
                if (line.startsWith("--perl")) {
                    isPerlSkip = true;
                }
                if (line.startsWith("EOF")) {
                    isPerlSkip = false;
                    continue;
                }
                if (isPerlSkip) {
                    continue;
                }
                if (line.trim().startsWith("--") || line.trim().startsWith("#") || line.trim().equals("")) {
                    continue;
                }
                if (line.endsWith(delimiter)) {
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
    
                if (line.startsWith("delimiter")) {
                    delimiter = line.substring("delimiter".length() + 1, "delimiter".length() + 2);
                }
            }
        } catch (Exception e) {
            System.out.println("wrong carset" + each.getFileName().toString());
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
        Path path = Paths.get("src/main/resources/result/mySql.txt");
        Files.write(path, (sql + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
    }
}
