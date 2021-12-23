package org.test;

import org.apache.shardingsphere.sql.parser.api.CacheOption;
import org.apache.shardingsphere.sql.parser.api.SQLParserEngine;
import org.apache.shardingsphere.sql.parser.api.SQLVisitorEngine;
import org.apache.shardingsphere.sql.parser.core.ParseContext;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.stream.Collectors;

public abstract class AbstractSqlTest {
    
    protected Long passed = 0L;
    
    protected Long total = 0L;
    
    protected void testSQLParse(final Path path, final String databaseType) throws IOException {
        for (Path each : Files.list(path).collect(Collectors.toList())) {
            if (each.getFileName().toString().endsWith(".sql") || each.getFileName().toString().endsWith(".test")) {
                String sqls = null;
                try {
                    sqls = Files.readString(each);
                } catch (Exception e) {
                    System.out.println("wrong carset" + each.getFileName().toString());
                }
                if (sqls != null) {
                    testParse(sqls, databaseType);
                }
            }
        }
    }
    
    private void testParse(final String text, final String databaseType) throws IOException {
        String[] sqls = text.split(";");
        CacheOption cacheOption = new CacheOption(128, 1024L, 4);
        SQLParserEngine parserEngine = new SQLParserEngine(databaseType, cacheOption, false);
        SQLVisitorEngine visitorEngine = new SQLVisitorEngine(databaseType, "STATEMENT", new Properties());
        for (String sql : sqls) {
            total++;
            if (databaseType.equals("MySQL")) {
                sql = sql.replace("--", "-- ");
            }
            try {
                ParseContext parseContext = parserEngine.parse(sql, false);
                SQLStatement sqlStatement = visitorEngine.visit(parseContext);
                passed++;
            } catch (Exception e) {
                writeErrorSqlIntoFile(sql);
            }
        }
    }
    
    protected abstract void writeErrorSqlIntoFile(final String sql) throws IOException;
}
