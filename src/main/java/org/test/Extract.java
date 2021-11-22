package org.test;


import org.apache.shardingsphere.sql.parser.api.SQLParserEngine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Extract {
    public static void main(String[] args) throws IOException {
        Path path = Paths.get("/Users/chenchuxin/Documents/GitHub/extract-sql-for-shardingsphere-parser-test/src/main/resources/sql/aggregates.sql");
        final String s = Files.readString(path);
        String[] sqls = s.split(";");
        SQLParserEngine parserEngine = new SQLParserEngine("PostgreSQL", false);
        for (String sql : sqls) {
            try {
                parserEngine.parse(sql, false);
            } catch (Exception e) {
                System.out.println("error: " + sql);
            }
            
        }
        
        
    }
}
