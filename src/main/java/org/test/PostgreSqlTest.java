package org.test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class PostgreSqlTest extends AbstractSqlTest {
    
    void test() throws IOException {
        testSQLParse(Paths.get("src/main/resources/postgresql"), "PostgreSQL");
        System.out.printf("PostgreSql Test passed amount %d, total amount %d, Passing rate is %s", passed, total, new BigDecimal(passed).divide(new BigDecimal(total), 5, RoundingMode.HALF_UP));
    }
    
    @Override
    protected void writeErrorSqlIntoFile(final String sql) throws IOException {
        Path path = Paths.get("src/main/resources/result/postgreSql.txt");
        Files.write(path,("\n" + sql+"\n================================").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
    }
}
