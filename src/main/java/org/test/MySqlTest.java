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
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;

public class MySqlTest {
    
    protected Long passed = 0L;
    
    protected Long total = 0L;
    
    private String delimiter = ";";
    
    private boolean isPerlSkip = false;
    
    private boolean isErrorParseTest = false;
    
    private int leftBigParantheses = 0;
    
    private int rightBigParantheses = 0;
    
    private boolean isFlowControl = false;
    
    private final StringBuilder multiLineSql = new StringBuilder();
    
    private final CacheOption cacheOption = new CacheOption(128, 1024L, 4);
    private final SQLParserEngine parserEngine = new SQLParserEngine("MySQL", cacheOption, false);
    private final SQLVisitorEngine visitorEngine = new SQLVisitorEngine("MySQL", "STATEMENT", new Properties());
    
    void test() throws IOException {
        testSQLParse(Paths.get("src/main/resources/mysql"));
//        textSqlParseEachFile(Paths.get("src/main/resources/mysql/subquery_sj_firstmatch.test"));
        System.out.printf("MySQL Test passed amount %d, total amount %d, Passing rate is %s", passed, total, new BigDecimal(passed).divide(new BigDecimal(total), 5, RoundingMode.HALF_UP));
    }
    
    protected void testSQLParse(final Path path) throws IOException {
        for (Path each : Files.list(path).collect(Collectors.toList())) {
            if (each.getFileName().toString().endsWith(".test")) {
                textSqlParseEachFile(each);
                cleanState();
            }
        }
    }
    
    private void cleanState() {
        delimiter = ";";
        isPerlSkip = false;
        isErrorParseTest = false;
        leftBigParantheses = 0;
        rightBigParantheses = 0;
        isFlowControl = false;
    }
    
    private void textSqlParseEachFile(final Path each) throws IOException {
        try {
            BufferedReader reader = Files.newBufferedReader(each);
            String line;
            while (null != (line = reader.readLine())) {
                if (handleEmptyLine(line.trim())) continue;
                if (handlePerl(line.trim())) continue;
                if (handleErrorParseTest(line.trim())) continue;
                if (handleComment(line.trim())) continue;
                if (handleFlowControl(line)) continue;
                line = line.trim();
                line = handTestCommandSubString(line);
                if (line.isBlank()) continue;
                handleThisLine(line);
                if (line.startsWith("delimiter") || line.startsWith("DELIMITER")) {
                    delimiter = line.substring("delimiter".length() + 1, line.contains(delimiter) ? line.indexOf(delimiter) : delimiter.length() + 2);
                }
            }
        } catch (Exception e) {
            System.out.println("wrong charset" + each.getFileName().toString());
        }
    }
    
    private String handTestCommandSubString(final String line) {
        if (line.startsWith("query_vertical ")) {
            return line.substring(14).trim();
        }
        if (line.toLowerCase().startsWith("send")) {
            return line.substring(4).trim();
        }
        return line;
    }
    
    private boolean handleFlowControl(final String line) throws IOException {
        if (!isFlowControl && (line.startsWith("if") || line.startsWith("while"))) {
            isFlowControl = true;
            judgeIsFlowControlEnd(line);
            writeSkipSqlIntoFile(line);
            return true;
        }
        if (isFlowControl) {
            judgeIsFlowControlEnd(line);
            writeSkipSqlIntoFile(line);
            return true;
        }
        return false;
    }
    
    private void judgeIsFlowControlEnd(final String line) {
        leftBigParantheses += line.length() - line.replace("{", "").length();
        rightBigParantheses += line.length() - line.replace("}", "").length();
        if (leftBigParantheses != 0 && leftBigParantheses == rightBigParantheses) {
            isFlowControl = false;
            leftBigParantheses = 0;
            rightBigParantheses = 0;
        }
    }
    
    private boolean handleErrorParseTest(final String line) throws IOException {
        if (line.startsWith("--error ER_PARSE_ERROR")) {
            isErrorParseTest = true;
            writeSkipSqlIntoFile(line);
            return true;
        }
        return false;
    }
    
    private void handleThisLine(final String line) throws IOException {
        if (line.endsWith(delimiter)) {
            if (multiLineSql.length() == 0) {
                String sql = line.substring(0, line.lastIndexOf(delimiter));
                if (handleNeedSkip(sql)) { 
                    return;
                }
                testParse(sql);
            } else {
                multiLineSql.append(" ").append(line);
                String sql = multiLineSql.substring(0, multiLineSql.lastIndexOf(delimiter));
                if (handleNeedSkip(sql)) {
                    multiLineSql.setLength(0);
                    return;
                }
                testParse(sql);
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
    
    private boolean handleNeedSkip(final String line) throws IOException {
        if (isErrorParseTest) {
            isErrorParseTest = false;
            writeSkipSqlIntoFile(line);
            return true;
        }
        if (line.startsWith("/*") && line.endsWith("*/")) return true;
        if (line.startsWith("!include ") || line.startsWith("!includedir")) return true;
        if (line.toLowerCase().startsWith("help ")) return true;
        if (line.startsWith("garbage")) return true;
        if (line.startsWith("TYPE=")) return true;
        if (line.startsWith("[auto] ") || line.startsWith("[client] ") || line.startsWith("[mysqld] ") || line.startsWith("[mysqlpump] ") || line.startsWith("[mysqltest1] ")) return true;
        if (line.startsWith("query_attributes")) return true;
        return handleMySqlTestCommand(line);
    }
    
    private boolean handleMySqlTestCommand(final String line) throws IOException {
        boolean result = line.toLowerCase().startsWith("connection") || line.toLowerCase().startsWith("let") || line.toLowerCase().startsWith("eval") || line.toLowerCase().startsWith("disconnect") || line.toLowerCase().startsWith("connect ") || line.startsWith("connect(") || line.startsWith("source")
                || line.toLowerCase().startsWith("echo") || line.startsWith("remove_file") || line.toLowerCase().startsWith("reap") || line.toLowerCase().startsWith("disable_query_log") || line.toLowerCase().startsWith("enable_query_log") || line.toLowerCase().startsWith("dec ")
                || line.startsWith("cat_file") || line.startsWith("change_user") || line.startsWith("copy_file") || line.startsWith("diff_files") || line.startsWith("dirty_close") || line.startsWith("disable_connect_log") || line.startsWith("enable_connect_log") || line.startsWith("disable_info")
                || line.startsWith("enable_info") || line.startsWith("disable_reconnect") || line.startsWith("enable_reconnect") || line.startsWith("disable_result_log") || line.startsWith("enable_result_log") || line.startsWith("disable_warnings") || line.startsWith("enable_warnings")
                || line.startsWith("file_exists") || line.startsWith("force-rmdir") || line.startsWith("inc") || line.startsWith("mkdir") || line.startsWith("replace_regex") || line.startsWith("replace_result") || line.startsWith("reset_connection") || line.startsWith("sleep") 
                || line.startsWith("write_file") || line.toLowerCase().startsWith("exec");
        if (result) {
            writeSkipSqlIntoFile(line);
        }
        return result;
    }
    
    private boolean handleEmptyLine(final String line) {
        return line.equals("");
    }
    
    private boolean handleComment(final String line) throws IOException {
        boolean result = line.startsWith("--") || line.startsWith("#");
        if (result) {
            writeSkipSqlIntoFile(line);
        }
        return result;
    }
    
    private boolean handlePerl(final String line) throws IOException {
        if (line.startsWith("--perl") || line.startsWith("perl")) {
            isPerlSkip = true;
            writeSkipSqlIntoFile(line);
        }
        if (line.startsWith("EOF")) {
            isPerlSkip = false;
            writeSkipSqlIntoFile(line);
            return true;
        }
        if (isPerlSkip) {
            writeSkipSqlIntoFile(line);
        }
        return isPerlSkip;
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
    
    protected void writeErrorSqlIntoFile(String sql) throws IOException {
        sql = sql.trim();
        // TODO delete
        if(sql.toLowerCase().startsWith("create ") || sql.toLowerCase().startsWith("select ") || sql.toLowerCase().startsWith("insert ") || sql.toLowerCase().startsWith("update ") || sql.toLowerCase().startsWith("delete ")) {
            return;
        }
        Path path = Paths.get("src/main/resources/result/mySql.txt");
        Files.write(path, (sql + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
    }
    
    protected void writeSkipSqlIntoFile(final String sql) throws IOException {
        Path path = Paths.get("src/main/resources/skip/skip.txt");
        Files.write(path, (sql.trim() + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
    }
}
