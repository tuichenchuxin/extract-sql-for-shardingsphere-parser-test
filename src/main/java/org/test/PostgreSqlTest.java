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
import java.util.HashSet;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PostgreSqlTest {
    
    protected Long passed = 0L;
    
    protected Long total = 0L;
    
    private String delimiter = ";";
    
    private HashSet<String> dollarSet = new HashSet<>();
    
    private HashSet<String> quotSet = new HashSet<>();
    
    private boolean docComment = false;
    
    private final StringBuilder stringBuilder = new StringBuilder();
    
    private final Pattern pattern = Pattern.compile("\\$[A-Za-z]*?\\$");
    
    private final Pattern quotPattern = Pattern.compile("'");
    
    private final Pattern commentPattern = Pattern.compile("\\/\\*|\\*\\/");
    
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
                clearStatus();
            }
        }
    }
    
    private void clearStatus() {
        dollarSet.clear();
        stringBuilder.setLength(0);
        quotSet.clear();
        docComment = false;
    }
    
    private void testPerFile(final Path each) throws IOException {
        BufferedReader bufferedReader = Files.newBufferedReader(each);
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            if(handleComment(line)) continue;
            if (handleEmptyLine(line)) continue;
            if (handleTestCommand(line)) continue;
            line = handleLastLineComment(line);
            handleDollar(line);
            handleQuot(line);
            handleDocComment(line);
            if (line.trim().endsWith(delimiter) && dollarSet.isEmpty() && quotSet.isEmpty() && !docComment) {
                if (stringBuilder.length() == 0) {
                    testParse(line);
                    clearStatus();
                } else {
                    stringBuilder.append(" ").append(line);
                    testParse(stringBuilder.toString());
                    clearStatus();
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
    
    private void handleDocComment(final String line) {
        Matcher matcher = commentPattern.matcher(line);
        while(matcher.find()) {
            docComment = !docComment;
        }
    }
    
    private String handleLastLineComment(final String line) {
        int index = line.trim().indexOf(" --");
        if (index > 0) {
            return line.substring(0, index);
        }
        return line;
    }
    
    private void handleQuot(final String line) {
        Matcher matcher = quotPattern.matcher(line);
        while (matcher.find()) {
            String s = line.substring(matcher.start(), matcher.end());
            if (quotSet.contains(s)) {
                quotSet.remove(s);
            } else {
                quotSet.add(s);
            }
        }
    }
    
    private boolean handleTestCommand(final String line) {
        return line.trim().startsWith("\\d+") || line.trim().startsWith("\\d") || line.trim().startsWith("\\!")
                || line.trim().startsWith("\\pset") || line.trim().startsWith("\\set") || line.trim().startsWith(":init_range_parted")
                || line.trim().startsWith(":show_data") || line.trim().startsWith("\\echo") || line.trim().startsWith("\\a\\t")
                || line.trim().startsWith("\\c") || line.trim().startsWith("\\copy") || line.startsWith("\\.")
                || line.trim().startsWith("\\z") || line.trim().startsWith("\\crosstabview") || line.trim().startsWith("\\p")
                || line.trim().startsWith("\\r") || line.trim().startsWith("\\if") || line.trim().startsWith("\\elif")
                || line.trim().startsWith("\\else") || line.trim().startsWith("\\endif");
    }
    
    private boolean handleEmptyLine(final String line) {
        return line.trim().equals("");
    }
    
    private boolean handleComment(final String line) {
        return line.trim().startsWith("--") || line.trim().startsWith("#");
    }
    
    private void handleDollar(final String line) {
        Matcher matcher = pattern.matcher(line);
        while (matcher.find()) {
            String s = line.substring(matcher.start(), matcher.end());
            if (dollarSet.contains(s)) {
                dollarSet.remove(s);
            } else {
                dollarSet.add(s);
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
        Files.write(path,(sql.trim() + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
    }
}
