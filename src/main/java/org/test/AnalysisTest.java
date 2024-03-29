package org.test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class AnalysisTest {
    public static void main(String[] args) throws IOException {
        analysisMySql();
        analysisPostgreSql();
        analysisOpenGauss();
    }
    
    private static void analysisOpenGauss() throws IOException {
        List<String> lines = Files.readAllLines(Paths.get("/Users/chenchuxin/Documents/GitHub/extract-sql-for-shardingsphere-parser-test/src/main/resources/result/opengauss.txt"));
        lines = new ArrayList<>(new LinkedHashSet<>(lines));
        Collections.sort(lines);
        List<String> result = new ArrayList<>(lines.size());
        for (String each : lines) {
            result.add(replaceSpace(each));
        }
        Path path = Paths.get("src/main/resources/analysis/opengauss-analysis.txt");
        Files.write(path, result, StandardOpenOption.APPEND);
    }
    
    private static String replaceSpace(final String sql) {
        String result = sql;
        if (sql.contains("  ")) {
            result = replaceSpace(sql.replaceAll("  ", " "));
        }
        if (sql.contains("\t")) {
            result = replaceSpace(sql.replaceAll("\t", " "));
        }
        return result;
    }
    
    public static void analysisMySql() throws IOException {
        List<String> lines = Files.readAllLines(Paths.get("/Users/chenchuxin/Documents/GitHub/extract-sql-for-shardingsphere-parser-test/src/main/resources/result/mySql.txt"));
        Collections.sort(lines);
        Path path = Paths.get("src/main/resources/analysis/mysql-analysis.txt");
        Files.write(path, lines, StandardOpenOption.APPEND);
        appendPercent(lines, path);
    }
    
    private static void appendPercent(final List<String> lines, final Path path) throws IOException {
        Map<String, BigDecimal> map = new HashMap<>();
        for (String each : lines) {
            if (each.contains(" ")) {
                String word = each.substring(0, each.indexOf(" ")).toLowerCase();
                map.merge(word, BigDecimal.ONE, BigDecimal::add);
            } else {
                map.merge(each.toLowerCase(), BigDecimal.ONE, BigDecimal::add);
            }
        }
        Map<BigDecimal, String> sorted = new TreeMap<>(BigDecimal::compareTo);
        
        for (Map.Entry<String, BigDecimal> entry : map.entrySet()) {
            sorted.merge(entry.getValue(), entry.getKey(), (s1, s2) -> s1 + "===" + s2);
        }
        for (Map.Entry<BigDecimal, String> entry : sorted.entrySet()) {
            String s = String.format("this type %s percentage is %s \n", entry.getValue(), entry.getKey().divide(new BigDecimal(lines.size()), 4, RoundingMode.HALF_EVEN));
            Files.write(path, s.getBytes(), StandardOpenOption.APPEND);
        }
    }
    
    public static void analysisPostgreSql() throws IOException {
        List<String> lines = Files.readAllLines(Paths.get("/Users/chenchuxin/Documents/GitHub/extract-sql-for-shardingsphere-parser-test/src/main/resources/result/postgreSql.txt"));
        Collections.sort(lines);
        Path path = Paths.get("src/main/resources/analysis/postgresql-analysis.txt");
        Files.write(path, lines, StandardOpenOption.APPEND);
        appendPercent(lines, path);
    }
}
