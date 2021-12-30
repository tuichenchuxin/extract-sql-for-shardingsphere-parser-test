package org.test;


import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class UnsupportedGenerator {
    public static void main(String[] args) throws IOException {
        String line;
        BufferedReader bufferedReader = Files.newBufferedReader(Paths.get("src/main/resources/generator/source.txt"));
        int count = 1;
        while(null != (line = bufferedReader.readLine())) {
            line = line.replace("&", "&amp;");
            line = line.replace("'", "&apos;");
            line = line.replace("\"", "&quot;");
            line = line.replace(">", "&gt;");
            line = line.replace("<", "&lt;");
            String newLine = "<sql-case id=\"create_by_mysql_source_test_case" + count++ +"\" value=\"" + line + "\" db-types=\"MySQL\"/>";
            write(newLine);
        }
    }
    private static void write(String line) throws IOException {
        Files.write(Paths.get("src/main/resources/generator/target.txt"),(line + "\n").getBytes(), StandardOpenOption.APPEND);
    }
}
