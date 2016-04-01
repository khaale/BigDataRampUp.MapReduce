package com.khaale.bigdatarampup.mapreduce.hw3.part1.parsing;

import org.apache.hadoop.io.Text;

import java.util.Scanner;
import java.util.regex.Pattern;

public class ScannerUserTagsIdParser implements UserTagsIdParser {

    private Scanner scanner;
    private static final Pattern delimiter = Pattern.compile("\\t");

    @Override
    public void set(Text row) {
        scanner = new Scanner(row.toString());
        scanner.useDelimiter(delimiter);
    }

    @Override
    public long getUserTagsId() {

        for(int i = 0; i < 20; i++) {
            scanner.next();
        }

       return scanner.nextLong();
    }
}
