package com.nibado.example.spark;

import com.nibado.example.spark.sentiment.Analyser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Demonstrates reading a file, printing contents, mapping to Java object and filtering.
 */
public class Example1 {
    /*
        Read textFile
        Take 100 output
        Map to comment
        Filter deleted
        Add analyser
        Take 100
        Print all
     */
    public static void main(String... argv) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Spark Sentiment")
                        .setMaster("local[1]"));

        String input = System.getProperty("user.home") + "/data/RC_2015-01.bz2";

        Analyser analyser = new Analyser();

        sc.textFile(input);

        sc.close();
    }
}










/*
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Spark Sentiment")
                        .setMaster("local[1]"));

        String input = System.getProperty("user.home") + "/data/RC_2015-01.bz2";

        Analyser analyser = new Analyser();

        sc.textFile(input)
                .map(Mappers::toComment)
                .filter(c -> !c.isDeleted())
                .map(c -> { analyser.analyse(c);return c;})
                .take(100)
                .forEach(System.out::println);

        sc.close();
 */
