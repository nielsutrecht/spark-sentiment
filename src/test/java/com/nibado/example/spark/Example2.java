package com.nibado.example.spark;

import com.nibado.example.spark.sentiment.Analyser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Analyse comments and write to object file.
 */
public class Example2 {
    /*
        Add analyser
        Write to object file
     */
    public static void main(String... argv) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Spark Sentiment")
                        .setMaster("local[1]"));

        String input = System.getProperty("user.home") + "/data/RC_2015-01.bz2";
        String output = System.getProperty("user.home") + "/data/RC_2015-01.bz2-object-" + System.currentTimeMillis();

        Analyser analyser = new Analyser();

        sc.textFile(input)
                .map(Mappers::toComment)
                .filter(c -> !c.isDeleted());

        sc.close();
    }
}





































/*
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Spark Sentiment")
                        .setMaster("local[1]"));

        String input = System.getProperty("user.home") + "/data/RC_2015-01.bz2";
        String output = System.getProperty("user.home") + "/data/RC_2015-01.bz2-object-" + System.currentTimeMillis();

        Analyser analyser = new Analyser();

        sc.textFile(input)
                .map(Mappers::toComment)
                .filter(c -> !c.isDeleted())
                .map(c -> {analyser.analyse(c);return c;})
                .saveAsObjectFile(output);
 */