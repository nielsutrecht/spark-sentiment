package com.nibado.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Example1 {
    /*
        Read textFile
        Take 100 output
        Map to comment
        Filter deleted
        Take 100
     */
    public static void main(String... argv) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Spark Sentiment")
                        .setMaster("local[1]"));

        String input = System.getProperty("user.home") + "/data/RC_2015-01.bz2";

        sc.close();
    }
}





































/*
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Spark Sentiment")
                        .setMaster("local[1]"));

        String input = System.getProperty("user.home") + "/data/RC_2015-01.bz2";

        sc.textFile(input)
                .map(Mappers::toComment)
                .filter(c -> !c.isDeleted())
                .take(100).forEach(System.out::println);
 */
