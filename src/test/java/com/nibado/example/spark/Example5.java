package com.nibado.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Prints the subreddits with total, positive and negative counts.
 */
public class Example5 {
    /*
        RDDs for total, positive, negative
        Join all 3
        Map to Tuple4
        Sort
        Print top 25
     */
    public static void main(String... argv) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Spark Sentiment")
                        .setMaster("local[8]"));

        String input = System.getProperty("user.home") + "/data/object-file-small";

        JavaRDD<Comment> comments = sc.objectFile(input);

        sc.close();
    }
}











/*
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Spark Sentiment")
                        .setMaster("local[8]"));

        String input = System.getProperty("user.home") + "/data/object-file-small";

        JavaRDD<Comment> comments = sc.objectFile(input);

        JavaPairRDD<String, Integer> totals = comments
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .filter(t -> t._2() > 10000);

        JavaPairRDD<String, Integer> positive = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.POSTIVE)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .filter(t -> t._2() > 10000);

        JavaPairRDD<String, Integer> negative = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.NEGATIVE)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .filter(t -> t._2() > 10000);

        totals
                .join(negative)
                .join(positive)
                .mapToPair(t -> new Tuple2<>(t._2._1._1, new Tuple3<>(t._1, t._2._2, t._2._1._2)))
                .sortByKey(false)
                .map(t -> new Tuple4<>(t._2()._1(), t._1(), t._2()._2(), t._2()._3()))
                .take(25)

                .forEach(System.out::println);

        sc.close();
 */