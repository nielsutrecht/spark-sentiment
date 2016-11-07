package com.nibado.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.List;

/**
 * Prints the subreddits with total, negative and positive counts.
 */
public class Example5 {
    /*
        RDDs for total, positive, negative
        Join all 3
        Map to Tuple4
        Collect
        Sort
        Print top 4
     */
    public static void main(String... argv) {
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

        JavaPairRDD<String, Integer> negative = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.NEGATIVE)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .filter(t -> t._2() > 10000);

        JavaPairRDD<String, Integer> positive = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.POSTIVE)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .filter(t -> t._2() > 10000);

        List<Tuple4<String, Integer, Integer, Integer>> results = totals
                .join(negative)
                .join(positive)
                .map(t -> new Tuple4<>(t._1, t._2._1._1,  t._2._1._2, t._2._2))
                //.map(t -> new Tuple4<>(t._2(), t._1(), t._3(), t._4()))
                .collect();

        results = new ArrayList<>(results);

        results.stream()
                .sorted((a, b) -> Integer.compare(b._2(), a._2()))
                .limit(25)
                .forEach(System.out::println);

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

        JavaPairRDD<String, Integer> negative = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.NEGATIVE)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .filter(t -> t._2() > 10000);

        JavaPairRDD<String, Integer> positive = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.POSTIVE)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .filter(t -> t._2() > 10000);

        List<Tuple4<String, Integer, Integer, Integer>> results = totals
                .join(negative)
                .join(positive)
                .map(t -> new Tuple4<>(t._1(), t._2._1._1,  t._2._1._2, t._2._2))
                .collect();

        results = new ArrayList<>(results);

        results.stream()
                .sorted((a, b) -> Integer.compare(b._2(), a._2()))
                .limit(25)
                .forEach(System.out::println);

        sc.close();
 */