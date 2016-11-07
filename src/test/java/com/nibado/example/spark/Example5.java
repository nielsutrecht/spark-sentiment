package com.nibado.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Prints word counts
 */
public class Example5 {
    public static void main(String... argv) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Spark Sentiment")
                        .setMaster("local[8]"));

        String input = System.getProperty("user.home") + "/data/object-file-small";

        JavaRDD<Comment> comments = sc.objectFile(input);

        List<Tuple2<String, Integer>> results = comments
                .flatMap(c -> Arrays.asList(c.getWords()).iterator())
                .mapToPair(w -> new Tuple2<>(w, 1))
                .reduceByKey((a, b) -> a + b)
                .filter(t -> t._2() > 10000)
                .collect();

        results = new ArrayList<>(results);

        results.stream()
                .sorted((a, b) -> Integer.compare(b._2(), a._2()))
                .limit(25)
                .forEach(System.out::println);

        sc.close();
    }
}
