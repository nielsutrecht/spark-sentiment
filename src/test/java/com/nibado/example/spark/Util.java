package com.nibado.example.spark;

import com.nibado.example.spark.sentiment.Analyser;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple4;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static com.nibado.example.spark.Csv.writeTuple2;
import static com.nibado.example.spark.Csv.writeTuple4;
import static com.nibado.example.spark.Examples.*;
import static com.nibado.example.spark.Mappers.toDayOfWeek;
import static java.util.Arrays.asList;

public class Util implements Serializable {
    private Analyser analyser = new Analyser();
    private static final SparkConf CONFIG = new SparkConf().setAppName("HelloSparkWorld").setMaster("local[1]");

    @Test
    public void deleteSample() throws Exception {
        FileUtils.deleteDirectory(new File(SMALL_SAMPLE));
        FileUtils.deleteDirectory(new File(OBJECT_FILE));
    }

    @Test
    public void writeSample() throws IOException {
        JavaSparkContext sc = new JavaSparkContext(CONFIG);

        LocalDateTime start = LocalDateTime.now();

        sc
                .textFile(BIG_FILE)
                .filter(s -> Math.random() <= 0.1) //Keep 10% of data.
                .saveAsTextFile(SMALL_SAMPLE);

        System.out.println(Duration.between(LocalDateTime.now(), start));
    }

    @Test
    public void writeObjectFile() throws Exception {
        //FileUtils.deleteDirectory(new File(OBJECT_FILE));

        JavaSparkContext sc = new JavaSparkContext(CONFIG);

        LocalDateTime start = LocalDateTime.now();
        sc
                .textFile(BIG_FILE)
                .map(Mappers::toComment)
                .filter(c -> !c.isDeleted())
                .map(c -> { analyser.analyse(c);return c;})
                .saveAsObjectFile(OBJECT_FILE);

        System.out.println(Duration.between(LocalDateTime.now(), start));
    }

    @Test
    public void writeSmallObjectFile() throws Exception {
        FileUtils.deleteDirectory(new File(OBJECT_FILE_SMALL));

        JavaSparkContext sc = new JavaSparkContext(CONFIG);

        LocalDateTime start = LocalDateTime.now();
        sc
                .objectFile(OBJECT_FILE)
                .filter(o -> Math.random() <= 0.1)
                .map(c -> (Comment)c)
                .map(c -> {c.setBody(null);return c;})
                .saveAsObjectFile(OBJECT_FILE_SMALL);

        System.out.println(Duration.between(LocalDateTime.now(), start));
    }

    @Test
    public void countSample() {
        JavaSparkContext sc = new JavaSparkContext(CONFIG);

        LocalDateTime start = LocalDateTime.now();

        long count = sc
                .textFile(SMALL_SAMPLE)
                .count();

        System.out.println(count);
        System.out.println(Duration.between(LocalDateTime.now(), start));
    }

    @Test
    public void home() {
        System.out.println(System.getProperty("user.home"));
    }

    @Test
    public void writeAll() {
        JavaRDD<Comment> comments = ctx().objectFile(OBJECT_FILE);

        List<Tuple2<String, Integer>> results;

        results = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.NEGATIVE)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuple2(results, "subNegative.csv");

        results = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.POSTIVE)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuple2(results, "subPositive.csv");

        results = comments
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuple2(results, "subTotal.csv");

        results = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.NEGATIVE)
                .mapToPair(c -> new Tuple2<>(toDayOfWeek(c.getDateTime()), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuple2(results, "dayNegative.csv");

        results = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.POSTIVE)
                .mapToPair(c -> new Tuple2<>(toDayOfWeek(c.getDateTime()), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuple2(results, "dayPositive.csv");

        results = comments
                .mapToPair(c -> new Tuple2<>(toDayOfWeek(c.getDateTime()), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuple2(results, "dayTotal.csv");

        results = comments
                .mapToPair(c -> new Tuple2<>(c.getAuthor(), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuple2(results, "authorTotal.csv");


        results = comments
                .flatMap(c -> asList(c.getWords()).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuple2(results, "wordTotal.csv");
    }

    @Test
    public void join() {
        JavaRDD<Comment> comments = ctx().objectFile(OBJECT_FILE);

        JavaPairRDD<String, Integer> subNegative = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.NEGATIVE)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b);

        JavaPairRDD<String, Integer> subPositive = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.POSTIVE)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b);


        JavaPairRDD<String, Integer> subTotal = comments
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b);

        List<Tuple4<String, Integer, Integer, Integer>> results;

        results = subTotal
                .join(subNegative)
                .join(subPositive).map(t -> new Tuple4<>(t._1(), t._2._1._1,  t._2._1._2, t._2._2)).collect();

        writeTuple4(results, "subCombined.csv");
    }
}
