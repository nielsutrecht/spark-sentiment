package com.nibado.example.spark;

import com.nibado.example.spark.sentiment.Analyser;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static com.nibado.example.spark.Csv.writeTuples;
import static com.nibado.example.spark.Examples.BIG_FILE;
import static com.nibado.example.spark.Examples.SMALL_SAMPLE;
import static com.nibado.example.spark.Examples.ctx;
import static com.nibado.example.spark.Mappers.toDayOfWeek;
import static java.util.Arrays.asList;

public class Util implements Serializable {
    private static final String OBJECT_FILE = "target/object-file";
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

        writeTuples(results, "subNegative.csv");

        results = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.POSTIVE)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, "subPositive.csv");

        results = comments
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, "subTotal.csv");

        results = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.NEGATIVE)
                .mapToPair(c -> new Tuple2<>(toDayOfWeek(c.getDateTime()), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, "dayNegative.csv");

        results = comments
                .filter(c -> c.getSentiment() == Comment.Sentiment.POSTIVE)
                .mapToPair(c -> new Tuple2<>(toDayOfWeek(c.getDateTime()), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, "dayPositive.csv");

        results = comments
                .mapToPair(c -> new Tuple2<>(toDayOfWeek(c.getDateTime()), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, "dayTotal.csv");

        results = comments
                .mapToPair(c -> new Tuple2<>(c.getAuthor(), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, "authorTotal.csv");


        results = comments
                .flatMap(c -> asList(c.getWords()).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, "wordTotal.csv");
    }
}
