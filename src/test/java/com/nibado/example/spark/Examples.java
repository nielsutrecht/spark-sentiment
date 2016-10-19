package com.nibado.example.spark;

import com.nibado.example.spark.sentiment.Analyser;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

import static com.nibado.example.spark.Csv.writeTuples;

@Slf4j
public class Examples implements Serializable {
    public static final String BIG_FILE = System.getProperty("user.home") + "/data/RC_2015-01.bz2";
    public static final String SMALL_SAMPLE = System.getProperty("user.home") + "/data/RC_2015-01-small/part*";
    private static final SparkConf CONFIG_EIGHT = new SparkConf().setAppName("HelloSparkWorld").setMaster("local[8]");
    private static final SparkConf CONFIG_ONE = new SparkConf().setAppName("HelloSparkWorld").setMaster("local[1]");

    private static JavaSparkContext sparkContext;

    private Analyser analyser = new Analyser();

    @BeforeClass
    public static void beforeClass() {
        log.info("Start");
    }

    @Test
    public void printLines() {
        JavaSparkContext sc = new JavaSparkContext(CONFIG_ONE);

        sc
                .textFile(BIG_FILE)
                .take(1000)
                .forEach(System.out::println);
    }

    @Test
    public void countLines() {
        long start = System.currentTimeMillis();
        System.out.println(ctx().textFile(SMALL_SAMPLE).count());
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void printComments() {
        long start = System.currentTimeMillis();
        long count = ctx()
                .textFile(SMALL_SAMPLE)
                .map(Mappers::toComment)
                .filter(c -> !c.isDeleted())
                .map(c -> { analyser.analyse(c);return c;})
                .count();
        System.out.println(count);
        System.out.println(System.currentTimeMillis() - start);
    }

    private JavaRDD<Comment> comments() {
        log.info("Reading comments from {}", SMALL_SAMPLE);
        return ctx()
                .textFile(SMALL_SAMPLE)
                .map(Mappers::toComment)
                .filter(c -> !c.isDeleted())
                .map(c -> { analyser.analyse(c);return c;});
    }

    @Test
    public void groupPositiveBySubReddit() {
        List<Tuple2<String, Integer>> results;

        results = comments()
                .filter(c -> c.getSentiment() == Comment.Sentiment.POSTIVE)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, "subPositive.csv");
    }

    @Test
    public void groupNegativeBySubReddit() {
        List<Tuple2<String, Integer>> results;

        results = comments()
                .filter(c -> c.getSentiment() == Comment.Sentiment.NEGATIVE)
                .mapToPair(c -> new Tuple2<>(c.getSubReddit(), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();

        writeTuples(results, "subNegative.csv");
    }

    @Test
    public void groupAll() {
        JavaRDD<Comment> comments = comments();

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
    }

    public static JavaSparkContext ctx() {
        if(sparkContext == null) {
            sparkContext = new JavaSparkContext(CONFIG_EIGHT);
        }
        return sparkContext;
    }
}
