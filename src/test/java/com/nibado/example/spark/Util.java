package com.nibado.example.spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;

import static com.nibado.example.spark.Examples.BIG_FILE;
import static com.nibado.example.spark.Examples.SMALL_SAMPLE;

public class Util implements Serializable {
    private static final SparkConf CONFIG = new SparkConf().setAppName("HelloSparkWorld").setMaster("local[1]");

    @Test
    public void deleteSample() throws Exception {
        FileUtils.deleteDirectory(new File(SMALL_SAMPLE));
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
}
