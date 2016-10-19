package com.nibado.example.spark;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

@Slf4j
public class Csv {
    public static void writeTuples(List<Tuple2<String, Integer>> list, String file) {
        File path = new File("target/" + file);
        log.info("Writing {} tuples to {}", list.size(), path.getAbsolutePath());

        try(PrintWriter outs = new PrintWriter(new FileWriter(path))) {
            list.stream()
                    .map(t -> t._1() + ", " + t._2())
                    .forEach(outs::println);
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
