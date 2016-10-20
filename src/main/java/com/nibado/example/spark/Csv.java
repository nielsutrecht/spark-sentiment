package com.nibado.example.spark;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

@Slf4j
public class Csv {
    public static void writeTuple2(List<Tuple2<String, Integer>> list, String file) {
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

    public static void writeTuple3(List<Tuple3<String, Integer, Integer>> list, String file) {
        File path = new File("target/" + file);
        log.info("Writing {} tuples to {}", list.size(), path.getAbsolutePath());

        try(PrintWriter outs = new PrintWriter(new FileWriter(path))) {
            list.stream()
                    .map(t -> t._1() + ", " + t._2() + ", " + t._3())
                    .forEach(outs::println);
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeTuple4(List<Tuple4<String, Integer, Integer, Integer>> list, String file) {
        File path = new File("target/" + file);
        log.info("Writing {} tuples to {}", list.size(), path.getAbsolutePath());

        try(PrintWriter outs = new PrintWriter(new FileWriter(path))) {
            list.stream()
                    .map(t -> t._1() + ", " + t._2() + ", " + t._3()+ ", " + t._4())
                    .forEach(outs::println);
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
