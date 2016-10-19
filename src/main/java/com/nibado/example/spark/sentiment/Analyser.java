package com.nibado.example.spark.sentiment;

import com.nibado.example.spark.Comment;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Analyser implements Serializable {
    private static final Map<String, Integer> LIST = new HashMap<>();

    public void analyse(Comment comment) {
        Score score = score(comment.getWords());
        comment.setScore(score);
    }

    public Score score(String[] words) {
        int score = 0;

        for(String word : words) {
            score += LIST.getOrDefault(word, 0);
        }

        return new Score(score, words.length);
    }

    private static void load(InputStream ins) {
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(ins))) {
            reader.lines()
                    .map(String::trim)
                    .map(l -> l.split("\\s+"))
                    .filter(a -> a.length == 2)
                    .forEach(a -> LIST.put(a[0], Integer.parseInt(a[1])));
        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    static {
        synchronized (LIST) {
            load(Analyser.class.getResourceAsStream("/data/wordlist.txt"));
        }
    }
}
