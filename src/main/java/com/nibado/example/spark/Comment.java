package com.nibado.example.spark;

import com.nibado.example.spark.sentiment.Score;
import lombok.Data;

import java.io.Serializable;
import java.util.Locale;

import static com.nibado.example.spark.Comment.Sentiment.NEGATIVE;
import static com.nibado.example.spark.Comment.Sentiment.NEUTRAL;
import static com.nibado.example.spark.Comment.Sentiment.POSTIVE;
import static com.nibado.example.spark.Mappers.toDateString;

@Data
public class Comment implements Serializable {
    public static final long serialVersionUID = 1L;
    private String subReddit;
    private String author;
    private long timeStamp;
    private String body;
    private boolean deleted;
    private String[] words;
    private Score score;

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "%s %s %s: %s", toDateString(timeStamp), subReddit, score, body);
    }

    public Sentiment getSentiment() {
        if(score.getScore() > 0) {
            return POSTIVE;
        }
        else if(score.getScore() < 0) {
            return NEGATIVE;
        }
        else {
            return NEUTRAL;
        }
    }

    public double normalizedScore() {
        return (double) score.getScore() / (double)score.getWords();
    }

    public enum Sentiment {
        POSTIVE,
        NEGATIVE,
        NEUTRAL
    }
}
