package cs523.finalproject.analyzer;

import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

public class SentimentAnalyzer {
    static StanfordCoreNLP pipeline;

    public SentimentAnalyzer() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        pipeline = new StanfordCoreNLP(props);
    }

    public String findSentiment(String tweet) {
        String sentimentType = "";
        if (tweet != null && tweet.length() > 0) {
            Annotation annotation = pipeline.process(tweet);
            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                sentimentType = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
            }
        }
        return sentimentType;
        // sentiment ranges from very negative, negative, neutral, positive, very positive
    }
}