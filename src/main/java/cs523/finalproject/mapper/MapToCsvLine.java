package cs523.finalproject.mapper;

import com.google.gson.reflect.TypeToken;
import com.twitter.clientlib.JSON;
import com.twitter.clientlib.model.Point;
import com.twitter.clientlib.model.StreamingTweetResponse;
import com.twitter.clientlib.model.Tweet;
import com.twitter.clientlib.model.TweetGeo;
import cs523.finalproject.analyzer.SentimentAnalyzer;
import org.apache.hadoop.util.StringUtils;

import java.lang.reflect.Type;

public class MapToCsvLine {

    private static SentimentAnalyzer sentimentAnalyzer = new SentimentAnalyzer();
    private static Type localVarReturnType = new TypeToken<StreamingTweetResponse>() {
    }.getType();

    public static String convertMessageToCommaSeperated(String message) {
        StringBuilder stringBuilder = new StringBuilder();

        StreamingTweetResponse streamingTweetResponse = JSON.getGson().fromJson(message, localVarReturnType);
        Tweet data = streamingTweetResponse.getData();
        stringBuilder
                .append(clean(data.getId())).append(StringUtils.COMMA_STR)
                .append(clean(data.getAuthorId())).append(StringUtils.COMMA_STR)
                .append(data.getCreatedAt()).append(StringUtils.COMMA_STR)
                .append(getPlaceId(data.getGeo())).append(StringUtils.COMMA_STR) //placeId
                .append(getCoordinates(data.getGeo())).append(StringUtils.COMMA_STR) //cooridvates
                .append(data.getPossiblySensitive()).append(StringUtils.COMMA_STR)
                .append(clean(data.getSource())).append(StringUtils.COMMA_STR)
                .append(clean(data.getText())).append(StringUtils.COMMA_STR)
                .append(sentiment(data.getText()));
        return stringBuilder.toString();
    }

    static String sentiment(String tweet){
        if(tweet != null || tweet.length() !=0 ){
            return sentimentAnalyzer.findSentiment(tweet);
        }
        return "Neutral";
    }

    static String clean(String data) {
        if (data == null) {
            data = "";
        }
        return data;

    }

    static String getPlaceId(TweetGeo geo) {
        if (geo != null) {
            return geo.getPlaceId();
        }
        return "";

    }

    static String getCoordinates(TweetGeo geo) {
        if (geo != null) {
            Point p = geo.getCoordinates();
            if (p != null) {
                return p.getCoordinates().toString();
            }
        }
        return "[]";
    }
}
