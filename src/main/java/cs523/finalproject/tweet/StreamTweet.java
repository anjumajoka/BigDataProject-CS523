package cs523.finalproject.tweet;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import cs523.finalproject.kafka.TweetPublisher;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 *  Uses @{@link TwitterApi} to connect with twitter and streams the tweets received from the Twitter.
 *  Publishes the continuous tweet stream to {@link cs523.finalproject.util.AppConstants.TWEETER_DATA_TOPIC} topic using @{@link TweetPublisher}
 */

public class StreamTweet {



    public static void main(String[] args) {

        TweetPublisher tweetPublisher = new TweetPublisher();

        TwitterApi apiInstance = new TwitterApi(new TwitterCredentialsBearer("AAAAAAAAAAAAAAAAAAAAAB%2FDhAEAAAAA7%2BbjyFa5G7DupACLBWJr0LfpkmE%3DWkfAICf9xNsgf7ekXQMLN5tSlE5ChjT0WO6kys64fZjlQCaAX1"));

        Set<String> tweetFields = new HashSet<>();
        tweetFields.add("id");
        tweetFields.add("author_id");
        tweetFields.add("created_at");
        tweetFields.add("geo");
        tweetFields.add("lang");
        tweetFields.add("possibly_sensitive");
        tweetFields.add("source");
        tweetFields.add("text");

        try {
            InputStream streamResult = apiInstance.tweets().sampleStream()
                    .backfillMinutes(0)
                    .tweetFields(tweetFields)
                    .execute();

            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(streamResult));
                String tweetInfo = reader.readLine();
                while (tweetInfo != null ) {
                    if (tweetInfo.isEmpty()) {
                        System.err.println("==> " + tweetInfo.isEmpty());
                        tweetInfo = reader.readLine();
                        continue;

                    }
                    tweetPublisher.sendMessage(tweetInfo);
                    tweetInfo = reader.readLine();

                }

                System.out.println("Closing Stream.......");
                streamResult.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(e);
            }

        } catch (ApiException e) {
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}

