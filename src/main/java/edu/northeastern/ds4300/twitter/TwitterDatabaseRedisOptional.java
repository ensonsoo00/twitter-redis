package edu.northeastern.ds4300.twitter;

import redis.clients.jedis.Jedis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

/**
 * This class represents an API that connects to a Redis database and provides functionality to insert Tweet objects or
 * retrieve a given user's home timeline. The implementations of the API methods in this class are based on the optional strategy,
 * which involves non-precomputed timelines.
 */
public class TwitterDatabaseRedisOptional implements TwitterDatabaseAPI {

    private Jedis jedis;


    /**
     * Inserts a single Tweet object into the Redis database.
     * It serializes the Tweet object into a string using pipes to separate each Tweet field. The posting process creates
     * a tweet key-value and also adds the tweet ID to the user's list of tweets.
     *
     * @param t Tweet object to be inserted
     */
    @Override
    public void postTweet(Tweet t) {
        // generating tweet timestamp
        Date tweetTimestamp = new Date(System.currentTimeMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // serializing Tweet object as a string
        String tweet = t.getUserID() + "|" + sdf.format(tweetTimestamp) + "|" + t.getTweetText();

        // increment currTweetID and store it as nextTweetID
        // Note: currTweetID is incremented first, in case the key does not already exist
        jedis.incr("currTweetID");
        String nextTweetID =  jedis.get("currTweetID");
        jedis.set("tweet:" + nextTweetID, tweet);

        // add tweet ID to the user's list of tweets
        jedis.lpush("usertweet:" + t.getUserID(), nextTweetID);
    }

    /**
     * Inserts multiple Tweet objects (in a list) into the Redis database. The implementation of this method applies the same logic as
     * the postTweet() method.
     * @param tweets list of Tweet objects
     */
    @Override
    public void postTweets(List<Tweet> tweets) {
        for (Tweet tweet : tweets) {
            postTweet(tweet);
        }
    }

    /**
     * Retrieves the home timeline of a given user. The user's home timeline consists of the 10 most recent tweets
     * from users that the given user follows.
     * In this implementation, the timeline for the given user is generated on the fly. First, we retrieve the followees of the
     * given user. For each followee, retrieve their 10 latest tweets. These tweets are concatenated, creating a list of tweets
     * of 10 tweets for each followee. To create the timeline, sort the list of tweets in order of most recent to least recent, and
     * retrieve the first 10 most recent tweets.
     * @param userID user ID of the user
     * @return list of Tweet objects representing the user home timeline
     */
    @Override
    public List<Tweet> getTimeline(Integer userID) {
        // retrieve followees of given user
        List<Integer> followees = getFollowees(userID);

        // retrieve 10 most recent tweets from each followee
        List<String> tweetIDList = new ArrayList<>();
        for (Integer followeeID : followees) {
            List<String> tweets = jedis.lrange("usertweet:" + followeeID, 0, 9);
            tweetIDList.addAll(tweets);
        }

        // process each tweet, create Tweet objects, and construct list of Tweets
        List<Tweet> tweets = new ArrayList<>();
        for (String tweetID : tweetIDList) {
            // retrieve tweet data
            String tweetString = jedis.get("tweet:" + tweetID);
            // parse tweet data by splitting by delimiter "|"
            // note: split() limits to 3 splits to avoid splitting on potential tweet text (if it contains "|")
            String[] tweetSplit = tweetString.split("\\|", 3);

            int tweetUserID = Integer.parseInt(tweetSplit[0]);
            Date tweetTimestamp = null;
            try {
                tweetTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(tweetSplit[1]);
            } catch (ParseException e) {
                System.out.println("ERROR: could not parse the tweet timestamp: " + tweetSplit[1]);
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
            String tweetText = tweetSplit[2];

            // constructing new Tweet object
            Tweet tweet = new Tweet(Integer.parseInt(tweetID), tweetUserID, tweetTimestamp, tweetText);
            // add tweet to list of tweets
            tweets.add(tweet);
        }

        // sort Tweet list from most recent to least recent
        tweets.sort(new TweetComparator());

        // return 10 most recent Tweets from the Tweet list
        return tweets.subList(0, Math.min(10, tweets.size()));
    }

    /**
     * Get a list of all unique user IDs that follow at least one other user. This method simply retrieves the set of
     * users (constructed when populating the "following/follower" relationships in the Redis setup step).
     * @return list of user IDs
     */
    @Override
    public List<Integer> getUsers() {
        Set<String> users = jedis.smembers("users");

        // convert set of integer strings to list of integers
        List<Integer> usersID = new ArrayList<>();
        for (String user : users) {
            usersID.add(Integer.parseInt(user));
        }
        return usersID;
    }

    /**
     * Get a list of user IDs that follow the given user ID. This method simply retrieves the list of user IDs from the
     * "followers" key-values (constructed when populating the "following/follower" relationships in the Redis setup step).
     * @param userID given user ID
     * @return list of user IDs that follow the given user ID
     */
    @Override
    public List<Integer> getFollowers(Integer userID) {
        // retrieve followers of given user
        List<String> followers = jedis.lrange("followers:" + userID, 0, -1);

        // convert set of integer strings to list of integers
        List<Integer> followersID = new ArrayList<>();
        for (String follower : followers) {
            followersID.add(Integer.parseInt(follower));
        }
        return followersID;
    }

    /**
     * Get a list of user IDs that the given user follows. This method simply retrieves the list of user IDs from the
     * "following" key-values (constructed when populating the "following/follower" relationships in the Redis setup step).
     * @param userID given user ID
     * @return list of user IDs that the given user ID follows
     */
    @Override
    public List<Integer> getFollowees(Integer userID) {
        // retrieve followees of given user
        List<String> followees = jedis.lrange("following:" + userID, 0, -1);

        // convert set of integer strings to list of integers
        List<Integer> followeesID = new ArrayList<>();
        for (String followee : followees) {
            followeesID.add(Integer.parseInt(followee));
        }
        return followeesID;
    }

    /**
     * Initializes Jedis instance (method parameters are irrelevant in regards to the Redis database connection).
     * @param url database connector URL N/A
     * @param user database username N/A
     * @param password database password N/A
     */
    @Override
    public void authenticate(String url, String user, String password) {
        this.jedis = new Jedis();
    }

    /**
     * Closes the Redis connection.
     */
    @Override
    public void closeConnection() {
        jedis.quit();
    }
}
