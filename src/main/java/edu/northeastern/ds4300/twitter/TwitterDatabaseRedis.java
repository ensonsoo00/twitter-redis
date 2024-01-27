package edu.northeastern.ds4300.twitter;

import redis.clients.jedis.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Date;

/**
 * This class represents an API that connects to a Redis database and provides functionality to insert Tweet objects or
 * retrieve a given user's home timeline.
 */
public class TwitterDatabaseRedis implements TwitterDatabaseAPI {

    private Jedis jedis;


    /**
     * Inserts a single Tweet object into the Redis database.
     * It serializes the Tweet object into a string using pipes to separate each Tweet field. The posting process adds
     * the tweet ID to the timelines of the user's followers (pre-computes timelines).
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

        // for every user that follows the tweet's user, add this tweet to their timeline
        List<Integer> followers = getFollowers(t.getUserID());
        for (Integer followerID : followers) {
            jedis.lpush("timeline:" + followerID, nextTweetID);
        }
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
     * In this implementation, a timeline key-value for each user was generated when tweets were posted, so this method simply
     * retrieves the first 10 tweet IDs from the pre-computed timeline of the given user. Then, with the tweet IDs, the tweet
     * information can be retrieved from the tweet key-values.
     * @param userID user ID of the user
     * @return list of Tweet objects representing the user home timeline
     */
    @Override
    public List<Tweet> getTimeline(Integer userID) {
        List<Tweet> tweets = new ArrayList<>();

        // retrieve 10 most recently inserted tweets from timeline key-value store
        List<String> timeline = jedis.lrange("timeline:" + userID, 0, 9);

        // extract tweet data of each tweet ID from the timeline
        for (String tweetID : timeline) {
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
            // add tweet to timeline
            tweets.add(tweet);
        }
        return tweets;
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
        this.jedis.quit();
    }


}
