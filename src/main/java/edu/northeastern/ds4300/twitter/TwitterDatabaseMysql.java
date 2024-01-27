package edu.northeastern.ds4300.twitter;

import edu.northeastern.database.DBUtils;

import java.sql.*;
import java.util.*;

/**
 * This class represents an API that connects to a MySQL database and provides functionality to insert Tweet objects or retrieve a given user's home timeline.
 */
public class TwitterDatabaseMysql implements TwitterDatabaseAPI {

    private DBUtils dbu;


    /**
     * Inserts a single Tweet object into the `tweet` table in the MySQL database.
     * <p>Creates and executes a MySQL INSERT statement to insert the single Tweet object. When inserting
     * records into the database, the database handles the tweet IDs (auto-incremented) and the tweet timestamp values.</p>
     * @param t Tweet object to be inserted
     */
    @Override
    public void postTweet(Tweet t) {
        String statement = "INSERT INTO tweet (user_id, tweet_ts, tweet_text) VALUES (" + t.getUserID() + ", NOW(), '" + t.getTweetText() + "')";
        dbu.executeUpdate(statement);
    }

    /**
     * Inserts multiple Tweet objects from a list into the `tweet` table in the MySQL database.
     * <p> Creates and executes a MySQL INSERT statement containing the Tweet objects from the list of tweets. When inserting
     * records into the database, the database handles the tweet IDs (auto-incremented) and the tweet timestamp values. </p>
     * @param tweets list of Tweet objects
     */
    @Override
    public void postTweets(List<Tweet> tweets) {
        if (tweets.isEmpty()) return;
        String statement = "INSERT INTO tweet (user_id, tweet_ts, tweet_text) VALUES";
        Tweet firstTweet = tweets.get(0); // separating first tweet to account for comma placements in the SQL statement
        statement += (" (" + firstTweet.getUserID() + ", NOW(), '" + firstTweet.getTweetText() + "')");
        int index = 1;
        int len = tweets.size();
        // generate MySQL INSERT statement with all of the tweets in the list
        while (index < len) {
            Tweet tweet = tweets.get(index);
            statement += (", (" + tweet.getUserID() + ", NOW(), '" + tweet.getTweetText() + "')");
            index++;
        }
        dbu.executeUpdate(statement);

    }

    /**
     * Retrieves the home timeline of a given user. The user's home timeline consists of the 10 most recent tweets
     * from users that the given user follows.
     * <p>In this implementation, a SQL query is executed to select the user's followees, join with the `tweet` table
     * to extract the followees' tweets, sort them by timestamp, then return the 10 most recent tweets. The resulting
     * query table is used to create a list of Tweet objects representing the home timeline.</p>
     * @param userID user ID of the user
     * @return list of Tweet objects representing the user home timeline
     */
    @Override
    public List<Tweet> getTimeline(Integer userID) {
        String statement = "SELECT tweet_id, user_id, tweet_ts, tweet_text FROM (SELECT follows_id FROM `follows` WHERE user_id = "
                + userID + ") followees JOIN tweet ON (followees.follows_id = tweet.user_id) "
                + "ORDER BY tweet_ts DESC LIMIT 10";
        List<Tweet> tweets = new ArrayList<>();
        try {
            Connection con = dbu.getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(statement);
            while (rs.next()) {
                // read in query result table and create Tweet objects from the extracted field data of each row
                int tweetID = rs.getInt(1);
                int tweetUserID = rs.getInt(2);
                Timestamp tweetTimestamp = rs.getTimestamp(3);
                String tweetText = rs.getString(4);
                tweets.add(new Tweet(tweetID, tweetUserID, tweetTimestamp, tweetText)); // create Tweet object and add it to timeline list
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            System.out.println("ERROR: Could not execute query: " + statement);
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        return tweets;
    }

    /**
     * Get a list of all unique user IDs that follow at least one other user in the `follows` table in the MySQL database.
     * @return list of user IDs
     */
    @Override
    public List<Integer> getUsers() {
        String statement = "SELECT DISTINCT user_id FROM `follows`";
        List<Integer> users = new ArrayList<>();
        try {
            Connection con = dbu.getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(statement);
            while (rs.next()) {
                // read in query result table and add each user ID into the user list
                users.add(rs.getInt(1));
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            System.out.println("ERROR: Could not execute query: " + statement);
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        return users;
    }

    @Override
    public List<Integer> getFollowers(Integer userID) {
        return null;
    }

    @Override
    public List<Integer> getFollowees(Integer userID) {
        return null;
    }


    /**
     * Set connection settings
     * @param url database connector URL
     * @param user database username
     * @param password database password
     */
    @Override
    public void authenticate(String url, String user, String password) {

        dbu = new DBUtils(url, user, password);
    }

    /**
     * Close the connection when application finishes
     */
    @Override
    public void closeConnection() {
        dbu.closeConnection();
    }

}
