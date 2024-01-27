package edu.northeastern.ds4300.twitter;

import java.util.List;

/**
 * This interface represents an API that provides functionality to insert Tweet objects into a database or retrieve a given user's home timeline.
 */
public interface TwitterDatabaseAPI {

    /**
     * Inserts a single Tweet object into the database
     * @param t Tweet object to be inserted
     */
    public void postTweet(Tweet t);

    /**
     * Inserts multiple Tweet objects from a list into the database
     * @param tweets list of Tweet objects
     */
    public void postTweets(List<Tweet> tweets);

    /**
     * Retrieves the home timeline of a given user. The user's home timeline consists of the 10 most recent tweets
     * from users/followees that the given user follows.
     * @param userID user ID of the user
     * @return list of Tweet objects representing the user home timeline
     */
    public List<Tweet> getTimeline(Integer userID);

    /**
     * Get a list of all unique user IDs that follow at least one other user.
     * @return list of user IDs
     */
    public List<Integer> getUsers();

    /**
     * Get a list of user IDs that follow the given user ID
     * @param userID given user ID
     * @return list of user IDs that follow the given user ID
     */
    public List<Integer> getFollowers(Integer userID);

    /**
     * Get a list of user IDs that the given user ID follows
     * @param userID given user ID
     * @return list of user IDs that the given user ID follows
     */
    public List<Integer> getFollowees(Integer userID);

    /**
     * Set connection settings
     * @param url database connector URL
     * @param user database username
     * @param password database password
     */
    public void authenticate(String url, String user, String password);

    /**
     * Close the connection when application finishes
     */
    public void closeConnection();


}
