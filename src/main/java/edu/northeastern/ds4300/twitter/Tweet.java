package edu.northeastern.ds4300.twitter;


import java.util.Date;

/**
 * This class represents a Tweet containing data of the tweet ID, the tweet's user ID, the tweet timestamp, and tweet text contents.
 */
public class Tweet {

    private int tweetID;
    private int userID;
    private Date tweetTimestamp;
    private String tweetText;

    public Tweet(int userID, String tweetText) {
        this.tweetID = -1;
        this.userID = userID;
        this.tweetTimestamp = null;
        this.tweetText = tweetText;
    }

    public Tweet(int tweetID, int userID, Date tweetTimestamp, String tweetText) {
        this.tweetID = tweetID;
        this.userID = userID;
        this.tweetTimestamp = tweetTimestamp;
        this.tweetText = tweetText;
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "tweetID=" + tweetID +
                ", userID=" + userID +
                ", tweetTimestamp=" + tweetTimestamp +
                ", tweetText='" + tweetText + '\'' +
                '}';
    }

    /**
     * Gets the tweet ID for this Tweet object.
     * @return tweet ID
     */
    public int getTweetID() {
        return this.tweetID;
    }

    /**
     * Sets the tweet ID for this Tweet object to the given tweet ID.
     * @param tweetID new tweet ID
     */
    public void setTweetID(int tweetID) {
        this.tweetID = tweetID;
    }

    /**
     * Gets the user ID for this Tweet object.
     * @return user ID of the tweet
     */
    public int getUserID() {
        return this.userID;
    }

    /**
     * Sets the user ID for this Tweet object to the given user ID
     * @param userID new user ID
     */
    public void setUserID(int userID) {
        this.userID = userID;
    }

    /**
     * Gets the tweet timestamp of this Tweet object
     * @return tweet timestamp
     */
    public Date getTweetTimestamp() {
        return this.tweetTimestamp;
    }

    /**
     * Sets the tweet timestamp of this Tweet to the given timestamp
     * @param timestamp the new tweet timestamp
     */
    public void setTweetTimestamp(Date timestamp) {
        this.tweetTimestamp = timestamp;
    }

    /**
     * Gets the tweet text from this Tweet object.
     * @return tweet text
     */
    public String getTweetText() {
        return this.tweetText;
    }

    /**
     * Sets the tweet text of the Tweet object to the given text
     * @param tweetText new tweet text
     */
    public void setTweetText(String tweetText) {
        this.tweetText = tweetText;
    }
}
