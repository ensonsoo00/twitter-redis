package edu.northeastern.ds4300.twitter;

import java.util.Comparator;

/**
 * This class defines a comparator for Tweet objects based on the tweet's recency.
 */
public class TweetComparator implements Comparator<Tweet> {

    /**
     * Compares 2 Tweet object in terms of their tweet recency, which defines an ordering for Tweet objects. In this implementation,
     * more recent tweets precede less recent tweets. If the tweets have the same timestamp, then the tweet with the higher tweet ID precedes
     * the tweet with the lower tweet ID.
     * @param tweet1 the first tweet
     * @param tweet2 the second tweet
     * @return a negative value if tweet1 is more recent than tweet2; 0 if tweet1 has the same timestamp and tweet ID as tweet2;
     *          a positive value if tweet1 has less recent than tweet2
     */
    @Override
    public int compare(Tweet tweet1, Tweet tweet2) {
        // the tweet with the later timestamp precedes the other tweet
        int timestampCompare = tweet2.getTweetTimestamp().compareTo(tweet1.getTweetTimestamp());

        // if the tweets have the same timestamp then compare the tweet ID
        if (timestampCompare == 0) {
            return tweet2.getTweetID() - tweet1.getTweetID();
        }
        return timestampCompare;
    }
}
