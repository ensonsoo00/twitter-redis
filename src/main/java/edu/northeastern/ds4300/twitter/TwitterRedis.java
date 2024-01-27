package edu.northeastern.ds4300.twitter;
import redis.clients.jedis.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

/**
 * This class exercises the TwitterDatabaseAPI (Redis implementation Strategy 2).
 */
public class TwitterRedis {
    private static TwitterDatabaseAPI api = new TwitterDatabaseRedis();

    /**
     * Note:
     * - the TwitterRedisSetup.main() method should be called first to setup the Redis database (populate 'following/follower'
     *   relationships between users).
     * - the arguments are slightly different from the MySQL driver because it does not require a username and password to connect to the database.
     *
     * Based on the given arguments, this method will either post tweets into a database from a given tweets CSV file
     * or retrieve a given number of user home timelines. The runtime results of the corresponding commands will be displayed.
     *
     * This method requires a total of 2 arguments.
     * - The 1st argument should be either "post" (to insert tweets) or "retrieve" (get home timelines).
     * - The 2nd argument depends on the 1st argument. If "post" was the 1st argument, the 2nd argument should be the tweet CSV filename. If
     *      "retrieve" was the 1st argument, then the 2nd argument should be the number of home timelines to retrieve.
     *
     *
     * Examples of possible commands:
     * - "post res/tweet.csv" : insert tweets from the file "res/tweet.csv"
     * - "retrieve 1000" : retrieve 1000 user home timelines from the database
     * @param args the arguments required for the main() function
     *             args[0] : either "post" (posting tweets) or "retrieve" (retrieving timelines)
     *             args[1] : if args[0] == "post" then args[1] should be the tweets CSV filename
     *                        otherwise args[1] should be the number of iterations/timelines to retrieve
     *
     */
    public static void main(String[] args) {

        // checking if arguments is empty
        if (args.length == 0) {
            System.out.println("Error: should provide a command; either post tweets or retrieve timelines.");
            return;
        }

        // connect to Redis database
        api.authenticate(null, null, null);

        // checking arguments for "post" and "retrieve" commands
        if (args[0].equals("post")) {
            if (args.length < 2) {
                System.out.println("Error: Must provide csv filename.");
            } else {
                postTweets(new File(args[1]));
            }
        }
        else if (args[0].equals("retrieve")) {
            if (args.length < 2) {
                System.out.println("Error: Must provide number of iterations.");
            } else {
                int iterations = Integer.parseInt(args[1]);
                retrieveTimelines(iterations);
            }
        }
        else {
            System.out.println("Error: 1st argument must be either 'post' or 'retrieve'");
        }

        // close connection when finished
        api.closeConnection();
    }


    /**
     * Reads and processes tweets from a given CSV file and inserts them into a database (without batching). This function
     * also displays the runtime results at every 50,000 inserts and after all tweets from the file are inserted into the database.
     *
     * @param tweetCSV CSV file that contains tweet data
     */
    public static void postTweets(File tweetCSV) {
        int rows_inserted = 0; // tracking number of rows inserted
        long pre_timestamp = System.currentTimeMillis();
        try {
            Scanner sc = new Scanner(tweetCSV);
            if (sc.hasNextLine()) sc.nextLine(); // ignores the columns headers
            pre_timestamp = System.currentTimeMillis();

            // reading and processing CSV file
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String[] tweet = line.split(",");
                int userID = Integer.parseInt(tweet[0]);
                String tweetText = "";
                if (tweet.length > 1) tweetText = tweet[1]; // edge case for empty tweet text
                Tweet toPost = new Tweet(userID, tweetText); // creating Tweet object
                api.postTweet(toPost);
                rows_inserted++;
                // display runtime at every 50,000 inserted rows
                if (rows_inserted % 50000 == 0) {
                    double sec = (System.currentTimeMillis() - pre_timestamp) / 1000.0;
                    System.out.println(rows_inserted + " rows inserted at " + sec + " seconds: " + rows_inserted / sec + " tweets per second");
                }
            }
            sc.close();
        } catch (FileNotFoundException e) {
            System.out.println("Could not find provided csv file.");
            System.out.println(e.getMessage());
            e.printStackTrace();
            return;
        }
        long post_timestamp = System.currentTimeMillis();
        double runtime = (post_timestamp - pre_timestamp) / 1000.0; // calculate total runtime for inserting tweets

        // displaying runtime results
        System.out.println("Runtime to insert " + rows_inserted + " tweets: " + runtime + " seconds");
        System.out.println("Tweets inserted per second: " + rows_inserted / runtime);
    }

    /**
     * Retrieve a given number ("iterations") of home timelines from randomly selected users.
     * First, the API is called to generate a list of all unique user IDs to randomly select from. In each iteration, a
     * random user is selected from the list and the API is called to retrieve the home timeline for that randomly selected user.
     * The runtime results will be displayed at every 10,000 timelines retrieved and after all timelines have been retrieved.
     *
     * @param iterations number of timelines to retrieve
     */
    public static void retrieveTimelines(int iterations) {
        if (iterations == 0) return;
        Random rd = new Random();

        // API call to retrieve a list of all unique user IDs to randomly select from
        List<Integer> users = api.getUsers();
        int len = users.size();
        // cannot retrieve timelines if there is no user-following data
        if (len == 0) {
            System.out.println("Error: insufficient user-following data");
            return;
        }
        int counter = 0; // tracking number of timelines retrieved / API calls
        long pre_timestamp = System.currentTimeMillis();
        while (counter < iterations) {
            int random_user = users.get(rd.nextInt(len)); // randomly select a user ID from the list of user IDs
            api.getTimeline(random_user); // API call to retrieve home timeline of selected user
            counter++;

            // at every 10,000 home timelines retrieved, display the runtime
            if (counter % 10000 == 0) {
                double sec = (System.currentTimeMillis() - pre_timestamp) / 1000.0;
                System.out.println(counter + " timelines retrieved at " + sec + " seconds: " + counter / sec + " timelines per second");
            }
        }
        long post_timestamp = System.currentTimeMillis();
        double runtime = (post_timestamp - pre_timestamp) / 1000.0; // calculate total runtime for retrieving timelines

        // displaying final runtime results
        System.out.println(counter + " timelines retrieved");
        System.out.println("Runtime for retrieving " + counter + " timelines: " + runtime + " seconds");
        System.out.println("Timeline retrieved per second: " + counter / runtime);
    }
}
