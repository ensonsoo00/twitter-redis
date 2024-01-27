package edu.northeastern.ds4300.twitter;


import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

/**
 * This class exercises the TwitterDatabaseAPI (MySQL implementation).
 */
public class TwitterMySQL {

    private static TwitterDatabaseAPI api = new TwitterDatabaseMysql();

    /**
     * <p> Based on the given arguments, this method will either post tweets into a database from a given tweets CSV file
     * or retrieve a given number of user home timelines. The runtime results of the corresponding commands will be displayed.</p>
     *
     * This method requires a total of 4 arguments.
     * <p>- The first 2 arguments should be the database user credentials (username and password).</p>
     * <p>- The 3rd argument should be either "post" (to insert tweets) or "retrieve" (get home timelines). </p>
     * <p>- The 4th argument depends on the 3rd argument. If "post" was the 3rd argument, the 4th argument should be the tweet CSV filename. If
     *      "retrieve" was the 3rd argument, then the 4th argument should be the number of home timelines to retrieve. </p>
     *
     * <p> Examples of possible commands: </p>
     * <p> - "user password post res/tweet.csv" : insert tweets from the file "res/tweet.csv" </p>
     * <p> - "user password retrieve 1000" : retrieve 1000 user home timelines from the database </p>
     * @param args the arguments required for the main() function
     *             <p> args[0] : database username </p>
     *             <p> args[1] : database password </p>
     *             <p> args[2] : either "post" (posting tweets) or "retrieve" (retrieving timelines) </p>
     *             <p> args[3] : if args[2] == "post" then args[3] should be the tweets CSV filename</p>
     *                        <p>otherwise args[3] should be the number of iterations/timelines to retrieve</p>
     *
     *
     */
    public static void main(String[] args) {

        // checking if username and password are provided in the function arguments
        if (args.length < 2) {
            System.out.println("Error: Must provide username and password to connect to database.");
            return;
        }

        // Authenticate your access to the server.
        String url = "jdbc:mysql://localhost:3306/twittertweets?serverTimezone=EST5EDT";
        String user = args[0];
        String password = args[1];

        api.authenticate(url, user, password);

        // checking arguments for "post" and "retrieve" commands
        if (args.length > 2) {
            if (args[2].equals("post")) {
                if (args.length < 4) {
                    System.out.println("Error: Must provide csv filename.");
                } else postTweets(new File(args[3]));
            } else if (args[2].equals("retrieve")) {
                if (args.length < 4) {
                    System.out.println("Error: Must provide number of iterations.");
                } else {
                    int iterations = Integer.parseInt(args[3]);
                    retrieveTimelines(iterations);
                }
            } else {
                System.out.println("Error: third argument must be either 'post' or 'retrieve'");
            }
        }
        else {
            System.out.println("Error: should provide a command; either post tweets or retrieve timelines");
        }

        api.closeConnection();
    }

    /**
     * Reads and processes tweets from a given CSV file and inserts them into a database (with batch size 5). At the end, if there are tweets
     * remaining (batch size less than 5), they are inserted afterwards. This function also displays the runtime results at every 50,000
     * inserts and after all tweets from the file are inserted into the database.
     *
     * @param tweetCSV CSV file that contains tweet data
     */
    public static void postTweets(File tweetCSV) {
        int rows_inserted = 0; // tracking number of rows inserted
        int api_calls = 0; // tracking number of API calls
        long pre_timestamp = System.currentTimeMillis();
        try {
            Scanner sc = new Scanner(tweetCSV);
            if (sc.hasNextLine()) sc.nextLine(); // ignores the columns headers
            List<Tweet> tweetBatch = new ArrayList<>(); // batch size = 5
            pre_timestamp = System.currentTimeMillis();

            // reading and processing CSV file
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String[] tweet = line.split(",");
                int userID = Integer.parseInt(tweet[0]);
                String tweetText = "";
                if (tweet.length > 1) tweetText = tweet[1]; // edge case for empty tweet text
                Tweet toInsert = new Tweet(userID, tweetText); // creating Tweet object
                tweetBatch.add(toInsert);

                // when the list has 5 tweets, call API to post the tweets list
                if (tweetBatch.size() == 5) {
                    api.postTweets(tweetBatch);
                    api_calls++;
                    rows_inserted += 5;
                    tweetBatch = new ArrayList<>(); // create new list after tweets are posted

                    // display runtime at every 50,000 inserted rows
                    if (rows_inserted % 50000 == 0) {
                        double sec = (System.currentTimeMillis() - pre_timestamp) / 1000.0;
                        System.out.println(rows_inserted + " rows inserted at " + sec + " seconds: " + rows_inserted / sec + " tweets per second");
                        System.out.println("\t" + api_calls + " API calls at " + sec + " seconds: " + api_calls / sec + " API calls per second");
                    }
                }
            }
            sc.close();
            // if there are tweets that have not yet been posted (i.e. when tweets remaining is less than 5), call API to post remaining tweets
            if (!tweetBatch.isEmpty()) {
                api.postTweets(tweetBatch);
                api_calls++;
                rows_inserted += tweetBatch.size();
            }
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
        System.out.println("API calls: " + api_calls);
        System.out.println("API calls per second: " + api_calls / runtime);
    }

    /**
     * Retrieve a given number ("iterations") of home timelines from randomly selected users.
     * <p>First, the API is called to generate a list of
     * all unique user IDs to randomly select from. In each iteration, a random user is selected from the list and the API is called to
     * retrieve the home timeline for that randomly selected user. </p>
     * <p>The runtime results will be displayed at every 100 timelines retrieved and after all
     * timelines have been retrieved.</p>
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

            // at every 100 home timelines retrieved, display the runtime
            if (counter % 100 == 0) {
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

