package edu.northeastern.ds4300.twitter;

import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * This class is used to setup the Redis Twitter database by populating it with "follows/following" relationships between
 * users.
 */
public class TwitterRedisSetup {

    /**
     * Note: This method should be called before posting tweets or retrieving timelines.
     *
     * This method takes in 1 argument, which is the CSV filename of the "follows" table. This method initializes a new Redis
     * database and a current Tweet ID counter. Then it processes the given CSV file to create key-value pairs representing
     * "following/follower" relationships between users. A set of user IDs is also recorded, which is used for random user selection when
     * retrieving timelines.
     *
     * @param args the arguments required for the main() method
     *             args[0]: the CSV filename for the "follows" table
     */
    public static void main(String[] args) {

        // check if arguments is empty
        if (args.length == 0) {
            System.out.println("ERROR: must provide follows csv filename");
        }

        // initializing Jedis
        Jedis jedis = new Jedis();

        // clears all keys in the Redis database
        jedis.flushAll();

        // initialize the currTweetID to 0 in preparation for posting tweets
        // the currTweetID represents the tweet ID of the last posted tweet
        jedis.set("currTweetID", "0");


        String followsFilename = args[0];
        File follows = new File(followsFilename);
        try {
            Scanner sc = new Scanner(follows);
            if (sc.hasNextLine()) sc.nextLine(); // ignores the columns headers

            // reading and processing CSV file
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String[] follow = line.split(",");
                String userID = follow[0];
                String followID = follow[1];

                // "following" key represents the user, the values represent users that the key user follows
                jedis.lpush("following:" + userID, followID);

                // "followers" key represents the user, the values represent the users that follow the key user
                jedis.lpush("followers:" + followID, userID);

                // add the user ID to the set of users
                jedis.sadd("users", userID);
            }
        } catch (FileNotFoundException e) {
            System.out.println("ERROR: Could not find file: " + followsFilename);
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        System.out.println("Successfully loaded csv file");

        // close the connection when finished
        jedis.quit();
    }

}
