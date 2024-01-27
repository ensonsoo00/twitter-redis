package edu.northeastern.ds4300.twitter;


import redis.clients.jedis.*;
import java.util.List;
public class JedisTester {
    public static void main(String[] args) {
        Jedis jedis = new Jedis();

        jedis.flushAll();
        jedis.set("hello", "world");
        jedis.set("foo", "10");
        jedis.incr("foo");
        jedis.lpush("friends", "joe");
        jedis.lpush("friends", "mary");
        List<String> friends = jedis.lrange("friends", 0, -1);

        String value = jedis.get("hello");
        String foo = jedis.get("foo");
        System.out.println(value + "\t" + foo + "\t" + friends.toString());

        long N = 100000;
        long startTime = System.nanoTime();
        for (int i = 0; i < N; i++)
            jedis.set("key:" + i, "" + i);
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1000000;
        System.out.println("Seconds: " + duration / 1000.0);
        System.out.println((float) N * 1000.0 / duration + " inserts per second");


    }

}
