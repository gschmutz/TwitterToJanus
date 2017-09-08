/**
 * Copyright 2013 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package kafka.sample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.sample.producer.AvroTweetProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import avro.sample.twitter.TwitterStatusUpdateConverter;
import ch.trivadis.sample.twitter.avro.v1.TwitterStatusUpdate;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;
import com.twitter.hbc.twitter4j.handler.StatusStreamHandler;
import com.twitter.hbc.twitter4j.message.DisconnectMessage;
import com.twitter.hbc.twitter4j.message.StallWarningMessage;

public class TwitterToKafka  {
	
	private final static Logger logger = LoggerFactory.getLogger(TwitterToKafka.class);

	List<String> followTerms = new ArrayList<String>();
	private int numberOfProcessingThreads = 5; 
	private Client hbcClient;
	private AvroTweetProducer kafkaProducer = new AvroTweetProducer();
	String consumerKey = null;
	String consumerSecret = null;
	String accessToken = null;
	String accessTokenSecret = null;

	public TwitterToKafka(List<String> followTerms, int numberOfProcessingThreads, 
							String consumerKey, String consumerSecret,
							String accessToken, String accessTokenSecret) {
		this.followTerms = followTerms;
		this.numberOfProcessingThreads = numberOfProcessingThreads;
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
	}

	public void start() {
		System.out.println("start() ...");
		// Create an appropriately sized blocking queue
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		
		// create the endpoint
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(followTerms);
		endpoint.stallWarnings(false);

		// create an authentication
		Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);

		// Create a new BasicClient. By default gzip is enabled.
		ClientBuilder builder = new ClientBuilder().name("sampleExampleClient")
				.hosts(Constants.STREAM_HOST)
				.endpoint(endpoint)
				.authentication(auth)
				.processor(new StringDelimitedProcessor(queue));
		hbcClient = builder.build();
		System.out.println("client created ...");

		// Create an executor service which will spawn threads to do the actual
		// work of parsing the incoming messages and
		// calling the listeners on each message
		ExecutorService service = Executors
				.newFixedThreadPool(this.numberOfProcessingThreads);

		// Wrap our BasicClient with the twitter4j client
		Twitter4jStatusClient t4jClient = new Twitter4jStatusClient(hbcClient,
				queue, Lists.newArrayList(listener), service);

		// Establish a connection
		t4jClient.connect();
		System.out.println("connection established ...");

		for (int threads = 0; threads < this.numberOfProcessingThreads; threads++) {
			// This must be called once per processing thread
			t4jClient.process();
			System.out.println("thread " + threads + " started ...");

		}
	};

	public void stop() {
		hbcClient.stop();
	};

	public boolean isRunning() {
		return true;
	};
	
	// A bare bones StatusStreamHandler, which extends listener and gives some
	// extra functionality
	private StatusListener listener = new StatusStreamHandler() {

		@Override
		public void onStatus(Status status) {
			if (status == null) {
				System.err.println("status is null");
			} else {

				try {
			        TwitterStatusUpdate statusAvro = TwitterStatusUpdateConverter.convert(status);

			        kafkaProducer.produce(statusAvro);
				} catch (Exception e) {
					logger.error("Error occured in onStatus()", e);
					e.printStackTrace();
				}
			}
		}

		@Override
		public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
			//too many, do not log
			logger.info("=====> onDeletionNotice: " + statusDeletionNotice);
		}

		@Override
		public void onTrackLimitationNotice(int limit) {
			logger.warn("=====> onTrackLimitationNotice: " + limit);
		}

		@Override
		public void onScrubGeo(long user, long upToStatus) {
			logger.warn("=====> onScrubGeo: " + user);
		}

		@Override
		public void onException(Exception e) {
			logger.error("=====> onException", e);
		}

		@Override
		public void onDisconnectMessage(DisconnectMessage message) {
			logger.error("=====> onDisconnectMessage: " + message);
		}

		@Override
		public void onUnknownMessageType(String s) {
			logger.warn("=====> onUnknownMessageType: " + s);
		}

		@Override
		public void onStallWarning(StallWarning arg0) {
			logger.warn("=====> onStallWarning: " + arg0);
		}

		@Override
		public void onStallWarningMessage(StallWarningMessage arg0) {
			logger.warn("=====> onStallWarningMessage: " + arg0);
		}

	};

	public static void main(String[] args) {
		
		// Twitter Terms to follow
		List<String> followTerms = new ArrayList<String>();
		followTerms.add("bondo");
		followTerms.add("bergsturzBondo");
		
		// Twitter Authentication
		String consumerKey = "y5w9TGTAEEqtLb0pOXqJSvPgM";
		String consumerSecret = "gKgSSoaJkWRKhh4QloE1kVHckZJ6oX8heDKLK17d0Nl7jNQwT8";
		String accessToken = "570980992-BhCt5sbywMh03UneJIYqH7iuMtkcFrIC7Pi2Nism";
		String accessTokenSecret = "XQv4DeAGUAjL55zqN0NpomM41slEHT2SDZTP1jb5O54TZ";
		
		
		// Number of threads to use for retrieving the tweets from Twitter
		int numberOfThreads = 3;
		TwitterToKafka twitterToKafka = new TwitterToKafka(followTerms,
		numberOfThreads, consumerKey, consumerSecret,
		accessToken, accessTokenSecret);
		twitterToKafka.start();
	}
	
	// Get Tweet structure
	// tweetstruct = Twitter structure as passed by the stream
	
	// Immediate parameters
	// [tweet_property_ID, tweet_ID_string] = tweetstruct[tweetID, ID_string]
	// [tweet_property_text, twitter_string] = tweetstruct[tweet_text, text_string]
	// [tweet_property_timestamp, timestamp] = tweetstruct[tweet_timestamp,timestamp_datetimeFormat]  //YYYY_MM_DD_hh_mm_ss
	// If geo_enabled
	// [tweet_property_coordinates,{long,lat}] = tweetstruct[tweet_coordinates_coordinates,{long_double,lat_double}]
	
	
	// [usertweet_property_edge_tweeted, user_ID_begin, twitter_property_ID_end] = tweetstruct
	
	
	// if isretweet

	
	// [user_property_ID, ID_string] = tweetstruct[userID, ID_string]
	// [user_property_location, {location_city_string, location_country_string}] = 
	// ..... tweetstruct[user_location, {location_city_string, location_country_string}]
	// [user_property_likes, nfollows] = tweetstruct[user_follows,follows_int]
	// [user_property_retweets, nretweets] = tweetstruct[user,followers_int]
	
	// [user_property_edge_following,user_ID_begin,user_ID_end] = tweetstruct[,]
	
	
	// Delayed parameters
	// [tweet_property_likes, nlikes] = tweetstruct[tweet_likes,likes_int]
	// [tweet_property_retweets, nretweets] = tweetstruct[tweet_retweets,retweets_int]
	// [tweet_property_replies, nreplies] = tweetstruct[tweet_replies,replies_int]
	
	// [tweet_property_edge_repliedwith,twitter_property_ID_begin,twitter_property_ID_end] = tweetstruct[,]
	
	// [usertweet_property_edge_repliedto,user_ID_begin,tweet_property_ID_end] = tweetstruct[,]
	// [usertweet_property_edge_liked, user_ID_begin, tweet_property_ID_end] = tweetstruct
	// [usertweet_property_edge_retweeted, user_ID_begin, tweet_property_ID_end] = tweetstruct


	
	// [tweet_property_edge_tagged,tweet_property_ID_begin, {hashtag_vtx1, hashtag_vtx2,....},{hashtag1_string, hashtag2_string,...}] = 
	//  .... tweetstruct[tweetID, ID_string, hashtaglist, {hashtag1_string, hashtag2_string,...}]
	// [,] = tweetstruct[,]

}
