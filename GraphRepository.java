package ch.casbd.graph;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;

import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import ch.trivadis.sample.twitter.avro.v1.TwitterStatusUpdate;
import ch.trivadis.sample.twitter.avro.v1.TwitterHashtagEntity;
// URLs not returned in tweet
import ch.trivadis.sample.twitter.avro.v1.TwitterURLEntity;
import ch.trivadis.sample.twitter.avro.v1.TwitterUserMentionEntity;

public class GraphRepository {
	public static final String INDEX_NAME = "search";
	private JanusGraph graph = null;
	
	public GraphRepository() {
        JanusGraphFactory.Builder config = JanusGraphFactory.build();
        config.set("storage.backend", "astyanax");
        config.set("storage.hostname", "127.0.0.1");
        config.set("storage.port", "9160");
        config.set("index." + INDEX_NAME + ".backend", "elasticsearch");

        graph = config.open();
	}
	
	public void persist(TwitterStatusUpdate tweet) {
        JanusGraphTransaction tx = graph.newTransaction();

        
//        this.createdAt = createdAt;
//        this.createdAtAsLong = createdAtAsLong;
//        this.tweetId = tweetId;
//        this.retweetCount = retweetCount;
//        this.text = text;
//        this.isRetweet = isRetweet;
//        this.coordinatesLatitude = coordinatesLatitude;
//        this.coordinatesLongitude = coordinatesLongitude;
//        this.hashtagEntities = hashtagEntities;
//        this.urlEntities = urlEntities;
//        this.userMentionEntities = userMentionEntities;
//        this.User = User;
        
        // Initialize vertices (necessary?)
        Vertex v_tweet = tx.addVertex();
        Vertex v_location = tx.addVertex();
        Vertex v_hashtag = tx.addVertex();
        Vertex v_url = tx.addVertex();
        Vertex v_user = tx.addVertex();

        
        // Check if the tweet vertex is already created 
        boolean b_tweet = tx.traversal().V().has("tweet", "tweetId", tweet.getTweetId()).hasNext();
        System.out.println(b_tweet);       
   	
     	if (!b_tweet){

     		v_tweet  = tx.addVertex(T.label,"tweet", "tweetId", tweet.getTweetId(), 
     				"timeCreated", tweet.getCreatedAtAsLong(), 
     				"isRetweet", tweet.getIsRetweet(), 
     				"countRetweet",tweet.getRetweetCount());

     		// Check coordinates and add (werden die Koordinaten immer angegeben, also nicht leeres Feld, wenn nicht angegeben?)
     		if (tweet.getCoordinatesLatitude() > 0.0d && tweet.getCoordinatesLongitude() > 0.0d) {
     			System.out.println("===> has coordinates " + tweet.getCoordinatesLatitude() + " " + tweet.getCoordinatesLongitude());

     			// Check if a location vertex with same values is already created
     			boolean b_location = tx.traversal().V().has("location", "coordinates", tweet.getTweetId()).hasNext();
     			System.out.println(b_location);


     			if (!b_location){

     				// Insert new vertex
     				v_location  = tx.addVertex(T.label, "location", "coordinates", Geoshape.point(tweet.getCoordinatesLatitude(), tweet.getCoordinatesLongitude()));

     			}	
     			else{

     				// Get vertex if existing
     				v_location = tx.traversal().V().has("location", "coordinates",Geoshape.point(tweet.getCoordinatesLatitude(), tweet.getCoordinatesLatitude())).next();;
     			}

     			v_tweet.addEdge("located", v_location);
     		}    	 				

     		// Check if tweet has hashtags
     		if (!tweet.getHashtagEntities().isEmpty()){

     			// Loop over hashtags
     			for (TwitterHashtagEntity hashtagEntity : tweet.getHashtagEntities()) 
     			{
     				System.out.println("===> Hashtag:" + hashtagEntity.getText());

     				// Check if a hashtag is already in the graph
     				boolean b_hashtag = tx.traversal().V().has("hashtag", "hashtagstring", hashtagEntity.getText()).hasNext();
     				System.out.println(b_hashtag);

     				if (!b_hashtag){

     					// Insert new hashtag vertex
     					v_hashtag = tx.addVertex(T.label,"hashtag", "hashtagstring", hashtagEntity.getText());

     				}	
     				else{

     					// Get vertex if existing
     					v_hashtag  = tx.traversal().V().has("hashtag", "hashtagstring", hashtagEntity.getText()).next();

     				}
     				v_tweet.addEdge("tagged", v_hashtag);
     			}
     		}

     		// Check if tweet has URLs
     		if (!tweet.getUrlEntities().isEmpty()){

     			// Loop over hashtags
     			for (TwitterURLEntity urlEntity : tweet.getUrlEntities()) 
     			{
     				System.out.println("===> URL:" + urlEntity.getDisplayURL());

     				// Check if a hashtag is already in the graph
     				boolean b_url = tx.traversal().V().has("url", "urlstring", urlEntity.getDisplayURL()).hasNext();
     				System.out.println(b_url);

     				if (!b_url){

     					// Insert new url vertex
     					v_url  = tx.addVertex(T.label,"url", "urlstring", urlEntity.getDisplayURL());

     				}	
     				else{

     					// Get vertex if existing
     					v_url  = tx.traversal().V().has("url", "urlstring", urlEntity.getDisplayURL()).next();

     				}
     				v_tweet.addEdge("linked", v_url);
     			}
     		}
     		
     		// Check if tweet has user mentions
     		if (!tweet.getUserMentionEntities ().isEmpty()){

     			// Loop over mentioned users
     			for (TwitterUserMentionEntity userMentionEntity : tweet.getUserMentionEntities()) 
     			{
     				System.out.println("===> mentioned user:" + userMentionEntity.getScreenName());

     				// Check if a user is already in the graph
     				boolean b_user_m = tx.traversal().V().has("url", "urlstring", userMentionEntity.getScreenName()).hasNext();
     				System.out.println(b_user_m);

     				if (!b_user_m){

     					// Insert new user vertex
     					v_user  = tx.addVertex(T.label,"user", "screenName", userMentionEntity.getScreenName());

     				}	
     				else{

     					// Get vertex if existing
     					v_user  = tx.traversal().V().has("user", "screenName", userMentionEntity.getScreenName()).next();

     				}
     				v_tweet.addEdge("mentioned", v_user);
     			}
     		}
     	}
     	
     	else{

     		v_tweet = tx.traversal().V().has("tweet", "tweetId", tweet.getTweetId()).next();
     		System.out.println("====> Vertex " + v_tweet.value("tweetId"));
     	}
     			
     	// -------------------------------------
     	// User-related stuff 
     	// -------------------------------------   	
     	// Check if user is existing
        boolean b_user = tx.traversal().V().has("user", "screenName", tweet.getUser().getScreenName().toString()).hasNext();
        System.out.println(b_user);
   			
     	if (!b_user){
     		
     	 	v_user  = tx.addVertex(T.label, "user", "screenName", tweet.getUser().getScreenName().toString(),"countFollowers", tweet.getUser().getFollowersCount(), "countFollowing",tweet.getUser().getFriendsCount());
     	}	
     	else{
     			
     		v_user = tx.traversal().V().has("user", "screenName", tweet.getUser().getScreenName().toString()).next();
     	}
     	
     	v_user.addEdge("tweeted", v_tweet);
  
        // commit the transaction to disk
        tx.commit();

	}	
}
