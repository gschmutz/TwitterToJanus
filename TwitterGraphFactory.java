

//Copyright 2017 JanusGraph Authors
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.


import com.google.common.base.Preconditions;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import java.io.File;

/**
* Example Graph factory that creates a {@link JanusGraph} based on roman mythology.
* Used in the documentation examples and tutorials.
*
* @author Marko A. Rodriguez (http://markorodriguez.com)
*/
public class TwitterGraphFactory {

 public static final String INDEX_NAME = "search";
 private static final String ERR_NO_INDEXING_BACKEND = 
         "The indexing backend with name \"%s\" is not defined. Specify an existing indexing backend or " +
         "use TwitterGraphFactory.loadWithoutMixedIndex(graph,true) to load without the use of an " +
         "indexing backend.";

 public static JanusGraph create(final String directory) {
     JanusGraphFactory.Builder config = JanusGraphFactory.build();
     config.set("storage.backend", "cassandra");
     config.set("storage.directory", directory);
     config.set("index." + INDEX_NAME + ".backend", "elasticsearch");

     JanusGraph graph = config.open();
     TwitterGraphFactory.load(graph);
     return graph;
     
// 	Load graph or create one
// 	load(graphobjectname)        
// 
// 	if (null = graphobjectanme) // Create new graph
// 	      
// 	     graph = empty graphobject
// 	     existingvector.tweet = empty vector
//       existingvector.user = empty vector
// 	     existingvector.hashtag = empty vector
//       existingvector.url = empty vector
// 	     existingvector.location = empty vector
// 	else
// 	     // Get graph structure
// 	     graph = graphobjectname.graph
// 	     // Get structure with existing elements
// 	     existingvector = graphobjectname.existingvector
// 	end
// 	
// 	    // Get index vectors for vertices (for faster retrieval and inserts)
// 	     existingvector_tweet =  existingvector.tweet
// 	     existingvector_user =  existingvector.user
// 	     existingvector_hashtag =  existingvector.hashtag
//       existingvector_url = existingvectorl.url
// 	     existingvector_location = existingvector.location
// 	
// 	    // Get lengths of index vectors
// 	    length_existingvector_tweet = length(existingvector_tweet)
// 	    length_existingvector_user = length(existingvector_user)
// 	    length_existingvector_hashtag = length(existingvector_hashtag)
//      length_existingvector_url = length(existingvector_url)
// 	    length_existingvector_location = length(existingvector_location)
        
     
 }

 public static void loadWithoutMixedIndex(final JanusGraph graph, boolean uniqueNameCompositeIndex) {
     load(graph, null, uniqueNameCompositeIndex);
 }

 public static void load(final JanusGraph graph) {
     load(graph, INDEX_NAME, true);
 }

 private static boolean mixedIndexNullOrExists(StandardJanusGraph graph, String indexName) {
     return indexName == null || graph.getIndexSerializer().containsIndex(indexName) == true;
 }

 /**
  * @param graph
  * @param mixedIndexName
  * @param uniqueNameCompositeIndex
  */
 /**
  * @param graph
  * @param mixedIndexName
  * @param uniqueNameCompositeIndex
  */
 public static void load(final JanusGraph graph, String mixedIndexName, boolean uniqueNameCompositeIndex) {
     if (graph instanceof StandardJanusGraph) {
         Preconditions.checkState(mixedIndexNullOrExists((StandardJanusGraph)graph, mixedIndexName), 
                 ERR_NO_INDEXING_BACKEND, mixedIndexName);
     }

     // Create Schema
     JanusGraphManagement mgmt = graph.openManagement();
     
     //-----------------
     // Tweet parameters
     //-----------------
     // this.tweetId 
     final PropertyKey tweetId = mgmt.makePropertyKey("tweetId").dataType(Long.class).make();
     JanusGraphManagement.IndexBuilder tweetIdIndexBuilder = mgmt.buildIndex("tweetId", Vertex.class).addKey(tweetId);
     
     if (uniqueNameCompositeIndex)
         tweetIdIndexBuilder.unique();
     JanusGraphIndex tweetId_i = tweetIdIndexBuilder.buildCompositeIndex();
     mgmt.setConsistency(tweetId_i, ConsistencyModifier.LOCK);
     
     // this.createdAt  
     // final PropertyKey timeCreated = mgmt.makePropertyKey("timeCreated").dataType(SimpleDateFormat.class).make(); 
     // this.createdAtAsLong 
     final PropertyKey timeCreated = mgmt.makePropertyKey("timeCreated").dataType(Long.class).make(); 
     // this.text
     final PropertyKey text = mgmt.makePropertyKey("text").dataType(String.class).make();
     // this.isRetweet
     final PropertyKey isRetweet = mgmt.makePropertyKey("isRetweet").dataType(Boolean.class).make(); 
     // this.retweetCount
     final PropertyKey countRetweet = mgmt.makePropertyKey("countRetweet").dataType(Integer.class).make(); 
     // this.coordinatesLatitude
     // this.coordinatesLongitude
     final PropertyKey coordinates = mgmt.makePropertyKey("coordinates").dataType(Geoshape.class).make();
     // this.hashtagEntities
     final PropertyKey hashtag = mgmt.makePropertyKey("hashtagstring").dataType(String.class).make();
     // this.urlEntities 
     final PropertyKey url = mgmt.makePropertyKey("urlstring").dataType(String.class).make();
     // this.userMentionEntities  
     final PropertyKey usermention = mgmt.makePropertyKey("usermentionstring").dataType(String.class).make();
     // this.replyCount
     // final PropertyKey countReply = mgmt.makePropertyKey("countReply").dataType(Integer.class).make(); 	
     // this.likeCount
     // final PropertyKey countLike = mgmt.makePropertyKey("countLike").dataType(Integer.class).make();

     //-----------------
     // User parameters
     //-----------------
     // this.User.Id
     final PropertyKey userId = mgmt.makePropertyKey("userId").dataType(Long.class).make();
     JanusGraphManagement.IndexBuilder userIdindexBuilder = mgmt.buildIndex("userId", Vertex.class).addKey(userId);

     if (uniqueNameCompositeIndex)	
         userIdindexBuilder.unique();
     JanusGraphIndex userId_i = userIdindexBuilder.buildCompositeIndex();
     mgmt.setConsistency(userId_i, ConsistencyModifier.LOCK);
 
     // this.User.screenName
     final PropertyKey screenName = mgmt.makePropertyKey("screenName").dataType(String.class).make();
     // this.User.followerCount
     final PropertyKey countFollowers = mgmt.makePropertyKey("countFollowers").dataType(Integer.class).make();
     // this.User.friendsCount
     final PropertyKey countFollowing = mgmt.makePropertyKey("countFollowing").dataType(Integer.class).make(); 
     
     
     // Include all extra parameters, no matter which edge type (tweet, user, location,...)
     if (null != mixedIndexName)
    	mgmt.buildIndex("vertices", Vertex.class).addKey(timeCreated).addKey(text).addKey(isRetweet).addKey(countRetweet).buildMixedIndex(mixedIndexName); 

     // Is this correct? timeCreated already here or in EdgeLabel tweeted = mgmt.makeEdgeLabel("tweeted").signature(timeCreated).make();
     if (null != mixedIndexName)
     mgmt.buildIndex("edges", Edge.class).addKey(timeCreated).buildMixedIndex(mixedIndexName);
    	 
     // Vertex types
     mgmt.makeVertexLabel("tweet").make();
     mgmt.makeVertexLabel("location").make();
     mgmt.makeVertexLabel("hashtag").make();
     mgmt.makeVertexLabel("url").make();
     mgmt.makeVertexLabel("user").make();
        
     // Edge types
     mgmt.makeEdgeLabel("tweeted").multiplicity(Multiplicity.MANY2ONE).make();
     EdgeLabel tweeted = mgmt.makeEdgeLabel("tweeted").signature(timeCreated).make();
     mgmt.buildEdgeIndex(tweeted, "tweetsByTime", Direction.IN, Order.decr, timeCreated);     
     mgmt.makeEdgeLabel("tweetedby").make();
     mgmt.makeEdgeLabel("located").make();
     // mgmt.makeEdgeLabel("liked").multiplicity(Multiplicity.MANY2ONE).make();
     // mgmt.makeEdgeLabel("repliedTo").multiplicity(Multiplicity.MANY2ONE).make();
     // mgmt.makeEdgeLabel("repliedWith").make();
     // mgmt.makeEdgeLabel("following").make();
     
     mgmt.makeEdgeLabel("tagged").multiplicity(Multiplicity.MANY2ONE).make();
     mgmt.makeEdgeLabel("linked").multiplicity(Multiplicity.MANY2ONE).make();    
     mgmt.makeEdgeLabel("mentioned").multiplicity(Multiplicity.MANY2ONE).make(); 
     
     mgmt.commit();

     JanusGraphTransaction tx = graph.newTransaction();
     	   	    
  
     // Do while kafka stream is messages are in kafka stream
  
     	// get(tweetobject)
     	
        // Validate if schema of tweetobject is correct 
     
        // if yes, continue, else throw error
     
     	// feedGraph(tweetobject)
     
    	// Get tweetobject from kafka connector - fields according to TwitterStatusUpdate.java
     
     	
     	// tweetindex = find(tweetobject.tweetId, existingvector_tweet)
   	
     	// if (null = tweetindex)
     	// 		
        //		Set retweetflag = 0
     	//
     	// 		Vertex v_tweet  = tx.addVertex(T.label, "tweet", "tweetId", tweetobject.tweetId, "timeCreated", tweetobject.createdAt, "isRetweet", tweetobject.isRetweet, "countRetweet",tweetobject.retweetCount);

        //      Update vector with existing tweet IDs
     	//      length_existingvector_tweet = length_existingvector_tweet + 1;
     	//      existingvector_tweet(length_existingvector_tweet) = tweetobject.tweetId;
     	
     	// 		if (null != tweetobject.tweetId_repliedto)
     	
     	//       	tweetId_repliedto =  find(tweetobject.tweetId_repliedto, existingvector_tweet)
     	//       	v_tweet_repliedto  = g.V().has('tweetId',tweetId_repliedto).next()
     	//       	v_tweet_repliedto.addEdge("repliedwith", v_tweet)  			
     	//  	end
    		
        // 		if (null != tweetobject.coordinatesLatitude)
     
     	// 			cooindex = find({tweetobject.coordinatesLatitude,tweetobject.coordinatesLongitude} , existingvector_location) // introduce tolerance radius
     	//		
     	//			if (null = cooindex)
     	
     	// 				Vertex v_location  = tx.addVertex(T.label, "location", "coordinates", Geoshape.point(tweetobject.coordinatesLatitude, tweetobject.coordinatesLongitude));
     	     
     	//      		Update vector with existing locations
        //      		length_existingvector_location =  length_existingvector_location + 1;
     	//      		existingvector_location(length_existingvector_location) = {tweetobject.coordinatesLatitude; tweetobject.coordinatesLongitude};
     	// 			else
     	//				v_location = g.V().has('coordinates',{existingvector_location(cooindex,1),existingvector_location(cooindex,2)}).next()
     	//			end
     
     	// 			v_tweet.addEdge("located", v_location)
     	// 		end
     	
     	// 		if (null != tweetobject.hashtagEntities)
	
     	//    		do while i <= length(tweetobject.hashtagEntities)
     			    		
     	//    			hashtag_i = tweetobject.hashtagEntities(i)
     	//    			hasthtagindex =  find(hashtag_i, existingvector_hashtag)
     	
     	//    			if (null = hashtagindex)
     	
     	//				v_hashtag = tx.addVertex(T.label, "hashtag", "hashtagstring", hashtag_i)
     	//				
     	//    		else
     	//				v_hashtag  = g.V().has('hashtagstring', hashtag_i).next()
     	//			end

     	// 			v_tweet.addEdge("tagged", v_hashtag)
     	//			i = i + 1

     	//      	Update vector with existing hashtag IDs
     	//      	length_existingvector_hashtag = length_existingvector_hashtag + 1;
     	//      	existingvector_tweet(length_existingvector_hashtag) = hashtag_i;

     	//    	end
     
     	// 		same for tweetobject.urlEntities
     	// 		same for tweetobject.userMentionEntities
     	// 
     	// else
     	//     set retweetflag = 1
     	//     v_tweet = existingvector_tweet(tweetindex);
     	// end
     	
  			
     	// -------------------------------------
     	// User-related stuff 
     	// -------------------------------------   	
     	// userindex = find(tweetobject.User.Id, existingvector_user);
   			
     	// if (null = userindex)
     	// 		Vertex v_user  = tx.addVertex(T.label, "user", "userId", tweetobject.User.Id,"screenName", tweetobject.User.screenName, "countFollowers", tweetobject.User.followersCount, "countFollowing",tweetobject.User.friendsCount);

        //      Update vector with existing user IDs
     	//      length_existingvector_user =   length_existingvector_user + 1;
     	//      existingvector_user(length_existingvector_user) = tweetobject.User.Id;
  			
     	//  else
     			
     	//		v_user = existingvector_user(userindex);
     	//	end
     	//	
     	// if retweetflag = 0
     	// 		v_user.addEdge("tweeeted", v_tweet)
     
     	// 		if (null != tweetobject.isReply)
     	// 			v_user.addEdge("repliedto", v_tweet);
     	// 		end
     	// else
     	// 		v_user.addEdge("retweeted", v_tweet)
     	// end
     
     
     	// if (null != tweetobject.User.location)

     	// 		cooindex = find({tweetobject.User.location.coordinatesLatitude,tweetobject.User.location.coordinatesLongitude} , existingvector_location) // introduce tolerance radius
     	//		
     	//		if (null = cooindex)

     	// 			Vertex v_location  = tx.addVertex(T.label, "location", "coordinates", Geoshape.point(tweetobject.coordinatesLatitude, tweetobject.coordinatesLongitude));

     	//      	Update vector with existing locations
     	//      	length_existingvector_location =  length_existingvector_location + 1;
     	//      	existingvector_location(length_existingvector_location) = {tweetobject.User.location.coordinatesLatitude; tweetobject.User.location.coordinatesLongitude};
     	// 		else
     	//			v_location = g.V().has('coordinates',{existingvector_location(cooindex,1),existingvector_location(cooindex,2)}).next()
     	//		end

     	// 		v_user.addEdge("located", v_location)
     	// 	end
     
     
     //end while 

     // commit the transaction to disk
     tx.commit();
 }

 /**
  * Calls {@link JanusGraphFactory#open(String)}, passing the JanusGraph configuration file path
  * which must be the sole element in the {@code args} array, then calls
  * {@link #load(org.janusgraph.core.JanusGraph)} on the opened graph,
  * then calls {@link org.janusgraph.core.JanusGraph#close()}
  * and returns.
  * <p/>
  * This method may call {@link System#exit(int)} if it encounters an error, such as
  * failure to parse its arguments.  Only use this method when executing main from
  * a command line.  Use one of the other methods on this class ({@link #create(String)}
  * or {@link #load(org.janusgraph.core.JanusGraph)}) when calling from
  * an enclosing application.
  *
  * @param args a singleton array containing a path to a JanusGraph config properties file
  */
 public static void main(String args[]) {
     if (null == args || 1 != args.length) {
         System.err.println("Usage: TwitterGraphFactory <janusgraph-config-file>");
         System.exit(1);
     }

     JanusGraphFactory.Builder config = JanusGraphFactory.build();
     config.set("storage.backend", "astyanax");
     config.set("storage.hostname", "127.0.0.1");
     config.set("storage.port", "9160");
     config.set("index." + INDEX_NAME + ".backend", "elasticsearch");
     
     JanusGraph g = config.open();

//     JanusGraph graph = config.open();
//     TwitterGraphFactory.load(graph);       
     
//     JanusGraph g = JanusGraphFactory.open(args[0]);
     load(g);
     g.close();
 }
}
