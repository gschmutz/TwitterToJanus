
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

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import com.google.common.base.Preconditions;

/**
 * Example Graph factory that creates a {@link JanusGraph} based on roman
 * mythology. Used in the documentation examples and tutorials.
 *
 */
public class TwitterGraphFactory {

	public static final String INDEX_NAME = "search";
	private static final String ERR_NO_INDEXING_BACKEND = "The indexing backend with name \"%s\" is not defined. Specify an existing indexing backend or "
			+ "use TwitterGraphFactory.loadWithoutMixedIndex(graph,true) to load without the use of an "
			+ "indexing backend.";

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
			Preconditions.checkState(mixedIndexNullOrExists((StandardJanusGraph) graph, mixedIndexName),
					ERR_NO_INDEXING_BACKEND, mixedIndexName);
		}
		
		// Create Schema
		JanusGraphManagement mgmt = graph.openManagement();

		// -----------------
		// Tweet parameters
		// -----------------
		// this.tweetId
		final PropertyKey tweetId = mgmt.makePropertyKey("tweetId").dataType(Long.class).make();
		JanusGraphManagement.IndexBuilder tweetIdIndexBuilder = mgmt.buildIndex("tweetId", Vertex.class)
				.addKey(tweetId).unique();
		JanusGraphIndex tweetId_i = tweetIdIndexBuilder.buildCompositeIndex();
		mgmt.setConsistency(tweetId_i, ConsistencyModifier.LOCK);

		// this.createdAt
		// final PropertyKey timeCreated =
		// mgmt.makePropertyKey("timeCreated").dataType(SimpleDateFormat.class).make();
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
		// this.isReply
		// final PropertyKey isReply =
		// mgmt.makePropertyKey("isReply").dataType(Boolean.class).make();
		// this.tweetId_repliedTo
		// final PropertyKey tweetId_replyTo = mgmt.makePropertyKey("tweetId_replyTo").dataType(Long.class).make();
		// this.replyCount
		// final PropertyKey countReply =
		// mgmt.makePropertyKey("countReply").dataType(Integer.class).make();
		// this.likeCount
		// final PropertyKey countLike =
		// mgmt.makePropertyKey("countLike").dataType(Integer.class).make();

		// -----------------
		// User parameters
		// -----------------
		// this.User.Id
		final PropertyKey userId = mgmt.makePropertyKey("userId").dataType(Long.class).make();
		JanusGraphManagement.IndexBuilder userIdindexBuilder = mgmt.buildIndex("userId", Vertex.class).addKey(userId).unique();
		JanusGraphIndex userId_i = userIdindexBuilder.buildCompositeIndex();
		mgmt.setConsistency(userId_i, ConsistencyModifier.LOCK);

		// this.User.screenName
		final PropertyKey screenName = mgmt.makePropertyKey("screenName").dataType(String.class).make();
		// this.User.followerCount
		final PropertyKey countFollowers = mgmt.makePropertyKey("countFollowers").dataType(Integer.class).make();
		// this.User.friendsCount
		final PropertyKey countFollowing = mgmt.makePropertyKey("countFollowing").dataType(Integer.class).make();

		// Include all extra parameters
		if (null != mixedIndexName)
			mgmt.buildIndex("screenNameIx", Vertex.class).addKey(screenName).buildMixedIndex(mixedIndexName);
		
		// Vertex types
		mgmt.makeVertexLabel("tweet").make();
		mgmt.makeVertexLabel("location").make();
		mgmt.makeVertexLabel("hashtag").make();
		mgmt.makeVertexLabel("url").make();
		mgmt.makeVertexLabel("user").make();

		// Edge types
		mgmt.makeEdgeLabel("tweeted").multiplicity(Multiplicity.MANY2ONE).make();
		// EdgeLabel tweeted = mgmt.makeEdgeLabel("tweeted").signature(timeCreated).make();
		//mgmt.buildEdgeIndex(tweeted, "tweetsByTime", Direction.IN, Order.decr, timeCreated);
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
	}

	/**
	 * Calls {@link JanusGraphFactory#open(String)}, passing the JanusGraph
	 * configuration file path which must be the sole element in the {@code args}
	 * array, then calls {@link #load(org.janusgraph.core.JanusGraph)} on the opened
	 * graph, then calls {@link org.janusgraph.core.JanusGraph#close()} and returns.
	 * <p/>
	 * This method may call {@link System#exit(int)} if it encounters an error, such
	 * as failure to parse its arguments. Only use this method when executing main
	 * from a command line. Use one of the other methods on this class
	 * ({@link #create(String)} or {@link #load(org.janusgraph.core.JanusGraph)})
	 * when calling from an enclosing application.
	 *
	 * @param args
	 *            a singleton array containing a path to a JanusGraph config
	 *            properties file
	 */
	public static void main(String args[]) {
//		if (null == args || 1 != args.length) {
//			System.err.println("Usage: TwitterGraphFactory <janusgraph-config-file>");
//			System.exit(1);
//		}

		JanusGraphFactory.Builder config = JanusGraphFactory.build();
		config.set("storage.backend", "astyanax");
		config.set("storage.hostname", "127.0.0.1");
		config.set("storage.port", "9160");
		config.set("index." + INDEX_NAME + ".backend", "elasticsearch");

		JanusGraph g = config.open();
//		g.close();
//		org.janusgraph.core.util.JanusGraphCleanup.clear(g);

		load(g);
		g.close();
	}
}
