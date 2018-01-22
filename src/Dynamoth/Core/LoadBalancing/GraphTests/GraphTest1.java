package Dynamoth.Core.LoadBalancing.GraphTests;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.LoadAnalyzing.Channel;
import Dynamoth.Core.LoadAnalyzing.SliceStats;
import Dynamoth.Core.LoadBalancing.ChannelRelationGraphBuilder;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.DiscreteLoadEvaluator;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.LoadEvaluator;
import Dynamoth.Core.Util.RPubHostInfo;

public class GraphTest1 {

	private final int HOST_COUNT = 10;
	private final int CLIENT_COUNT = 1000;
	private final int CHANNEL_PER_HOST = 1000;
	private final int SUBSCRIBERS_PER_CHANNEL = 20;
	private final int PUBLISHERS_PER_CHANNEL = 2;
	
	private Random random = new Random();
	
	private int randomClient() {
		return random.nextInt(CLIENT_COUNT);
	}
	
	private int randomChannel() {
		return random.nextInt(CHANNEL_PER_HOST);
	}
	
	public void run() {
		// Build a dummy graph
		
		Map<RPubClientId, RPubHostInfo> hostInfoMap = new HashMap<RPubClientId, RPubHostInfo>();
		Map<RPubClientId, Map<Integer, Long>> measuredByteIn = new HashMap<RPubClientId, Map<Integer,Long>>();
		Map<RPubClientId, Map<Integer, Long>> measuredByteOut = new HashMap<RPubClientId, Map<Integer,Long>>();
		Map<RPubClientId, Integer> clientLastUpdateTimes = new HashMap<RPubClientId, Integer>();
		Map<RPubClientId, Map<String, Channel>> channels = new HashMap<RPubClientId, Map<String,Channel>>();
		
		// Fill in dummy data
		
		// For each hosts
		for (int i=0; i<HOST_COUNT; i++) {
			RPubClientId clientId = new RPubClientId(i);
			
			// === Create host info ===
			hostInfoMap.put(clientId, new RPubHostInfo(clientId, "hostname-" + i, 1, 6000L, 6000L));
			
			// === Create Measured ByteIn ===
			Map<Integer, Long> byteInMap = new HashMap<Integer, Long>();
			byteInMap.put(0, 5000L);
			measuredByteIn.put(clientId, byteInMap);
			
			// === Create Measured ByteOut ===
			Map<Integer, Long> byteOutMap = new HashMap<Integer, Long>();
			byteOutMap.put(0, 5000L);
			measuredByteOut.put(clientId, byteOutMap);
			
			// === Create ClientLastUpdateTimes ===
			clientLastUpdateTimes.put(clientId, 0);
			
			// === Create channels ===
			Map<String, Channel> ch = new HashMap<String, Channel>();
			channels.put(clientId, ch);
			
			// For each channel
			for (int j=0; j<CHANNEL_PER_HOST; j++) {
				Channel channel = new Channel("tile_" + j); 
				ch.put(channel.getChannelName(), channel);
				
				// Put one slice stats
				channel.initializeSliceStats(0);
				SliceStats stats = channel.getSliceStats(0);
				
				// +++ FILL IN SLICE STATS +++
				
				// Subscribers
				for (int k=0; k<SUBSCRIBERS_PER_CHANNEL; k++)
					stats.getSubscriberList().add(new RPubNetworkID(randomClient()));
				stats.setSubscribers(stats.getSubscriberList().size());
				
				// Publishers | Not used |
				/*for (int k=0; k<PUBLISHERS_PER_CHANNEL; k++) {
					//stats.getpu
				}*/
				
				// General publication stats
				stats.getPublicationStats().setByteIn(1000L);
				stats.getPublicationStats().setByteOut(1000L);
				stats.getPublicationStats().setPublications(10);
				stats.getPublicationStats().setSentMessages(100);
			}
		}
		
		// Generate load evaluator
		LoadEvaluator evaluator = new DiscreteLoadEvaluator(hostInfoMap, channels, measuredByteIn, measuredByteOut, clientLastUpdateTimes, 0);
		
		// Generate the graph
		ChannelRelationGraphBuilder graph = new ChannelRelationGraphBuilder(channels, evaluator, 0);
		
		//System.out.println(graph.generateOutput());
		try {
			String metisOutput = graph.generateMetisOutput();
			System.out.println(metisOutput);
			PrintWriter writer = new PrintWriter("/home/julien/Bureau/dynamoth_tmp/dynamoth.metis");
			writer.println(graph.generateMetisOutput());
			writer.flush();
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		GraphTest1 gt1 = new GraphTest1();
		gt1.run();
	}

}
