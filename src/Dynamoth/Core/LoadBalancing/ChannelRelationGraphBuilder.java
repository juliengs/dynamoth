package Dynamoth.Core.LoadBalancing;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.LoadAnalyzing.Channel;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.LoadEvaluator;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Core.Manager.Plan.PlanId;
import Dynamoth.Core.Manager.Plan.PlanImpl;
import Dynamoth.Core.Manager.Plan.PlanMappingImpl;

public class ChannelRelationGraphBuilder {

	// Raw channel info for all hosts - Includes capacity
	private Map<RPubClientId, Map<String, Channel>> channels;
	
	// Load balancer
	private LoadEvaluator loadEvaluator;
	
	// List of all channels
	private List<Channel> channelList = new LinkedList<Channel>();
	
	// List of nodes
	private Set<String> nodes = new HashSet<String>();
	private Map<ChannelPair, Integer> pairMultiplicity = new HashMap<ChannelPair, Integer>();
	
	// Time point for this LoadEvaluator
	private int time;
	
	// Misc flags
	private static boolean IGNORE_UNICAST_AND_SUBSCRIBE = true;
	
	public ChannelRelationGraphBuilder(Map<RPubClientId, Map<String, Channel>> channels, LoadEvaluator loadEvaluator, int time) {
		this.channels = channels;
		this.loadEvaluator = loadEvaluator;
		this.time = time;
		
		// Build the graph
		buildGraph();
	}

	private void buildGraph() {
		// Build our list of channels
		
		// Iterate over all values
		for (Map<String, Channel> map: this.channels.values()) {
			// Add all channels
			
			// TODO: deal with channels spread over multiple redis instances (replicated)
			
			// Add all but ignore if some channels should be ignored
			for (Map.Entry<String, Channel> entry: map.entrySet()) {
				if (IGNORE_UNICAST_AND_SUBSCRIBE) {
					if (entry.getKey().startsWith("unicast") == false && entry.getKey().startsWith("sub") == false) {
						if (entry.getKey().startsWith("tile")) {
							channelList.add(entry.getValue());	
						}
					}
				} else {
					channelList.add(entry.getValue());
				}
			}
			//channelList.addAll(map.values());
		}
		
		// Double-iteration to build pairs
		for (Channel channel1: channelList) {
			for (Channel channel2: channelList) {
				if (channel1 != channel2) {
					// Put channels as nodes in the set
					this.nodes.add(channel1.getChannelName());
					this.nodes.add(channel2.getChannelName());
					
					// Build pair
					ChannelPair pair = new ChannelPair(channel1.getChannelName(), channel2.getChannelName());
					
					// Check if it is contained in the map
					if (this.pairMultiplicity.containsKey(pair) == false) {
						// Count common subscribers
						int commonSubscribers = countCommonSubscribers(channel1, channel2);
						
						if (commonSubscribers > 0) {
							// Perform computation and add it
							this.pairMultiplicity.put(pair, commonSubscribers);
						}
					}
				}
			}	
		}
	}
	
	private int countCommonSubscribers(Channel channel1, Channel channel2) {
		int time1 = this.time;
		if (this.time > channel1.getLastTime()) {
			time1 = channel1.getLastTime();
		}
		int time2 = this.time;
		if (this.time > channel2.getLastTime()) {
			time2 = channel2.getLastTime();
		}
		
		// For debuggigng only
		if (channel1.getChannelName().equals("broadcast") || channel2.getChannelName().equals("broadcast")) {
			time1 = time1 + 0;
		}
		
		Set<RPubNetworkID> commonSubscribers = new HashSet<RPubNetworkID>(channel1.getSliceStats(time1).getSubscriberList());
		commonSubscribers.retainAll(channel2.getSliceStats(time2).getSubscriberList());
		
		return commonSubscribers.size();
	}

	public Set<String> getNodes() {
		return this.nodes;
	}

	public Map<ChannelPair, Integer> getPairMultiplicity() {
		return this.pairMultiplicity;
	}
	
	public String generateOutput() {
		StringBuilder sBuilder = new StringBuilder();
		
		// Header
		sBuilder.append("graph G {\n");
		
		// Append list of nodes
		for (String node: nodes) {
			sBuilder.append("   \"" + node + "\";\n");
		}
		
		// Append relations
		for (ChannelPair pair: pairMultiplicity.keySet()) {
			sBuilder.append("   \"" + pair.getChannel1() + "\" -- \"" + pair.getChannel2() + "\"" + " [label=" + "\"" + pairMultiplicity.get(pair) + "\"" + "]" + ";\n");
		}
		
		// Footer
		sBuilder.append("}\n");
		
		return sBuilder.toString();
	}
	
	public String generateMetisOutput() {
		StringBuilder sBuilder = new StringBuilder();
		StringBuilder sGraphBuilder = new StringBuilder();
		
		int doubleEdgeCount = 0;
		
		
		int nodeCount = 1;
		
		// For each node...
		for (Channel channel: channelList) {			
			// Print channel name as comment
			sGraphBuilder.append("%" + channel.getChannelName() + "| vertex=" + nodeCount + "\n");
			
			List<Integer> l = new ArrayList<Integer>();
			
			// Sum the total computed byte out for all hosts for channel 'channel'
			long outgoingBandwidth = 0;
			int connections = 0;
			for (RPubClientId clientId: loadEvaluator.getRPubClients()) {
				outgoingBandwidth += loadEvaluator.getClientChannelComputedByteOut(clientId, channel.getChannelName());
				connections += loadEvaluator.getClientChannelSubscribers(clientId, channel.getChannelName()) + loadEvaluator.getClientChannelPublishers(clientId, channel.getChannelName());
			}
			
			/* Weight of the node/vertex
			 * (1) outgoingBandwidth 
			 * (2) # connections
			 * */
			l.add( (int)outgoingBandwidth );
			l.add( connections );
			
			/* Get weight for all other nodes (weight must be >0) */
			// We must have a loop index because we refer to nodes by indexes
			int nodeId=1; // With METIS, loop index starts at 1
			for (Channel otherChannel: channelList) {
				// If adjacent node is the same as node, skip
				if (channel == otherChannel) {
					nodeId++;
					continue;
				}
				
				// Count common subscribers. If >0 then write.
				int commonSubscribers = countCommonSubscribers(channel, otherChannel);
				
				if (commonSubscribers > 0) {
					// Add the channel and add the weight (# subscribers for now)
					l.add(nodeId);
					l.add(commonSubscribers);
					doubleEdgeCount++;
				}
				
				// Increment loop index
				nodeId++;
			}
			
			// Append to builder
			sGraphBuilder.append(integerListToString(l));
			
			nodeCount++;
		}
		
		// Write header for main builder. We have to write the header at the end because
		// we don't know yet the # edges...
		
		// Header - # of nodes
		// Number of nodes, number of edges, format : {nodes are weighted, edges are weighted}, # of weights per node 
		sBuilder.append(integerListToString(buildIntegerList(channelList.size(), doubleEdgeCount/2, 11, 2)));
		
		// Write graph builder's output
		sBuilder.append(sGraphBuilder.toString());
		
		return sBuilder.toString();
	}
	
	// Generate a plan from a metis partition
	public PlanImpl generatePlanFromMetisPartition(Plan currentPlan, String partitionFileContents) {
		// Ordering of partition indexes based on ordering of channelList

		// Create a plan based on new plan
		PlanImpl plan = new PlanImpl( (PlanImpl)currentPlan );
		
		// Split by line
		String[] lines = partitionFileContents.split("\\r?\\n");
		
		// For each channel
		int i=0;
		for (Channel channel: channelList) {
			// Get shard index
			int shardIndex = Integer.parseInt(lines[i].trim());
			
			// Set entry in plan
			plan.setMapping(channel.getChannelName(), new PlanMappingImpl( new PlanId(currentPlan.getPlanId().getId()), channel.getChannelName(), new RPubClientId(shardIndex)));
			
			i++;
		}
		
		return plan;
	}
	
	private String integerListToString(List<Integer> ints) {
		String str = "";
		for (Integer integer: ints) {
			if (str.equals("") == false) {
				str += " ";
			}
			str += integer.toString();
		}
		return str + "\n";
	}
	
	private List<Integer> buildIntegerList(int... ints) {
		List<Integer> intList = new ArrayList<Integer>();
		for (int integer: ints) {
			intList.add(integer);
		}
		return intList;
	}
	
	/**
	   * Load a text file contents as a <code>String<code>.
	   * This method does not perform enconding conversions
	   *
	   * @param file The input file
	   * @return The file contents as a <code>String</code>
	   * @exception IOException IO Error
	   */
	  public static String deserializeString(File file)
	  throws IOException {
	      int len;
	      char[] chr = new char[4096];
	      final StringBuffer buffer = new StringBuffer();
	      final FileReader reader = new FileReader(file);
	      try {
	          while ((len = reader.read(chr)) > 0) {
	              buffer.append(chr, 0, len);
	          }
	      } finally {
	          reader.close();
	      }
	      return buffer.toString();
	  }
}
