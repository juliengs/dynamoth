package Dynamoth.Core.LoadBalancing.Rebalancing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.LoadEvaluator;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Core.Manager.Plan.PlanImpl;
import Dynamoth.Core.Manager.Plan.PlanMapping;
import Dynamoth.Core.Manager.Plan.PlanMapping.PlanMappingStrategy;
import Dynamoth.Core.Manager.Plan.PlanMappingImpl;
import Dynamoth.Core.Util.RPubHostInfo;

public class DynamothReplicationRebalancer extends LoadBasedRebalancer {

	//private double pubFullyConnectedThreshold = 30; // subscribers / publications per sec
	private double pubFullyConnectedThreshold = /*2000*/ 1000 /*+ 20000000*/; // msgs per sec [PLEASE REMOVE LAST PART]

	//private double subFullyConnectedThreshold = 1500; //publications per sec/subscribers
	private double subFullyConnectedThreshold = 2000000000; //msgs per sec
	
	private Set<String> ignoreChannels = new HashSet<String>();
	
	private Map<String, Long> lastChange = new HashMap<String, Long>();

	private static final long DELAY_BETWEEN_CHANGE = 30 * 1000;
	
	public DynamothReplicationRebalancer(Plan currentPlan, int currentTime,
			LoadEvaluator currentLoadEvaluator,
			Map<RPubClientId, RPubHostInfo> hostInfoMap) {
		super(currentPlan, currentTime, currentLoadEvaluator, hostInfoMap);
		
		ignoreChannels.addAll( Arrays.asList(new String[] {"loadbalancer-channel", "plan-push-channel", "loadanalyzer-channel", "track-info", "load-balancer-stats", "rgame-broadcast"}));
	}
	
	private boolean shouldIgnore(String channel) {
		return ignoreChannels.contains(channel);
	}

	@Override
	protected void processIteration() {
		// Make sure we can set a new plan
		if (this.canSetNewPlan() == false) {
			//System.out.println("Can't set new plan yet");
			return;
		}

		//System.out.println("Attempting to generate new plan");
		// 1) Attempt to generate a high load plan - if necessary
		PlanImpl newPlan = generatePlan();
		
		// 3) Apply the new plan (or null if there is no new plan)
		setNewPlan(newPlan);
	}

	/**
	 * Generate a high-load plan
	 * @return High load plan if any otherwise null
	 */
	private PlanImpl generatePlan() {
		
		// Create our new proposed plan
		PlanImpl proposedPlan = new PlanImpl( (PlanImpl) (this.getCurrentPlan()) );
		for(String ch : proposedPlan.getAllChannels()) {


			//if(!(shouldIgnore(ch) || ch.startsWith("sub") || ch.startsWith("unicast") || ch.startsWith("broadcast"))) {
			if (ch.startsWith("tile")) {
				PlanMapping mapping = proposedPlan.getMapping(ch);

				//int numPublications = (int) Math.ceil(getNumberPublications(mapping) / mapping.getShards().length);
				//int numPublications = 10;
				int numPublications = getNumberMessages(mapping);
				//int numSubscriptions = getNumberSubscribers(mapping);
				int numSubscriptions = getNumberMessages(mapping);
				if(mapping.getChannel().equals("tile_0_0xxxxxxxxxxxxxx")) {
					System.out.println("Number of publications (" + mapping.getChannel() + ")=" + numPublications);
					System.out.println("Number of subscriptions (" + mapping.getChannel() + ")=" + numSubscriptions);
				}
				
				Long timeOfLastChange = lastChange.get(ch);
				Long currentTime = System.currentTimeMillis();
				if(timeOfLastChange != null && (currentTime - timeOfLastChange) < DELAY_BETWEEN_CHANGE) {
					continue;
				}
				
				if(numPublications == 0 || numSubscriptions == 0) {
					continue;
				}

				//calculate ratios
				//double pubPerSub = (double) numPublications / numSubscriptions;
				double pubPerSub = (double) numPublications;
				//double subPerPub = (double) numSubscriptions / numPublications;
				double subPerPub = (double) numSubscriptions;
				
				//Get current mapping info
				PlanMappingStrategy strategy = mapping.getStrategy();
				int numServers = mapping.getShards().length;
				
				//calculate new mapping info (if necessary)
				if(shouldUseSubscriberFullyConnected(pubPerSub)) {
					numServers = Math.max(1, (int) Math.ceil(pubPerSub / subFullyConnectedThreshold));
					strategy = PlanMappingStrategy.SUBSCRIBERS_FULLY_CONNECTED;
				} else if(shouldUsePublisherFullyConnected(subPerPub)) {
					numServers = Math.max(1, (int) Math.ceil(subPerPub / pubFullyConnectedThreshold));
					strategy = PlanMappingStrategy.PUBLISHERS_FULLY_CONNECTED;
				} else {
					numServers = 1;
				}
				
				//If we are at one server go back to default strategy
				if(numServers == 1) {
					strategy = PlanMappingStrategy.DEFAULT_STRATEGY;
				}
				
				//Calculate new list of shards
				List<RPubClientId> shards = new ArrayList<RPubClientId>(Arrays.asList(mapping.getShards()));
				int shardDiff = numServers - shards.size();
				if(shardDiff < 0) {
					//SHOULD sort shard by descending load
					for(int i = 0; i < Math.abs(shardDiff); i++) {
					//for(int i = Math.abs(shardDiff)-1; i >= 0; i--) {
						//shards.remove(0);
						shards.remove(shards.size()-1);
					}
				} else if(shardDiff > 0) {
					//List<RPubClientId> allShards = new ArrayList<RPubClientId>(Arrays.asList(this.getCurrentPlan().getAllShards()));
					List<RPubClientId> allShards = new ArrayList<RPubClientId>(this.getCurrentLoadEvaluator().getRPubClients());
					allShards.removeAll(shards);
					//SHOULD sort allShards by ascending load
					// For loop that executes for each shard that should be added
					for(int i = 0; i < Math.abs(shardDiff); i++) {
				
						if(allShards.isEmpty()) {
							break;
						}
						
						// Find the least-busy shard according to the current load evaluator
						// (and not only the "first" shard)
						
						RPubClientId lowestShard = null;
						
						for (int j=0; j < allShards.size(); j++) {
						
							// Blacklist rpub2
							if (allShards.get(j).getId() == 2) {
								allShards.remove(j);
								j--;
								continue;
							}
							
							if (lowestShard == null) {
								lowestShard = allShards.get(j);
							} else {
								// Check if cur shard has lower byteout ratio
								if (currentLoadEvaluator.getClientByteOutRatio(allShards.get(j)) < currentLoadEvaluator.getClientByteOutRatio(lowestShard)) {
									lowestShard = allShards.get(j);
								}
							}
						
						}
						
						// Add lowest shard and remove from pool
						shards.add(lowestShard);
						allShards.remove(lowestShard);
					}
				}
				if(shardDiff != 0 || strategy != mapping.getStrategy()) {
					lastChange.put(ch, currentTime);
				}
				proposedPlan.setMapping(ch, new PlanMappingImpl(proposedPlan.getPlanId(), ch, shards.toArray(new RPubClientId[]{}), strategy));
			}
		}
		return proposedPlan;
	}

	private int getNumberSubscribers(PlanMapping mapping) {
		int subscribers = 0;
		for(RPubClientId shard : mapping.getShards()) {
			subscribers += this.currentLoadEvaluator.getClientChannelSubscribers(shard, mapping.getChannel());
		}
		if(mapping.getChannel().equals("replication-test-dynamic")) {
			for(RPubClientId shard : this.getCurrentPlan().getAllShards()) {
					//System.out.println("sub(" + shard.getId() + ")=" + this.currentLoadEvaluator.getClientChannelSubscribers(shard, mapping.getChannel()));
			}
		}
		return subscribers;
	}

	private int getNumberPublications(PlanMapping mapping) {
		int publications = 0;
		for(RPubClientId shard : mapping.getShards()) {
			publications += this.currentLoadEvaluator.getClientChannelPublications(shard, mapping.getChannel());
		}
		// If PFC, then real # of publications must be divided by # of shards since all publications are duplicated
		if (mapping.getStrategy().equals(PlanMappingStrategy.PUBLISHERS_FULLY_CONNECTED)) {
			return publications / mapping.getShards().length;
		}
		return publications;
	}
	

	private int getNumberMessages(PlanMapping mapping) {
		int messages = 0;
		for(RPubClientId shard : mapping.getShards()) {
			messages += this.currentLoadEvaluator.getClientChannelSentMessages(shard, mapping.getChannel());
		}
		// If PFC, then real # of msgs must be divided by # of shards since all publications are duplicated
		/*if (mapping.getStrategy().equals(PlanMappingStrategy.PUBLISHERS_FULLY_CONNECTED)) {
			return messages / mapping.getShards().length;
		}*/
		return messages;
	}

	private boolean shouldUseSubscriberFullyConnected(double pubPerSub) {
		return pubPerSub > subFullyConnectedThreshold;
	}

	private boolean shouldUsePublisherFullyConnected(double subPerPub) {
		return subPerPub > pubFullyConnectedThreshold;
	}
	
}
