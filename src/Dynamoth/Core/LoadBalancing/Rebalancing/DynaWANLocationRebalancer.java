package Dynamoth.Core.LoadBalancing.Rebalancing;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.LoadEvaluator;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Core.Manager.Plan.PlanImpl;
import Dynamoth.Core.Manager.Plan.PlanMappingImpl;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.Util.RPubHostInfo;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * DynaWAN Location Rebalancer
 * Algorithm works as follows:
 * 
 * Let's assume domains A, B, .... We have: P=publisher, S=subscriber, H=host.
 * 
 * Count how many SS, SL and LL as follows (for each channel).
 * 
 * For all (P,S,D) pairs (for all issued messages, we have a publisher, a subscriber and a host):
 * (1) P,S,H in same domain => SS
 * (2) P,H OR S,H in same domain (but not all three) => SL
 * (3) P,H AND S,H in different domains => LL
 * 
 * OLD STUFF below
 * 
 * I- Server in A
 * (1) P in A, S in A => SS
 * (2) P in A, S in B => SL
 * (3) P in B, S in A => LS
 * (4) P in B, S in B => LL
 * 
 * II- Server in B
 * 
 * 
 * III- Ser
 * 
 * @author Julien Gascon-Samson
 */
public class DynaWANLocationRebalancer extends LoadBasedRebalancer {

	private String rebalancePrefix = "tile_";
	
	private Map<String, DynaWANMessageCounter> messageCounters = new HashMap<String, DynaWANMessageCounter>();
	private Object messageCountersLock = new Object();
	
	private boolean trivialRebalancingDone = false;
	
	private int startTime = 0;
	
	public DynaWANLocationRebalancer(Plan currentPlan, int currentTime, LoadEvaluator currentLoadEvaluator, Map<RPubClientId, RPubHostInfo> hostInfoMap) {
		super(currentPlan, currentTime, currentLoadEvaluator, hostInfoMap);
		startTime = currentTime;
	}
	
	public Map<String, DynaWANMessageCounter> getMessageCounters() {
		synchronized(messageCountersLock) {
			return new HashMap<String, DynaWANMessageCounter>(this.messageCounters);
		}
	}

	private void updateMessageCounters() {
		Set<String> tileChannels = getAllTileChannels();
		
		Map<String, DynaWANMessageCounter> counters = new HashMap<String, DynaWANMessageCounter>();
		
		// For each channel
		for (String channel: tileChannels) {
			int ssCount = 0, slCount = 0, llCount = 0;
			
			// Iterate though each host to get a list of publishers and subscribers
			for (RPubClientId clientId: this.currentLoadEvaluator.getRPubClients()) {
				
				// Get the list of publishers and subscribers
				// and add to global list of publishers and subscribers (accross all servers)
				Set<RPubNetworkID> publishers = this.currentLoadEvaluator.getClientChannelPublisherList(clientId, channel);
				Set<RPubNetworkID> subscribers = this.currentLoadEvaluator.getClientChannelSubscriberList(clientId, channel);
			
				// For every publisher and subscriber
				for (RPubNetworkID publisher: publishers) {
					for (RPubNetworkID subscriber: subscribers) {

						String publisherDomain = publisher.getDomain();
						String subscriberDomain = subscriber.getDomain();
						String hostDomain = "";
						if (clientId.getId() == 0 || clientId.getId() == 2)
							hostDomain = "us-east";
						else
							hostDomain = "ap-southeast";

						// Get amount of publications
						int publications = this.currentLoadEvaluator.getClientChannelPublisherPublications(clientId, channel, publisher);
						
						// Case 1: P,S,H in same domain?
						if (publisherDomain.startsWith(hostDomain) && subscriberDomain.startsWith(hostDomain)) {
							// Add to number of SS
							ssCount += publications;
						} else if (publisherDomain.startsWith(hostDomain) || subscriberDomain.startsWith(hostDomain)) {
							// Add to number of SL
							slCount += publications;
						} else {
							// Add to number of LL
							llCount += publications;
						}

					}
				}
			}
			
			// Update our message counter
			counters.put(channel, new DynaWANMessageCounter(ssCount, slCount, llCount));
		}
		
		// Swap counters while synchronized
		synchronized(messageCountersLock) {
			this.messageCounters = counters;
		}
	}
	
	@Override
	protected void processIteration() {

		// Update message counters
		updateMessageCounters();
		
		// Make sure we can set a new plan
		if (this.canSetNewPlan() == false)
			return;
		
		// Create our new proposed plan
		PlanImpl proposedPlan = new PlanImpl( (PlanImpl) (this.getCurrentPlan()) );
	
		// Now for the rebalancing part: if >X% of LL msgs, then rebalance... Is that good?
		synchronized(this.messageCountersLock) {
			
			for (String channel: getAllTileChannels()) {
				DynaWANMessageCounter counter = messageCounters.get(channel);
				//if (counter.getLLMessageRatio() > 0.10) {
					
					
					
				//}
				
				// Do something... Find best scheme: place on A or on B?
				if (counter.getLLMessageCount() > 0) {
					if (this.getCurrentPlan().getMapping(channel).getShards()[0].getId() == 0) {
						// Mapped to 0, switch to 1
						proposedPlan.setMapping(channel, new PlanMappingImpl(proposedPlan.getPlanId(), channel, new RPubClientId(1)));
					} else {
						// Mapped to 1, switch to 0
						//proposedPlan.setMapping(channel, new PlanMappingImpl(proposedPlan.getPlanId(), channel, new RPubClientId(0)));
					}
				}
			}
		}
		
		int runningTime = this.getCurrentTime() - startTime;

		// Dummy plan change
		/*if (runningTime > 20 && (!trivialRebalancingDone)) {
			System.out.println("***** DynaWANLocationRebalanceR: Setting temporary plan (t=30)");
			proposedPlan.setMapping("tile_0_0|B", new PlanMappingImpl(proposedPlan.getPlanId(), "tile_0_0|B", new RPubClientId(1)));
			setNewPlan(proposedPlan);
			trivialRebalancingDone = true;
		}*/
	}
	
	private Set<String> getAllTileChannels() {
		Set<String> tileChannels = new HashSet<String>();
		for (RPubClientId clientId: this.currentLoadEvaluator.getRPubClients()) {
			for (String channel: this.currentLoadEvaluator.getClientChannels(clientId)) {
				if (channel.startsWith(rebalancePrefix))
					tileChannels.add(channel);
			}
		}
		return tileChannels;
	}
}

