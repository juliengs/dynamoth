package Dynamoth.Core.LoadBalancing.Rebalancing;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.LoadEvaluator;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Core.Manager.Plan.PlanImpl;
import Dynamoth.Core.Manager.Plan.PlanMappingImpl;
import Dynamoth.Core.Manager.Plan.PlanMapping.PlanMappingStrategy;
import Dynamoth.Core.Util.RPubHostInfo;

public class SimpleDynaWANLocationRebalancer extends LoadBasedRebalancer {

	private String rebalancePrefix = "tile_";
	
	public SimpleDynaWANLocationRebalancer(Plan currentPlan, int currentTime, LoadEvaluator currentLoadEvaluator,
			Map<RPubClientId, RPubHostInfo> hostInfoMap) {
		super(currentPlan, currentTime, currentLoadEvaluator, hostInfoMap);
	}

	@Override
	protected void processIteration() {
		// Make sure we can set a new plan
		if (this.canSetNewPlan() == false)
			return;
		
		// Create our new proposed plan
		PlanImpl proposedPlan = new PlanImpl( (PlanImpl) (this.getCurrentPlan()) );
		
		// Algorithm idea:
		// Iterate through all channels, across all hosts...
		// If subs are only on domain A, then put the channel on RPub0
		// If subs are only on domain B, then put the channel on RPub1
		// If subs are on both domains,  then put the channel on RPub0 and RPub1 - replicated using DYNAWAN_ROUTING
		
		Set<String> tileChannels = getAllTileChannels();
		
		// For all channels: count how many subscribers we have on domains A and B
		for (String channel: tileChannels) {
			int subsDomainA = 0, subsDomainB = 0;
			
			// For all hosts
			for (RPubClientId clientId: this.currentLoadEvaluator.getRPubClients()) {
				Set<RPubNetworkID> subscribers = this.currentLoadEvaluator.getClientChannelSubscriberList(clientId, channel);
				
				// For all subscribers, count domain A and domain B
				for (RPubNetworkID subscriber: subscribers) {
					if (subscriber.getDomain().contains("us-east")) {
						subsDomainA++;
					}
					else if (subscriber.getDomain().contains("ap-southeast")) {
						subsDomainB++;
					}
				}
			}
			
			// Set appropriate channel mapping
			if (subsDomainA == 0 && subsDomainB == 0) {
				// No subscriber at all - don't do anything
			} else if (subsDomainB == 0) {
				// Subscribers on A only: no replication, channel on A only
				proposedPlan.setMapping(channel, new PlanMappingImpl(proposedPlan.getPlanId(), channel, new RPubClientId(0)));
			} else if (subsDomainA == 0) {
				// Subscribers on B only: no replication, channel on B only 
				proposedPlan.setMapping(channel, new PlanMappingImpl(proposedPlan.getPlanId(), channel, new RPubClientId(1)));
			} else {
				// Subscribers on both domains: DynaWan replication
				proposedPlan.setMapping(channel, new PlanMappingImpl(proposedPlan.getPlanId(), channel, new RPubClientId[] {
						new RPubClientId(0), new RPubClientId(1)
						}, PlanMappingStrategy.DYNAWAN_ROUTING));
			}
		}
		
		setNewPlan(proposedPlan);
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
