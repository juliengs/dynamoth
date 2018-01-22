package Dynamoth.Core.LoadBalancing.Rebalancing;

import java.util.Map;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.LoadEvaluator;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Core.Manager.Plan.PlanImpl;
import Dynamoth.Core.Manager.Plan.PlanMappingImpl;
import Dynamoth.Core.Util.RPubHostInfo;

public class ConsistentHashingRebalancer extends LoadBasedRebalancer {
	
	public ConsistentHashingRebalancer(Plan currentPlan, int currentTime,
			LoadEvaluator currentLoadEvaluator,
			Map<RPubClientId, RPubHostInfo> hostInfoMap) {
		super(currentPlan, currentTime, currentLoadEvaluator, hostInfoMap);
		
	}

	@Override
	protected void processIteration() {
		// Make sure we can set a new plan
		if (this.canSetNewPlan() == false)
			return;
		
		// Temp algo
		// Find the top busiest host and the least busy one
		RPubClientId busiestClientId = null;
		double busiestLoadRatio = 0;
		RPubClientId leastBusyClientId = null;
		double leastBusyLoadRatio = Double.MAX_VALUE;
		for (RPubClientId clientId: this.currentLoadEvaluator.getRPubClients()) {
			// Skip client 2 because we keep it for internal data
			if (clientId.getId() == 2) continue;
			double loadRatio = this.currentLoadEvaluator.getClientByteOutRatio(clientId);
			// Check for busiest
			if (loadRatio > 0.80 && loadRatio > busiestLoadRatio) {
				busiestClientId = clientId;
				busiestLoadRatio = loadRatio;
			}
			// Check for least busy - considering only the inactive hosts
			if (this.activeHosts.contains(clientId) == false && loadRatio < leastBusyLoadRatio) {
				leastBusyClientId = clientId;
				leastBusyLoadRatio = loadRatio;
			}
		}
		// Rebalancing not needed - bail out
		if (busiestClientId == null) {
			setNewPlan(null);
			return;
		}
		
		// If we did not find an inactive host then bail out
		if (leastBusyClientId == null) {
			setNewPlan(null);
			System.out.println("Rebalancing would be needed but no extra inactive hosts available!");
			return;
		}
		
		// Do the consistent-hashing transfer by xferring parts of the channels
		System.out.println("Rebalancing, busiest [" + busiestClientId.getId() + "] over threshold, transferring to [" + leastBusyClientId.getId() + "]");
		
		// Copy the current plan but increment the counter
		PlanImpl currentPlan = (PlanImpl) (this.getCurrentPlan());
		PlanImpl proposedPlan = new PlanImpl( (PlanImpl) (this.getCurrentPlan()) );
		
		// For all active hosts, give a portion of the channels to the new host
		double givePercentage = (1.0 / (this.activeHosts.size() + 1.0 )) / (this.activeHosts.size());
		for (RPubClientId clientId: this.activeHosts) {
			int giveCount = (int) (Math.round(givePercentage * currentPlan.getClientChannels(clientId).size()));
			
			// Migrate X % of channels
			int given = 0;
			int index = 0;
			for (String channel: currentPlan.getClientChannels(clientId)) {
				
				// BLACKLIST SOME CHANNELS
				if (channel.equals("loadbalancer-channel") || channel.equals("plan-push-channel") || channel.equals("loadanalyzer-channel") || channel.equals("track-info"))
					continue;
				
				// Set the mapping if modulo matches
				if (index % (currentPlan.getClientChannels(clientId).size() / giveCount) == 0) {
					proposedPlan.setMapping(channel, new PlanMappingImpl(proposedPlan.getPlanId(), channel, leastBusyClientId));
					given++;
					//System.out.println("--> RPubClient[" + clientId.toString() + "] gave " + given + " channels to new host " + leastBusyClientId.getId());
				}
				
				if (given >= giveCount) {
					// Print stats
					
					break;
				}
				
				index++;
			}
		}
		
		// Add active host
		this.activeHosts.add(leastBusyClientId);
		
		// Set new plan
		this.setNewPlan(proposedPlan);
	}

}
