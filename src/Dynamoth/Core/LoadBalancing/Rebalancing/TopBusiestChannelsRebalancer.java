package Dynamoth.Core.LoadBalancing.Rebalancing;

import java.util.Map;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.LoadEvaluator;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.NewPlanEstimatedLoadEvaluator;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Core.Manager.Plan.PlanImpl;
import Dynamoth.Core.Manager.Plan.PlanMappingImpl;
import Dynamoth.Core.Util.RPubHostInfo;

public class TopBusiestChannelsRebalancer extends LoadBasedRebalancer {

	public TopBusiestChannelsRebalancer(Plan currentPlan, int currentTime,
			LoadEvaluator currentLoadEvaluator,
			Map<RPubClientId, RPubHostInfo> hostInfoMap) {
		super(currentPlan, currentTime, currentLoadEvaluator, hostInfoMap);
	}

	@Override
	protected synchronized void processIteration() {
		
		// Make sure we can set a new plan
		if (this.canSetNewPlan() == false)
			return;
		
		// Temp algo
		// Find the top busiest host and the least busy one
		RPubClientId busiestClientId = null;
		double busiestLoadRatio = 0;
		RPubClientId leastBusyActiveClientId = null;
		double leastBusyActiveLoadRatio = Double.MAX_VALUE;
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
			// Check for least busy - considering only the active hosts
			if (this.activeHosts.contains(clientId) && loadRatio < leastBusyActiveLoadRatio) {
				leastBusyActiveClientId = clientId;
				leastBusyActiveLoadRatio = loadRatio;
			}
			// Check for least busy - considering all hosts
			if (loadRatio < leastBusyLoadRatio) {
				leastBusyClientId = clientId;
				leastBusyLoadRatio = loadRatio;
			}
		}
		
		// If least busy active client's load <= 0.40, then this host can be used! Otherwise,
		// the global least active client should be used (might select an inactive host)
		if (leastBusyActiveLoadRatio <= 0.40) {
			leastBusyClientId = leastBusyActiveClientId;
			leastBusyLoadRatio = leastBusyActiveLoadRatio;
		} else {
			// Might be adding another host...
			if (this.activeHosts.contains(leastBusyClientId) == false) {
				this.activeHosts.add(leastBusyClientId);
			}
		}
		
		// If we have no busy client then set the plan to null
		if (busiestClientId == null) {
			setNewPlan(null);
		}
		// If we have a busy client then migrate-estimate channels until we get below 40%
		else {
			
			System.out.println("Rebalancing, busiest [" + busiestClientId.getId() + "] over threshold, transferring to [" + leastBusyClientId.getId() + "]");
			
			// Determine a threshold load ratio for our rebalancing
			// Will be the middlepoint between the busiest and the lowest busy
			double thresholdLoadRatio = (busiestLoadRatio + leastBusyLoadRatio) / 2.0;
			/*if (thresholdLoadRatio < 0.70)
				thresholdLoadRatio = 0.70;*/
			
			double loadRatio = this.currentLoadEvaluator.getClientByteOutRatio(busiestClientId);
			
			// Copy the current plan but increment the counter
			PlanImpl proposedPlan = new PlanImpl( (PlanImpl) (this.getCurrentPlan()) );
			
			// While the load ratio is above the thrshold, then start migrating channels
			while (loadRatio > thresholdLoadRatio) {
				
				// Migrate first channel in the proposed plan
				if (proposedPlan.getClientChannels(busiestClientId).size() == 0) {
					System.out.println("Migration impossible because no channels");
					break;
				}
				
				// Take the busiest channel
				long busiestByteOut = 0;
				String busiestChannel = ""; 
				for (String channel: proposedPlan.getClientChannels(busiestClientId)) {
					
					// BLACKLIST SOME CHANNELS
					if (channel.equals("loadbalancer-channel") || channel.equals("plan-push-channel") || channel.equals("loadanalyzer-channel") || channel.equals("track-info"))
						continue;
					// BLACKLIST CHANNELS NOT STARTING WITH TILE (warning: rgame-specific!)
					/*if (channel.startsWith("tile_") == false)
						continue;*/
					
					long byteOut = currentLoadEvaluator.getClientChannelComputedByteOut(busiestClientId, channel);
					if (byteOut > busiestByteOut) {
						busiestByteOut = byteOut;
						busiestChannel = channel;
					}
				}
				
				// If no other channel available, then break out of the loop
				if (busiestChannel.equals("")) {
					break;
				}
				
				//String busiestChannel = proposedPlan.getClientChannels(busiestClientId).iterator().next();
				
				proposedPlan.setMapping(busiestChannel, new PlanMappingImpl(proposedPlan.getPlanId(), busiestChannel, leastBusyClientId));
				// Estimate load ratio
				NewPlanEstimatedLoadEvaluator estimatedLoadEvaluator = new NewPlanEstimatedLoadEvaluator(getCurrentPlan(), proposedPlan, currentLoadEvaluator, hostInfoMap, getCurrentTime());
				loadRatio = estimatedLoadEvaluator.getClientByteOutRatio(busiestClientId);
			}
			
			// Set new plan
			this.setNewPlan(proposedPlan);
			
			// Print estimate
			System.out.println("New Plan Estimated Load Ratio=" + loadRatio + " with currentLoadEvaluator=" + this.currentLoadEvaluator.hashCode());
		}

	}
}
