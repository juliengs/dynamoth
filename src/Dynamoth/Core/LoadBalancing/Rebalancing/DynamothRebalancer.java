package Dynamoth.Core.LoadBalancing.Rebalancing;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.LoadEvaluator;
import Dynamoth.Core.LoadBalancing.LoadEvaluation.NewPlanEstimatedLoadEvaluator;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Core.Manager.Plan.PlanImpl;
import Dynamoth.Core.Manager.Plan.PlanMapping.PlanMappingStrategy;
import Dynamoth.Core.Manager.Plan.PlanMappingImpl;
import Dynamoth.Core.Util.RPubHostInfo;

public class DynamothRebalancer extends LoadBasedRebalancer {

	/**
	 * Load ratio at which we consider that a given host is overloaded
	 */
	private double highLoadThreshold = 0.80;
	/**
	 * Load ratio at which we consider that a given host cannot accept any additional channel
	 * as part of a rebalancing operation. If the load is below this threshold, then this host
	 * can accept additional channels. 
	 */
	private double addLoadThreshold = 0.60;
	/**
	 * When rebalancing (high load plan), we will remove channels from this host
	 * until the LR falls below this threshold.
	 */
	private double reduceLoadThreshold = 0.60;
	
	/**
	 * Max avg load ratio at which we consider that a lowly loaded plan should be generated 
	 */
	
	private double maxLoadThresholdLowPlan = 0.60;
	
	//private double lowLoadThreshold 
	
	private Set<RPubClientId> ignoreRPubList = new HashSet<RPubClientId>();
	private Set<String> ignoreChannels = new HashSet<String>();
	private Set<String> defaultIgnoreChannels = new HashSet<String>();

	
	public DynamothRebalancer(Plan currentPlan, int currentTime,
			LoadEvaluator currentLoadEvaluator,
			Map<RPubClientId, RPubHostInfo> hostInfoMap) {
		super(currentPlan, currentTime, currentLoadEvaluator, hostInfoMap);
		
		// Setup ignore list
		ignoreRPubList.add(new RPubClientId(2));
		defaultIgnoreChannels.addAll( Arrays.asList(new String[] {"loadbalancer-channel", "plan-push-channel", "loadanalyzer-channel", "track-info"}));
		ignoreChannels.addAll(defaultIgnoreChannels);
	}
	
	private boolean shouldIgnore(RPubClientId clientId) {
		return ignoreRPubList.contains(clientId);
	}
	
	private boolean shouldIgnore(String channel) {
		return ignoreChannels.contains(channel);
	}

	@Override
	protected void processIteration() {
		// Make sure we can set a new plan
		if (this.canSetNewPlan() == false)
			return;

		// Ignore repl. channels
		ignoreReplicatedChannels();
		
		// 1) Attempt to generate a high load plan - if necessary
		PlanImpl newPlan = generateHighLoadPlan();
		if (newPlan != null) {
			System.out.println("New high load plan (1)!!");
		}
		
		if (newPlan == null) {
			// 2) If no high load plan is needed, then we might generate a low-load plan
			newPlan = generateLowLoadPlan();
		}
		
		if (newPlan != null) {
			System.out.println("New high load plan (2)!!");
		}
		
		// 3) Apply the new plan (or null if there is no new plan)
		setNewPlan(newPlan);
	}

	/**
	 * Generate a high-load plan
	 * @return High load plan if any otherwise null
	 */
	private PlanImpl generateHighLoadPlan() {
		
		/*
		 * Algorithm plan:
		 * ---PART A---
		 * Check if any server is >highLoadThreshold
		 * If none then return null - no high load plan needed :-)
		 * 
		 * If yes then do rebalancing as follows:
		 * Do:
		 * 	Find the busiest host
		 * 	Find the busiest channel
		 * 	Assign the busiest channel to the lowest busy host with load > addLoadThreshold
		 * 	If no suitable host can be found, then spawn a new host
		 * 	Repeat (until busiest host <highLoadThreshold)
		 * 
		 */
		
		// 1- Check if a high load plan is needed
		boolean highLoadRebalancingNeeded = false;
		for (RPubClientId clientId: this.currentLoadEvaluator.getRPubClients()) {
			// Ignore clients from ignore list
			if (shouldIgnore(clientId))
				continue;
			
			// Check if load above threshold
			double loadRatio = this.currentLoadEvaluator.getClientByteOutRatio(clientId);
			if (loadRatio > highLoadThreshold) {
				highLoadRebalancingNeeded = true;
				break;
			}
		}
		
		// If a high load plan is not needed, then return Null
		if (highLoadRebalancingNeeded == false) {
			return null;
		}
		
		// Create our new proposed plan
		PlanImpl proposedPlan = new PlanImpl( (PlanImpl) (this.getCurrentPlan()) );
		
		// Iteratively repeat the following until all hosts are below threshold
		LoadEvaluator loadEvaluator = this.currentLoadEvaluator;
		Set<RPubClientId> highestClientLookup = new HashSet<RPubClientId>(loadEvaluator.getRPubClients());
		do {
			
			// Get busiest host
			RPubClientId highestClientId = loadEvaluator.getClientHighestByteOut(highestClientLookup);
			double highestByteOut = loadEvaluator.getClientByteOutRatio(highestClientId);
			highestClientLookup.remove(highestClientId);
			
			// If no host > highLoadThreshold stop iteration
			if (highestByteOut < this.highLoadThreshold)
				break;
			
			boolean migratedAtLeastOnce = false;
			// Migrate channels until this host's load ratio is below 60%
			do {
			
				// Get busiest channel
				String busiestChannel = loadEvaluator.getClientChannelHighestByteOut(highestClientId, ignoreChannels, proposedPlan);
				// If highest not found (no more channels) then HMMM WHAT SHOULD WE DO?
				// WE CANNOT REBALANCE THIS HOST!
				if (busiestChannel == null) {
					// We cannot do anything... this case should be handled
					// We should skip processing this server
					// Remove it from highestClientLookup
					highestClientLookup.remove(highestClientId);
					System.out.println("No more channels! Rebalancing error. Client removed.");
					//continue;
					// Fake that we migrated at least once so that we don't bail out
					migratedAtLeastOnce = true;
					break;
				}
				
				// Get lowest busy host amongst all active hosts
				RPubClientId lowestClientId = loadEvaluator.getClientLowestByteOut(this.activeHosts);
				double lowestByteOut = loadEvaluator.getClientByteOutRatio(lowestClientId);
				// If above add threshold then a new host should be spawned
				if (lowestByteOut > this.addLoadThreshold) {
					// Spawn a new host if possible
					// To do so: recompute lowest host amongst ALL hosts. If an inactive host is yielded, then
					// make it active. Then the algo proceeds. If NO other host available (an active host is still yielded),
					// then ... ... ...
					
					// But exclude RPub #2...
					Set<RPubClientId> allHosts = new HashSet<RPubClientId>(loadEvaluator.getRPubClients());
					allHosts.remove(new RPubClientId(2));
					lowestClientId = loadEvaluator.getClientLowestByteOut(allHosts);
					lowestByteOut = loadEvaluator.getClientByteOutRatio(lowestClientId);
					
					// Add the host to the active hosts if not contained
					if (this.activeHosts.contains(lowestClientId) == false) {
						this.activeHosts.add(lowestClientId);
					}
					
				}
				
				// Make sure the lowest busy server's load < highest busy server's load
				// Otherwise, we simply bail out since we are so jam-packed that we cannot even rebalance anymore
				if (lowestByteOut >= highestByteOut) {
					break;
				} // Or, highest and lowest are on the same server... break!
				else if (highestClientId.equals(lowestClientId)) {
					break;
				}
				
				// Assign busiest channel to lowest busy server in proposed plan
				System.out.println("XXXMigrating " + busiestChannel + " to RPubClientId" + lowestClientId.getId());
				proposedPlan.setMapping(busiestChannel, new PlanMappingImpl(proposedPlan.getPlanId(), busiestChannel, lowestClientId));
				// Reestimate load ratio
				loadEvaluator = new NewPlanEstimatedLoadEvaluator(getCurrentPlan(), proposedPlan, currentLoadEvaluator, hostInfoMap, getCurrentTime());
				
				migratedAtLeastOnce = true;
				
				// Check if LR < reduceLoadThreshold
				if (loadEvaluator.getClientByteOutRatio(highestClientId) < reduceLoadThreshold) {
					break;
				}
				
			} while (true);
			
			// If we could not migrate at least once, then quit
			if (migratedAtLeastOnce == false) {
				break;
			}
			
		} while(true);
		
		return proposedPlan;
	}
	
	/**
	 * Generate a low-load plan
	 * @return Low load plan if any otherwise null
	 */
	private PlanImpl generateLowLoadPlan() {
		/*
		 * Algorithm plan:
		 * --- PART B---
		 * Check if global load ratio would allow one server to be freed with sufficient margin
		 * Take global load ratio
		 * COMPUTE AVG GLOBAL LR, EX 30%, n=3 SERVERS
		 * LIMIT LR AFTER MIN REBALANCING = 60%
		 * question is: if we split the AVG GLOBAL LR to (n-1) servers, does the new AvgLoad get over 60%?
		 * NEWAVGLOAD = AVGLOAD + AVGLOAD/(N-1)
		 * 50% over 6 hosts would become 60%
		 */
		
		// If only one host - ignore
		if (this.activeHosts.size() == 1) {
			return null;
		}
		
		// 1) Compute average load ratio
		double avgLoadRatio = 0.0;
		for (RPubClientId client: this.activeHosts) {
			avgLoadRatio += this.currentLoadEvaluator.getClientByteOutRatio(client);
		}
		avgLoadRatio /= this.activeHosts.size();
		
		// 2) Compute predicted average load ratio - bail out if above threshold
		double predictedAvgLoadRatio = avgLoadRatio + avgLoadRatio / (this.activeHosts.size() - 1);
		if (predictedAvgLoadRatio > this.maxLoadThresholdLowPlan) {
			return null;
		}
		
		// 3) Take the lowest loaded host except host 0 which is the default
		Set<RPubClientId> lowestSet = new HashSet<RPubClientId>(this.activeHosts);
		lowestSet.remove(new RPubClientId(0));
		RPubClientId lowestRPubClient = this.currentLoadEvaluator.getClientLowestByteOut(lowestSet);
		lowestSet.add(new RPubClientId(0));
		lowestSet.remove(lowestRPubClient);
		
		// Create our new proposed plan
		PlanImpl proposedPlan = new PlanImpl( (PlanImpl) (this.getCurrentPlan()) );
		LoadEvaluator loadEvaluator = this.currentLoadEvaluator;
		
		// 4) Distribute all channels to other hosts in the most equal way possible
		// Get client channels
		Set<String> channels = this.getCurrentPlan().getClientChannels(lowestRPubClient);
		for (String channel: channels) {
			
			// Take the lowest loaded host except lowestRPubClient and host 0
			RPubClientId targetRPubClient = loadEvaluator.getClientLowestByteOut(lowestSet);
			
			// Assign busiest channel to lowest busy server in proposed plan
			System.out.println("YYYMigrating " + channel + " to RPubClientId" + targetRPubClient.getId());
			proposedPlan.setMapping(channel, new PlanMappingImpl(proposedPlan.getPlanId(), channel, targetRPubClient));
			// Reestimate load ratio
			loadEvaluator = new NewPlanEstimatedLoadEvaluator(getCurrentPlan(), proposedPlan, currentLoadEvaluator, hostInfoMap, getCurrentTime());
			
		}
		
		// 5) Remove the host - mark it as inactive
		this.activeHosts.remove(lowestRPubClient);
		
		// Propagate new plan
		return proposedPlan;
		
	}

	private void ignoreReplicatedChannels() {
		boolean printReplicatedChannels = (this.getCurrentTime() % 1 == 0) && false;  
		
		if (printReplicatedChannels)
			System.out.print("   ReplicatedChannels={");
		
		this.ignoreChannels.clear();
		this.ignoreChannels.addAll(this.defaultIgnoreChannels);
		for (String channel: this.getCurrentPlan().getAllChannels()) {
			if (this.getCurrentPlan().getMapping(channel).getStrategy() != PlanMappingStrategy.DEFAULT_STRATEGY) {
				this.ignoreChannels.add(channel);
				
				if (printReplicatedChannels)
					System.out.print(channel + ",");
			}
		}
		
		if (printReplicatedChannels) {
			System.out.print("}");
			System.out.print(" | ActiveHost=" + this.activeHosts.size());
			System.out.print("\n");
		}
	}
	
}
