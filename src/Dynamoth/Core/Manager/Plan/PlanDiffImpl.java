package Dynamoth.Core.Manager.Plan;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.Manager.Plan.PlanMapping.PlanMappingStrategy;

public class PlanDiffImpl implements PlanDiff {

	private Plan oldPlan;
	private Plan newPlan;
	private int clientKey;
	
	/**
	 * Mapping - rpub client -> set of unsubscription channels
	 */
	private Map<RPubClientId, Set<String>> unsubscriptions = new HashMap<RPubClientId, Set<String>>();
	private Map<RPubClientId, Set<String>> subscriptions = new HashMap<RPubClientId, Set<String>>();
	
	public PlanDiffImpl(Plan oldPlan, Plan newPlan, int clientKey) {
		this.oldPlan = oldPlan;
		this.newPlan = newPlan;
		this.clientKey = clientKey;
		
		// Compute diff
		computeDiff();
		
		this.newPlan = newPlan;
	}
	
	/**
	 * Computes the diff between oldPlan and newPlan
	 */
	private void computeDiff() {
		// Get all shards in old plan and merge with all shards in new plan
		Set<RPubClientId> shards = new HashSet<RPubClientId>();
		shards.addAll(Arrays.asList(oldPlan.getAllShards()));
		shards.addAll(Arrays.asList(newPlan.getAllShards()));
		
		// Add default shard if not already in set
		if (shards.contains(RPubClientId.Default) == false) {
			shards.add(RPubClientId.Default);
		}
		
		// Create data structures
		for (RPubClientId shard: shards) {
			unsubscriptions.put(shard, new HashSet<String>());
			subscriptions.put(shard, new HashSet<String>());
		}
		
		
		// Compare the two instances

		// 1- Check for removed/added channels
		Set<String> removedChannels = new HashSet<String>(this.oldPlan.getAllChannels());
		removedChannels.removeAll(this.newPlan.getAllChannels());
		
		Set<String> addedChannels = new HashSet<String>(this.newPlan.getAllChannels());
		addedChannels.removeAll(this.oldPlan.getAllChannels());
		
		// For each removed channel, generate appropriate unsubscriptions
		for (String channel: removedChannels) {
			// Remove all channels from shards
			for (RPubClientId shard: oldPlan.getMapping(channel).getShards()) {
				unsubscriptions.get(shard).add(channel);
			}
		}
		
		// For each added channel, generate appropriate subscriptions
		// 2014-04-30: if channel is being added to a host other than "0",
		// then we should generate an unsubscription for host "0" because
		// it is possible that the channel was previously used and bound to
		// host "0" (channels not defined in plans are bound to host "0")
		for (String channel: addedChannels) {
			// Add all channels to shards
			boolean isBoundToDefaultShard = false;
			for (RPubClientId shard: newPlan.getMapping(channel).getShards()) {
				if (shard.equals(RPubClientId.Default)) {
					// Bound to (default) shard 0 - no need to do unsubscription first
					isBoundToDefaultShard = true;
				} else {
					//isBoundToDefaultShard = false;
				}
				//subscriptions.get(shard).add(channel);
			}
			
			// If we are not bound to shard 0 then we need to issue unsubscription
			// to shard 0
			if (isBoundToDefaultShard == false) {
				//unsubscriptions.get(RPubClientId.Default).add(channel);
			}
		}
		
		// 2- For other channels, check if the assignations changed
		Set<String> existingChannels = new HashSet<String>(newPlan.getAllChannels());
		// Retain all channels found in oldPlan
		//existingChannels.retainAll(oldPlan.getAllChannels());
		for (String existingChannel: existingChannels) {
			Set<RPubClientId> channelUnsubscriptions = new HashSet<RPubClientId>();
			Set<RPubClientId> channelSubscriptions = new HashSet<RPubClientId>();
			// Compute unsubs and subs
			computeSubscriptions(existingChannel, channelUnsubscriptions, channelSubscriptions);
			// Add them to our map
			for (RPubClientId shard: channelUnsubscriptions) {
				unsubscriptions.get(shard).add(existingChannel);
			}
			for (RPubClientId shard: channelSubscriptions) {
				subscriptions.get(shard).add(existingChannel);
			}
		}
	}
	
	/*
	 * IMPORTANT - TODO
	 * DEBUG ONE CLIENT (CONFIGURE DISTMOTH LAUNCHER TO SPAWN ONLY ONE CLIENT)
	 * ADD DEBUG FLAGS TO CMDLINE IN DISTMOTH LAUNCHER
	 * ESTABLISH SSH TUNEL
	 * PUT BREAKPOINT IN COMPUTESUBSCRIPTIONS
	 * CHECK WHY UNREGISTRATIONS DO NOT WORK WHEN APPLYING PFC WITHOUT AN EXPLICIT
	 * INITIAL PLAN (TILE_0_0 DEFAULT MAPS TO 0 BUT IF IT EXPLICITLY MAPS TO 0 IT WORKS)
	 */
	
	private void computeSubscriptions(String channel, Set<RPubClientId> unsubscriptions, Set<RPubClientId> subscriptions) {
		PlanMapping oldMapping = this.oldPlan.getMapping(channel);
		PlanMapping newMapping = this.newPlan.getMapping(channel);
		
		if (channel.equals("tile_0_0")) {
			channel = channel + "";
		}
		
		// Clear out unsubs and subs just to be sure
		unsubscriptions.clear();
		subscriptions.clear();
		
		// If mappings are the same... -> then we return right away
		if (oldMapping.equals(newMapping)) {
			return;
		}
		
		// At this point, mappings are different and we gotta be smart depending on the old & new strategies...		
		PlanMappingStrategy oldStrategy = oldMapping.getStrategy();
		PlanMappingStrategy newStrategy = newMapping.getStrategy();
		
		//Get the current subscription.
		//Note: should always return the same value (cannot be random)
		Set<RPubClientId> currentSubscriptions = new HashSet<RPubClientId>();
		currentSubscriptions.addAll(Arrays.asList(oldStrategy.selectSubscriptionShards(oldMapping, clientKey)));

		//Get subscription set
		subscriptions.addAll(Arrays.asList(newStrategy.selectSubscriptionShards(newMapping, clientKey)));
		
		//Get unsubscription set
		unsubscriptions.addAll(currentSubscriptions);
		unsubscriptions.removeAll(subscriptions);
		subscriptions.removeAll(currentSubscriptions);

	}
	
	

	@Override
	public Set<String> getChannels() {
		// Return all channels in unsubscriptions and subscriptions
		Set<String> channels = new HashSet<String>();
		for (Set<String> set: unsubscriptions.values()) {
			channels.addAll(set);
		}
		for (Set<String> set: subscriptions.values()) {
			channels.addAll(set);
		}
		return channels;
	}

	@Override
	public Set<RPubClientId> getShards() {
		// Return keyset of the union of both maps (subs-unsubs)
		Set<RPubClientId> shards = new HashSet<RPubClientId>();
		shards.addAll(subscriptions.keySet());
		shards.addAll(unsubscriptions.keySet());
		return shards;
	}

	@Override
	public PlanMapping getOldMapping(String channel) {
		return this.oldPlan.getMapping(channel);
	}

	@Override
	public PlanMapping getNewMapping(String channel) {
		return this.newPlan.getMapping(channel);
	}

	@Override
	public Set<String> getAllUnsubscriptions(RPubClientId clientId) {
		return unsubscriptions.get(clientId);
	}

	@Override
	public Set<String> getAllSubscriptions(RPubClientId clientId) {
		return subscriptions.get(clientId);
	}

	@Override
	public Set<String> getOwnUnsubscriptions(RPubClientId clientId, Set<String> currentSubscriptions) {
		Set<String> unsubscriptions = new HashSet<String>(this.getAllUnsubscriptions(clientId));
		// Retain only 'my' unsubscriptions
		unsubscriptions.retainAll(currentSubscriptions);
		return unsubscriptions;
	}

	@Override
	public Set<String> getOwnSubscriptions(RPubClientId clientId, Set<String> currentSubscriptions) {
		Set<String> subscriptions = new HashSet<String>(this.getAllSubscriptions(clientId));
		// Retain only 'my' subscriptions
		subscriptions.retainAll(currentSubscriptions);
		return subscriptions;
	}

}
