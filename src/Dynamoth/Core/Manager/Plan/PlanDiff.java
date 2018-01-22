package Dynamoth.Core.Manager.Plan;

import java.util.Set;

import Dynamoth.Core.Client.RPubClientId;

/**
 * Represents the difference between two 'plans', ie list of channels that 'changed',
 * list of new / abandoned channels for a given shard
 * 
 * Notes:
 * The two useful methods here will be to return a list of channel subscriptions and unsubscriptions
 * for a given shard. The strategy will be implicitly used here to return the appropriate set of
 * subs/unsubs so that the Manager does not need to take the strategy into account when applying the plan.
 * The Manager will need to take the strategy into account only when issuing publications.
 * 
 * @author Julien Gascon-Samson
 *
 */
public interface PlanDiff {
	
	/**
	 * Returns the set of channels in this PlanDiff instance; that is, the set of channels that changed 
	 * 
	 * @return Set of channels that changed
	 */
	Set<String> getChannels();
	
	/**
	 * Return the set of shards in this PlanDiff instance; that is, the set of shards for which there are changes.
	 * After calling this method, we can then call getUnsubscriptions and getSubscriptions with the returned RPubClientId.
	 *  
	 * @return Set of shards for which there are changes
	 */
	Set<RPubClientId> getShards();
	
	/**
	 * Returns the 'old' plan mapping for a given channel. If channel didn't change, old and new mapping will be the same. 
	 * @param channel Channel
	 * @return Old plan mapping
	 */
	PlanMapping getOldMapping(String channel);
	
	/**
	 * Returns the 'new' plan mapping for a given channel. If channel didn't change, old and new mapping will be the same. 
	 * @param channel Channel
	 * @return New plan mapping
	 */
	PlanMapping getNewMapping(String channel);
	
	/**
	 * Obtains a list of channels to unsubscribe on a given RPubClientId
	 * @param clientId RPub Client ID
	 * @return List of channels to unsubscribe
	 */
	Set<String> getAllUnsubscriptions(RPubClientId clientId);
	
	/**
	 * Obtains a list of channels to subscribe on a given RPubClientId
	 * @param clientId RPub Client ID
	 * @return List of channels to subscribe
	 */
	Set<String> getAllSubscriptions(RPubClientId clientId);
	
	/**
	 * Obtains a filtered list of channels to unsubscribe on a given RPubClientId, taking into consideration
	 * the list of channels that the caller is subscribed to
	 * @param clientId RPub Client ID
	 * @param currentSubscriptions Current list of subscribed channels
	 * @return
	 */
	Set<String> getOwnUnsubscriptions(RPubClientId clientId, Set<String> currentSubscriptions);
	
	/**
	 * Obtains a list of channels to unsubscribe on a given RPubClientId, taking into consideration
	 * the list of channels that the caller is subscribed to
	 * @param clientId RPub Client ID
	 * @param currentSubscriptions Current list of subscribed channels
	 * @return
	 */
	Set<String> getOwnSubscriptions(RPubClientId clientId, Set<String> currentSubscriptions);
}
