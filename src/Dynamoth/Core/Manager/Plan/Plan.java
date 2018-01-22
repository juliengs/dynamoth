package Dynamoth.Core.Manager.Plan;

import java.io.Serializable;
import java.util.Set;

import Dynamoth.Core.Client.RPubClientId;

public interface Plan extends Serializable {
	/**
	 * Obtains the ID for this plan. Each plan ID should be sequential so that a client can "apply" plans gradually.
	 * @return Plan ID
	 */
	PlanId getPlanId();
	
	/**
	 * Get all shards that are to be used for a given channel
	 * Typically, only one shard shall be returned; however, it might be the case that multiple shards
	 * correspond to the same channel.
	 * @param channelName
	 * @return Array of shards
	 */
	PlanMapping getMapping(String channelName);
	
	/**
	 * Retrieves a set of all channels defined in this plan
	 * @return Set of channels
	 */
	Set<String> getAllChannels();
	
	/**
	 * Retrieves a set of all channels defined in this plan for which a specific clientId is involved
	 * @return Set of channels for clientId
	 */
	Set<String> getClientChannels(RPubClientId clientId);
	
	/**
	 * Get a list of all the shards in this plan (only client id)
	 * Notifying clients of added and removed shards shall happen by using
	 * the corresponding control messages issued by the load balancer
	 * @return List of all shards
	 */
	RPubClientId[] getAllShards();
	
	/**
	 * Returns whether a correct shard has been used for a publication/subscription on channel 
	 * @param clientId Client ID
	 * @param channel Channel
	 * @return True if a correct shard was used; otherwise False
	 */
	boolean isCorrectShard(RPubClientId clientId, String channel);
	
	/**
	 * Gets the time at which the plan became active. Time is in seconds and should be set by clients
	 * upon reception.
	 * @return Plan time
	 */
	int getTime();
	
	/**
	 * Sets the time at which the plan became active. Time is in seconds and should be set by clients.
	 * @param time Plan time
	 */
	void setTime(int time);
}
