package Dynamoth.Core.Manager.Plan;

import java.io.Serializable;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.ShardsSelector.DynaWANLocalShardSelector;
import Dynamoth.Core.ShardsSelector.IdentityShardSelector;
import Dynamoth.Core.ShardsSelector.RepeatableRandomSelector;
import Dynamoth.Core.ShardsSelector.ShardSelector;

public interface PlanMapping extends Serializable {
	
	public enum PlanMappingStrategy {
		DEFAULT_STRATEGY(IdentityShardSelector.INSTANCE, IdentityShardSelector.INSTANCE) /* There is one shard OR the default value if there are multiple shards */,
		SUBSCRIBERS_FULLY_CONNECTED(IdentityShardSelector.INSTANCE, RepeatableRandomSelector.INSTANCE) /* Subscribers are connected to all shards / publishers are connected to one random shard */,
		PUBLISHERS_FULLY_CONNECTED(RepeatableRandomSelector.INSTANCE, IdentityShardSelector.INSTANCE)  /* Publishers are connected to all shards / subscribers are connected to one random shard */,
		PUBSUB_ROUTING(RepeatableRandomSelector.INSTANCE, RepeatableRandomSelector.INSTANCE) /* Publishers and subscribers are connected to one random shard / shard performs routing to all other shards */,
		DYNAWAN_ROUTING(DynaWANLocalShardSelector.INSTANCE, IdentityShardSelector.INSTANCE); /* Publishers and subscribers are connected to one random shard / shard performs routing to all other shards */

		private ShardSelector subscriptionShardSelector;
		private ShardSelector publicationShardSelector;
		
		private PlanMappingStrategy(ShardSelector subShardsSelector, ShardSelector pubShardsSelector) {
			this.subscriptionShardSelector = subShardsSelector;
			this.publicationShardSelector = pubShardsSelector;
		}
		
		public RPubClientId[] selectSubscriptionShards(PlanMapping mapping, int clientKey) {
			return this.subscriptionShardSelector.selector(mapping, clientKey);
		}
		
		public RPubClientId[] selectPublicationShards(PlanMapping mapping, int clientKey) {
			return this.publicationShardSelector.selector(mapping, clientKey);
		}
	}
	
	/**
	 * Obtains the channel name
	 * @return Channel name
	 */
	String getChannel(); 
	
	/**
	 * Obtains the array of shards for this plan mapping
	 * @return Array of shards
	 */
	RPubClientId[] getShards();
	
	/**
	 * Obtains the strategy that should be used for this plan mapping
	 * The strategy is only relevant if we have more than one shard
	 */
	PlanMappingStrategy getStrategy();
	
	/**
	 * Obtains the plan ID for this channel mapping. In the client's local plan,
	 * each mapping might correspond to a given plan ID.
	 * @return
	 */
	PlanId getPlanId();
}