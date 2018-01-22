package Dynamoth.Core.Util;

import Dynamoth.Core.Client.RPubClientId;

/**
 *
 * @author Julien Gascon-Samson
 */
public class ShardHasher {
	
	public static RPubClientId HashShard(String channelName, RPubClientId[] shards) {
		int hashCode = channelName.hashCode();
		if (hashCode < 0)
			hashCode = -hashCode;
		
		// Compute shard using modulo
		int shard = hashCode % shards.length;
		
		return shards[shard];
	}
	
}
