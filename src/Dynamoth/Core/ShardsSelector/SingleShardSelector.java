package Dynamoth.Core.ShardsSelector;

import java.util.Random;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.Manager.Plan.PlanMapping;

//To modify!
//Should be chosen at uniformly random across all shards
//but on a specific shard it should always return the same shard for the same list of shards
public class SingleShardSelector implements ShardSelector {
	
	public static final SingleShardSelector INSTANCE = new SingleShardSelector();
	
	private int rand = (new Random()).nextInt();
	
	private SingleShardSelector() {}

	@Override
	public RPubClientId[] selector(PlanMapping mapping, int clientKey) {
		String channel = mapping.getChannel();
		RPubClientId[] shards = mapping.getShards();
		int index = Math.abs((rand + channel.hashCode()) % shards.length);
		return new RPubClientId[]{ shards[index] };
	}

}
