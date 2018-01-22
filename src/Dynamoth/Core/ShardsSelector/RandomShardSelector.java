package Dynamoth.Core.ShardsSelector;

import java.util.Random;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.Manager.Plan.PlanMapping;

public class RandomShardSelector implements ShardSelector {
	
	public static final RandomShardSelector INSTANCE = new RandomShardSelector();
	private Random rand = new Random();
	
	private RandomShardSelector() {}

	@Override
	public RPubClientId[] selector(PlanMapping mapping, int clientKey) {
		RPubClientId[] shards = mapping.getShards();
		return new RPubClientId[]{ shards[rand.nextInt(shards.length)] };
	}

}
