package Dynamoth.Core.ShardsSelector;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.Manager.Plan.PlanMapping;

public class DynaWANLocalShardSelector implements ShardSelector {

	public DynaWANLocalShardSelector() {}

	public static final DynaWANLocalShardSelector INSTANCE = new DynaWANLocalShardSelector();
	
	public static String defaultRegion = "eu-central-1";
	
	// Ugly mapping region -> shardId
	public static int regionToShard(String region) {
		if (region.equals("eu-central-1")) {
			return 0;
		} else if (region.equals("ap-southeast-2")) {
			return 1;
		} else if (region.equals("ap-southeast-1")) {
			return 3;
		} else {
			return 0;
		}
		/*
		if (region.equals("us-east-1")) {
			return 0;
		} else if (region.equals("eu-central-1")) {
			return 1;
		} else if (region.equals("ap-southeast-1")) {
			return 3;
		} else {
			return 0;
		}
		*/
	}
	
	private boolean mappingContainsShard(PlanMapping mapping, int shardId) {
		for (RPubClientId id : mapping.getShards()) {
			if (id.getId() == shardId) {
				return true;
			}
		}
		return false;
	}
	
	private RPubClientId[] selectBetweenAllShards(PlanMapping mapping, int clientKey) {
		int index = clientKey % mapping.getShards().length;
		return new RPubClientId[] {mapping.getShards()[index]};
	}
	
	@Override
	public RPubClientId[] selector(PlanMapping mapping, int clientKey) {
		// We don't care about the parameters...
		// We obtain the ec2 region for this machine... and then based on the region, we select an appropriate shard!
		String ec2Region = System.getProperty("ec2.region", DynaWANLocalShardSelector.defaultRegion);

		// 1- Check if default shard is available
		int defaultShard = regionToShard(ec2Region);
		if (mappingContainsShard(mapping, defaultShard)) {
			return new RPubClientId[] {new RPubClientId(defaultShard)};
		}
		
		// 2- Otherwise, if only one shard available then return it
		if (mapping.getShards().length == 0) {
			return mapping.getShards();
		}
		
		// 3- Otherwise, 2 shards available, do some logic here
		return selectBetweenAllShards(mapping, clientKey);
	}

}
