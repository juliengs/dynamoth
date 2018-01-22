package Dynamoth.Core.ShardsSelector;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.Manager.Plan.PlanMapping;

public class IdentityShardSelector implements ShardSelector {

	public static final IdentityShardSelector INSTANCE = new IdentityShardSelector();

	private IdentityShardSelector() {
		
	}
	
	@Override
	public RPubClientId[] selector(PlanMapping mapping, int clientKey) {
		return mapping.getShards();
	}

}
