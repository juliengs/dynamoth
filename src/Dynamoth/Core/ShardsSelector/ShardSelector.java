package Dynamoth.Core.ShardsSelector;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.Manager.Plan.PlanMapping;

public interface ShardSelector {

	public RPubClientId[] selector(PlanMapping mapping, int clientKey);

}
