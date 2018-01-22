package Dynamoth.Core.Manager.Plan;

import java.util.Arrays;

import Dynamoth.Core.Client.RPubClientId;

public class PlanMappingImpl implements PlanMapping {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7601840255762785939L;
	private String channel;
	private RPubClientId[] shards;		
	private PlanMappingStrategy strategy;
	private PlanId planId;
	
	protected PlanMappingImpl() {
	}
	
	/**
	 * Copy constructor
	 * @param planMapping Other plan mapping
	 */
	public PlanMappingImpl(PlanMappingImpl planMapping) {
		this.planId = planMapping.planId;
		this.channel = planMapping.channel;
		this.shards = new RPubClientId[planMapping.shards.length];
		for (int i=0; i<planMapping.shards.length; i++) {
			this.shards[i] = new RPubClientId(planMapping.shards[i].getId());
		}
		this.strategy = planMapping.strategy;
	}
	
	public PlanMappingImpl(PlanId planId, String channel, RPubClientId shard) {
		this.planId = planId;
		this.channel = channel;
		this.shards = new RPubClientId[] {shard};
		this.strategy = PlanMappingStrategy.DEFAULT_STRATEGY;
	}
	
	public PlanMappingImpl(PlanId planId, String channel, RPubClientId[] shards) {
		this.planId = planId;
		this.channel = channel;
		this.shards = shards;
		this.strategy = PlanMappingStrategy.DEFAULT_STRATEGY;
	}
	
	public PlanMappingImpl(PlanId planId, String channel, RPubClientId[] shards, PlanMappingStrategy strategy) {
		this.planId = planId;
		this.channel = channel;
		this.shards = shards;
		this.strategy = strategy;
	}
	
	@Override
	public String getChannel() {
		return this.channel;
	}

	@Override
	public RPubClientId[] getShards() {
		return this.shards;
	}
	
	public void setShards(RPubClientId[] shards) {
		this.shards = shards;
	}

	@Override
	public PlanMappingStrategy getStrategy() {
		return this.strategy;
	}
	
	public void setStrategy(PlanMappingStrategy strategy) {
		this.strategy = strategy;
	}
	
	@Override
	public PlanId getPlanId() {
		return this.planId; 
	}
	
	public void setPlanId(PlanId planId) {
		this.planId = planId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((channel == null) ? 0 : channel.hashCode());
		result = prime * result + Arrays.hashCode(shards);
		result = prime * result
				+ ((strategy == null) ? 0 : strategy.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PlanMappingImpl other = (PlanMappingImpl) obj;
		if (channel == null) {
			if (other.channel != null)
				return false;
		} else if (!channel.equals(other.channel))
			return false;
		if (!Arrays.equals(shards, other.shards))
			return false;
		if (strategy != other.strategy)
			return false;
		return true;
	}

}
