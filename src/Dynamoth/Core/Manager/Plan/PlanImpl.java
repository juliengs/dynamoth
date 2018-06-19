package Dynamoth.Core.Manager.Plan;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.LoadAnalyzing.Channel;
import Dynamoth.Core.RPubNetworkEngine;

public class PlanImpl implements Plan {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4291159934319179868L;

	private PlanId planId;
	
	private Map<String,PlanMapping> planMappings = new ConcurrentHashMap<String, PlanMapping>();
	
	private int time = 0;
	
	protected PlanImpl() {
	}
	
	public PlanImpl(PlanId planId) {
		this.planId = planId;
	}
	
	/**
	 * Copy an existing plan
	 * @param plan Existing plan
	 */
	public PlanImpl(PlanImpl plan) {
		this.planId = new PlanId(plan.planId.getId());
		
		for (Map.Entry<String,PlanMapping> planMappingEntry: plan.planMappings.entrySet()) {
			planMappings.put(planMappingEntry.getKey(), new PlanMappingImpl((PlanMappingImpl) (planMappingEntry.getValue()) ));
		}
	}

	@Override
	public PlanId getPlanId() {
		return this.planId;
	}
	
	public void setPlanId(PlanId planId) {
		this.planId = planId;
	}
	
	/**
	 * Apply the plan's plan ID to all plan mappings
	 */
	public void applyPlanIdToMappings() {
		for (Map.Entry<String, PlanMapping> planEntry: planMappings.entrySet()) {
			PlanMappingImpl planMapping = (PlanMappingImpl)(planEntry.getValue());
			planMapping.setPlanId(this.getPlanId());
		}
	}
	
	/**
	 * Apply the plan's plan ID to changed plan mappings
	 */
	public void applyPlanIdToChangedMappings(Plan oldPlan) {
		for (Map.Entry<String, PlanMapping> planEntry: planMappings.entrySet()) {
			PlanMapping oldMapping = oldPlan.getMapping(planEntry.getKey());
			if(!oldMapping.equals(planEntry.getValue())) {
				PlanMappingImpl planMapping = (PlanMappingImpl)(planEntry.getValue());
				planMapping.setPlanId(this.getPlanId());
			}
		}
	}

	@Override
	public PlanMapping getMapping(String channelName) {
		PlanMapping mapping = planMappings.get(channelName);
		// If null, then return a default plan mapping which uses RPubId0 (temporary solution)
		if (mapping == null) {
			/* Return default rpub client to use
			 * For debugging purposes, here, we can override the RPubClientId to use
			 * for some channels
			 * */
			if (channelName.equals("track-info")) {
				return new PlanMappingImpl(this.getPlanId(), channelName, new RPubClientId(RPubNetworkEngine.getInfrastructureServer()));
			} else {
				return new PlanMappingImpl(this.getPlanId(), channelName, RPubClientId.Default);
			}
		} else {
			return mapping;
		}
	}
	
	public void setMapping(String channelName, PlanMapping mapping) {
		planMappings.put(channelName, mapping);
	}
	
	@Override
	public Set<String> getAllChannels() {
		return this.planMappings.keySet();
	}

	@Override
	public Set<String> getClientChannels(RPubClientId clientId) {
		Set<String> channels = new HashSet<String>();
		for (Map.Entry<String,PlanMapping> entry: planMappings.entrySet()) {
			if (Arrays.asList(entry.getValue().getShards()).contains(clientId)) {
				channels.add(entry.getKey());
			}
		}
		return channels;
	}

	@Override
	public RPubClientId[] getAllShards() {
		// Iterate through each plan and build a set of shards
		// Return this set of shards
		Set<RPubClientId> clientIds = new HashSet<RPubClientId>();
		for (Map.Entry<String,PlanMapping> entry: planMappings.entrySet()) {
			for (RPubClientId clientId : entry.getValue().getShards()) {
				clientIds.add(clientId);
			}
		}
		return clientIds.toArray(new RPubClientId[] {});
	}
	
	/**
	 * Adds all missing channels to this plan - which are channels which exist and for which
	 * info has been received by the LLAs but which are not defined in this plan 
	 * @param channels Mapping RPubClientId->Channels
	 */
	public void mergeMissingChannels(Map<RPubClientId, Map<String, Channel>> channels) {
		// List of all plan channels
		Set<String> planChannels = getAllChannels();
		
		// For every RPub client
		for (Map.Entry<RPubClientId, Map<String, Channel>> clientEntry: channels.entrySet()) {
			
			// Get a list of all used channels
			Set<String> usedChannels = new HashSet<String>(clientEntry.getValue().keySet());
			
			// For each channel: check if defined in the plan channels
			// if not defined, then create a simple mapping which maps to
			// the appropriate RPub Client
			for (String channel: usedChannels) {
				if (planChannels.contains(channel) == false) {
					// Create mapping
					PlanMappingImpl mapping = new PlanMappingImpl(this.getPlanId(), channel, clientEntry.getKey());
					this.setMapping(channel, mapping);
					
					System.out.println("RPubClientId" + clientEntry.getKey().getId() + ": adding missing channel to plan: " + channel);
				}
			}		
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((planId == null) ? 0 : planId.hashCode());
		result = prime * result
				+ ((planMappings == null) ? 0 : planMappings.hashCode());
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
		PlanImpl other = (PlanImpl) obj;
		if (planId == null) {
			if (other.planId != null)
				return false;
		} else if (!planId.equals(other.planId))
			return false;
		if (planMappings == null) {
			if (other.planMappings != null)
				return false;
		} else if (!planMappings.equals(other.planMappings))
			return false;
		return true;
	}

	@Override
	public boolean isCorrectShard(RPubClientId clientId, String channel) {
		PlanMapping mapping = getMapping(channel);
		return Arrays.asList(mapping.getShards()).contains(clientId); 
	}

	@Override
	public int getTime() {
		return this.time;
	}

	@Override
	public void setTime(int time) {
		this.time = time;
	}
}
