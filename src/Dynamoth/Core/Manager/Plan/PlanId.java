package Dynamoth.Core.Manager.Plan;

import Dynamoth.Core.RPubId;

public class PlanId extends RPubId {

	/**
	 * 
	 */
	private static final long serialVersionUID = -66021521651491650L;
	
	// Not the best thing in the world, but we assume that only one entity will
	// spawn IDs for new plans (the load balancer)
		
	static private int nextId = 0;

	protected PlanId() {
		super();
	}

	public PlanId(int id) {
		super(id);
	}
	
	public synchronized static PlanId generate() {
		int next = nextId;
		nextId++;
		return new PlanId(next);
	}
}
