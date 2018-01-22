package Dynamoth.Core.LoadBalancing.Rebalancing;

import Dynamoth.Core.Manager.Plan.Plan;

public interface Rebalancer {
	
	/**
	 * If this rebalancer decides that a new plan should be applied and a new plan is available
	 * return the new plan; otherwise, return null.
	 * Note: null will be returned if calling isNewPlanAvailable() returns false. 
	 * @return New plan that should be applied or null if no new plan needs to/should be applied.
	 */
	Plan getNewPlan();
	
	/**
	 * Returns whether this rebalancer thinks that a new plan should be applied.
	 * @return True if a new plan should be applied; otherwise, false.
	 */
	boolean isNewPlanAvailable();
	
	/**
	 * Starts this rebalancer. A rebalancer will run in the background to continuously generate plans.
	 */
	void start();
	
	/**
	 * Instructs this rebalancer to stop.
	 */
	void stop();
	
	/**
	 * Is this rebalancer running 
	 */
	boolean isRunning();
	
	/**
	 * Obtain the rebalancer's current plan
	 * @return Rebalancer's current plan
	 */
	Plan getCurrentPlan();
	
	/**
	 * Tells the rebalancer that the current plan has changed (and give the Rebalancer the current plan as input)
	 */
	void setCurrentPlan(Plan plan);
}
