package Dynamoth.Core.LoadBalancing.Rebalancing;

import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Core.Util.RPubUtil;

public abstract class AbstractRebalancer implements Rebalancer {

	private Plan currentPlan = null;
	private Plan newPlan = null;
	
	private int newPlanSetTime = -1;
	
	private int newPlanWaitInterval = 20;

	private int currentTime = 0;
	
	private Thread rebalancerThread = null;
	private boolean running = false;
	private boolean shouldStop = false;
	
	private int sleepTime = 500;
	
	public AbstractRebalancer(Plan currentPlan, int currentTime) {
		this.currentPlan = currentPlan;
		this.currentTime = currentTime;
		
		// Set the new plan set time to the current time
		this.newPlanSetTime = currentTime - 30;
	}

	protected synchronized void setNewPlan(Plan newPlan) {
		// If the new plan equals the current plan, then the new plan will be null
		if (newPlan == null || newPlan.equals(this.currentPlan)) {
			this.newPlan = null;
		} else {
			this.newPlan = newPlan;
			// Set the new time
			setNewPlanSetTime(RPubUtil.getCurrentSystemTime());
			
			//System.out.println("New plan summary:");
			//printPlanInfo(newPlan);
		}
	}

	private void printPlanInfo(Plan plan) {
		// Print the plan - debugging
		for (RPubClientId clientId: plan.getAllShards()) {
			System.out.println("---RPubClientId" + clientId.getId());
			for (String channel: plan.getClientChannels(clientId)) {
				System.out.println("------" + channel);
			}
		}
	}
	
	@Override
	public synchronized Plan getNewPlan() {
		return this.newPlan;
	}

	@Override
	public synchronized boolean isNewPlanAvailable() {
		return (this.newPlan != null);
	}
	
	@Override
	public synchronized boolean isRunning() {
		return this.running;
	}

	@Override
	public synchronized void start() {
		// Start processing on a new thread and invoke startInternal
		if (this.running)
			return;
		
		this.running = true;
		
		this.rebalancerThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				// Invoke processIteration until we must bail out
				while (shouldStop == false) {
					processIteration();
					try {
						Thread.sleep(sleepTime);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				shouldStop = false;
			}
		}, "Dynamoth-Rebalancer");
		
		this.rebalancerThread.start();
	}
	
	/**
	 * Method which continuously get called. It should compute rebalancing operation(s)
	 * but it should execute quickly so that the rebalancer can be stopped quickly.
	 */
	protected abstract void processIteration();

	@Override
	public synchronized void stop() {
		// If running set shouldStop to true
		if (this.running) {
			this.shouldStop = true;
			this.running = false;
		}
	}

	@Override
	public synchronized Plan getCurrentPlan() {
		return this.currentPlan;
	}

	@Override
	public synchronized void setCurrentPlan(Plan plan) {
		this.currentPlan = plan;
		//System.out.println("Set current plan summary:");
		//printPlanInfo(plan);
		// If the new plan equals the current plan, then the new plan will be null
		if (newPlan != null && newPlan.equals(plan)) {
			this.newPlan = null;
		}
	}

	public synchronized int getCurrentTime() {
		return currentTime;
	}

	public synchronized void setCurrentTime(int currentTime) {
		this.currentTime = currentTime;
	}
	
	protected synchronized int getNewPlanSetTime() {
		return newPlanSetTime;
	}

	protected synchronized void setNewPlanSetTime(int newPlanSetTime) {
		this.newPlanSetTime = newPlanSetTime;
	}

	protected synchronized int getNewPlanWaitInterval() {
		return newPlanWaitInterval;
	}

	protected synchronized void setNewPlanWaitInterval(int newPlanWaitInterval) {
		this.newPlanWaitInterval = newPlanWaitInterval;
	}
	
	/**
	 * Determines whether it is time to set a new plan. Compares the current time against
	 * the new plan set time and the new plan wait interval.
	 * @return True if we can safely set a new plan otherwise False.
	 */
	protected boolean canSetNewPlan() {
		int currentTime = RPubUtil.getCurrentSystemTime();
		return ( currentTime > getNewPlanSetTime() + getNewPlanWaitInterval() );
	}
}
