package Dynamoth.Core.ControlMessages;

import Dynamoth.Core.Manager.Plan.Plan;

public class ChangePlanControlMessage extends ControlMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8227938960283411444L;

	/**
	 * The new plan
	 */
	private Plan newPlan;
	
	public ChangePlanControlMessage(Plan newPlan) {
		this.newPlan = newPlan;
	}

	public Plan getNewPlan() {
		return newPlan;
	}

}
