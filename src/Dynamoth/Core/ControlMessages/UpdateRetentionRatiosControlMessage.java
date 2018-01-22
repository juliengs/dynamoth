package Dynamoth.Core.ControlMessages;

import java.util.HashMap;

public class UpdateRetentionRatiosControlMessage extends ControlMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 964354194267888913L;
	private HashMap<String, Double> retentionRatios;

	public UpdateRetentionRatiosControlMessage(HashMap<String,Double> retentionRatios) {
		this.retentionRatios = retentionRatios;
	}

	public HashMap<String, Double> getRetentionRatios() {
		return retentionRatios;
	}
	

}
