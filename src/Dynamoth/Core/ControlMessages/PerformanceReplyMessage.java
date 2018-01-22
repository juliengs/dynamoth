package Dynamoth.Core.ControlMessages;

public class PerformanceReplyMessage extends ControlMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6225119360724132314L;
	private int sourceMessageHash = 0;
	
	public PerformanceReplyMessage(int sourceMessageHash) {
		this.sourceMessageHash = sourceMessageHash;
	}

	public int getSourceMessageHash() {
		return sourceMessageHash;
	}

}
