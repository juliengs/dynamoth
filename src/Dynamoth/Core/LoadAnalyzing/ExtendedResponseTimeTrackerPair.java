package Dynamoth.Core.LoadAnalyzing;

public class ExtendedResponseTimeTrackerPair {

	private long senderTime = 0;
	private long receivedTime = 0;

	public ExtendedResponseTimeTrackerPair(long senderTime, long receivedTime) {
		this.senderTime = senderTime;
		this.receivedTime = receivedTime;
	}

	public long getSenderTime() {
		return senderTime;
	}

	public long getReceivedTime() {
		return receivedTime;
	}

	
}
