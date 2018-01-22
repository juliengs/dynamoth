package Dynamoth.Core.ControlMessages;

public class TrackInfoControlMessage extends ControlMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8706559773525644149L;
	private int time;
	private int[] responseTimes;
	private int averageTime;
	private int moveMessageCount;
	
	// The following are specific to RGame / DynAvail to track how many state updates
	// are missed during the reconfiguration process...
	private int stateUpdatesMissed = 0;
	private int stateUpdatesSent = 0;

	public TrackInfoControlMessage(int time, int averageTime, int moveMessageCount) {
		this(time, null, averageTime, moveMessageCount);
	}
	
	public TrackInfoControlMessage(int time, int[] responseTimes, int averageTime, int moveMessageCount) {
		this.time = time;
		this.responseTimes = responseTimes;
		this.averageTime = averageTime;
		this.moveMessageCount = moveMessageCount;
	}
	
	public TrackInfoControlMessage(int time, int[] responseTimes, int averageTime, int moveMessageCount, int stateUpdatesMissed, int stateUpdatesSent) {
		this.time = time;
		this.responseTimes = responseTimes;
		this.averageTime = averageTime;
		this.moveMessageCount = moveMessageCount;
		this.stateUpdatesMissed = stateUpdatesMissed;
		this.stateUpdatesSent = stateUpdatesSent;
	}

	public int getTime() {
		return time;
	}

	public int[] getResponseTimes() {
		return responseTimes;
	}

	public int getAverageTime() {
		return averageTime;
	}

	public int getMoveMessageCount() {
		return moveMessageCount;
	}

	public int getStateUpdatesMissed() {
		return stateUpdatesMissed;
	}

	public int getStateUpdatesSent() {
		return stateUpdatesSent;
	}

	
}
