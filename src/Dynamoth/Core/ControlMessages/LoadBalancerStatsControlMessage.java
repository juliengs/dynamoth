package Dynamoth.Core.ControlMessages;

public class LoadBalancerStatsControlMessage extends ControlMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8640432154565570110L;
	
	private long inMessage = 0;
	private long outMessage = 0;
	private long computedByteIn = 0;
	private long computedByteOut = 0;
	private long measuredByteIn = 0;
	private long measuredByteOut = 0;
	private double[] loadRatios = new double[] {};
	private boolean rebalancingRequested = false;
	private int hostCount = 1;

	public LoadBalancerStatsControlMessage(long inMessage, long outMessage, long computedByteIn, long computedByteOut, long measuredByteIn, long measuredByteOut, double[] loadRatios, boolean rebalancingTriggered, int hostCount) {
		this.inMessage = inMessage;
		this.outMessage = outMessage;
		this.computedByteIn = computedByteIn;
		this.computedByteOut = computedByteOut;
		this.measuredByteIn = measuredByteIn;
		this.measuredByteOut = measuredByteOut;
		this.loadRatios = loadRatios;
		this.rebalancingRequested = rebalancingTriggered;
		this.hostCount = hostCount;
	}


	public long getInMessage() {
		return inMessage;
	}


	public long getOutMessage() {
		return outMessage;
	}


	public long getComputedByteIn() {
		return computedByteIn;
	}


	public long getComputedByteOut() {
		return computedByteOut;
	}


	public long getMeasuredByteIn() {
		return measuredByteIn;
	}


	public long getMeasuredByteOut() {
		return measuredByteOut;
	}


	public double[] getLoadRatios() {
		return loadRatios;
	}


	public boolean isRebalancingRequested() {
		return rebalancingRequested;
	}


	public int getHostCount() {
		return hostCount;
	}
}
