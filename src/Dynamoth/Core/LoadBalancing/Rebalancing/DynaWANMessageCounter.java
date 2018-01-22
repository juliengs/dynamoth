/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Dynamoth.Core.LoadBalancing.Rebalancing;

/**
 *
 * @author Julien Gascon-Samson
 */
public class DynaWANMessageCounter {
	private int ssMessageCount = 0;
	private int slMessageCount = 0;
	private int llMessageCount = 0;
	
	public DynaWANMessageCounter(int ssMessageCount, int slMessageCount, int llMessageCount) {
		this.ssMessageCount = ssMessageCount;
		this.slMessageCount = slMessageCount;
		this.llMessageCount = llMessageCount;
	}

	/**
	 * @return the ssMessageCount
	 */
	public int getSSMessageCount() {
		return ssMessageCount;
	}

	/**
	 * @return the slMessageCount
	 */
	public int getSLMessageCount() {
		return slMessageCount;
	}

	/**
	 * @return the llMessageCount
	 */
	public int getLLMessageCount() {
		return llMessageCount;
	}
	
	public double getSSMessageRatio() {
		return ssMessageCount * 1.0 / (ssMessageCount + slMessageCount + llMessageCount);
	}
	
	public double getSLMessageRatio() {
		return slMessageCount * 1.0 / (ssMessageCount + slMessageCount + llMessageCount);
	}
	
	public double getLLMessageRatio() {
		return llMessageCount * 1.0 / (ssMessageCount + slMessageCount + llMessageCount);
	}
}
