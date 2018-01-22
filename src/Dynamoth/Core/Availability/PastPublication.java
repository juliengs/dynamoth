package Dynamoth.Core.Availability;

import Dynamoth.Core.RPubMessage;

/**
 *
 * @author Julien Gascon-Samson
 */
public class PastPublication {
	private String channel;
	private RPubMessage message;
	private int time; // in ms
	
	public PastPublication(String channel, RPubMessage message, int time) {
		this.channel = channel;
		this.message = message;
		this.time = time;
	}

	public String getChannel() {
		return channel;
	}

	public RPubMessage getMessage() {
		return message;
	}

	public int getTime() {
		return time;
	}

}
