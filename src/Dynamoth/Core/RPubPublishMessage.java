package Dynamoth.Core;

import java.io.Serializable;
import java.util.HashMap;

public class RPubPublishMessage extends RPubDataMessage {

	private static final long serialVersionUID = -6205795086065601947L;

	// For response time measurement:
	
	// Message sender time
	private long senderTime = 0;  
	
	// Hashmap of other <SenderID,SenderTime>
	private HashMap<RPubNetworkID, Long> timestamps = new HashMap<RPubNetworkID, Long>();  

	
	public RPubPublishMessage(RPubNetworkID sourceID, Serializable payload) {
		super(sourceID, payload);
		// TODO Auto-generated constructor stub
	}

	public long getSenderTime() {
		return senderTime;
	}

	public void setSenderTime(long senderTime) {
		this.senderTime = senderTime;
	}

	public HashMap<RPubNetworkID, Long> getTimestamps() {
		return timestamps;
	}
}
