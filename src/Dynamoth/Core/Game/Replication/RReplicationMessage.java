package Dynamoth.Core.Game.Replication;

import Dynamoth.Util.Message.MessageImpl;

public class RReplicationMessage extends MessageImpl {

	private static final long serialVersionUID = -7037090886152288460L;
	
	private long timestamp;
	private byte[] data;
	private int id;

	public RReplicationMessage(int id, byte[] data, long timestamp) {
		this.id = id;
		this.data = data;
		this.timestamp = timestamp;
	}

	public long getTimeStamp() {
		return this.timestamp;
	}

	public byte[] getData() {
		return this.data;
	}
	
	public int getId() {
		return this.id;
	}

}
