package Dynamoth.Core.Game.Replication.dynamic;

import Dynamoth.Util.Message.MessageImpl;

public class DReplicationMessage extends MessageImpl {

	private static final long serialVersionUID = -7037090886152288460L;
	
	private long timestamp;
	private byte[] data;

	private int id;

	private String messageId;

	public DReplicationMessage(int id, byte[] data, long timestamp, String msgId) {
		this.id = id;
		this.data = data;
		this.timestamp = timestamp;
		this.setMessageId(msgId);
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

	public String getMessageId() {
		return messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

}
