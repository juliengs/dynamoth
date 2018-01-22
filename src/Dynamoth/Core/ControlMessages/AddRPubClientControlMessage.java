package Dynamoth.Core.ControlMessages;

import Dynamoth.Core.Client.RPubClientId;

public class AddRPubClientControlMessage extends ControlMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1563150231131873570L;

	private RPubClientId clientId;
	private String hostName;
	private int hostPort; 
	
	protected AddRPubClientControlMessage() {
	}
	
	public AddRPubClientControlMessage(RPubClientId clientId, String hostName, int hostPort) {
		this.clientId = clientId;
		this.hostName = hostName;
		this.hostPort = hostPort;
	}

	public RPubClientId getClientId() {
		return clientId;
	}

	public String getHostName() {
		return hostName;
	}

	public int getHostPort() {
		return hostPort;
	}

}
