package Dynamoth.Core.Game.Messages;

import Dynamoth.Mammoth.NetworkEngine.NetworkEngineID;

public class RGameAcquirePlayerMessage extends RGameMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3469028449094124894L;
	private NetworkEngineID networkId = null;
	private String hostname;
	
	/**
	 * Constructor to use to query (obtain) a player ID
	 */
	public RGameAcquirePlayerMessage(String hostname, NetworkEngineID networkId) {
		super(-1);
		this.hostname = hostname;
		this.networkId  = networkId;
	}

	/**
	 * Constructor to use to send (return) a player ID
	 * @param playerId
	 */
	public RGameAcquirePlayerMessage(int playerId, NetworkEngineID networkId) {
		super(playerId);
		this.hostname = "";
		this.networkId = networkId;
	}
	
	/**
	 * Is it a query message (to acquire player id) or a return message (to return a player id)?
	 * @return
	 */
	public boolean isQuery() {
		return (this.getPlayerId() == -1);
	}

	public NetworkEngineID getNetworkId() {
		return networkId;
	}

	public String getHostname() {
		return hostname;
	}
}
