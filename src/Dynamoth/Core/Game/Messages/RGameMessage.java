package Dynamoth.Core.Game.Messages;

import Dynamoth.Util.Message.MessageImpl;

public class RGameMessage extends MessageImpl {

	/**
	 * 
	 */
	private static final long serialVersionUID = -690964254895999303L;

	private int playerId;

	public RGameMessage(int playerId) {
		this.playerId = playerId;
	}

	public int getPlayerId() {
		return this.playerId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + playerId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RGameMessage other = (RGameMessage) obj;
		if (playerId != other.playerId)
			return false;
		return true;
	}
}
