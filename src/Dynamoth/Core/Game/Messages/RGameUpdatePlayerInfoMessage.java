package Dynamoth.Core.Game.Messages;

public class RGameUpdatePlayerInfoMessage extends RGameMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2275813240963908132L;
	private int playerCount = 0;
	private int flockCount = 0;
	
	public RGameUpdatePlayerInfoMessage(int playerCount, int flockCount) {
		super(-1);
		
		this.playerCount = playerCount;
		this.flockCount  = flockCount;
	}

	public int getPlayerCount() {
		return playerCount;
	}

	public int getFlockCount() {
		return flockCount;
	}

}
