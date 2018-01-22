package Dynamoth.Core.Game.Messages;

public class RGameActivateMessage extends RGameMessage {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3482413428380144627L;
	private int playerStart;
	private int playerEnd;
	private boolean active;
	private boolean flocking;
	private String region;

	public RGameActivateMessage(int playerStart, int playerEnd, boolean active)
	{
		this(playerStart, playerEnd, active, true, "");
	}
	
	public RGameActivateMessage(int playerStart, int playerEnd, boolean active, boolean flocking, String region) {
		super(-1);
		this.playerStart = playerStart;
		this.playerEnd = playerEnd;
		this.active = active;
		this.flocking = flocking;
		this.region = region;
	}

	public int getPlayerStart() {
		return playerStart;
	}

	public int getPlayerEnd() {
		return playerEnd;
	}

	public boolean isActive() {
		return active;
	}

	public boolean isFlocking() {
		return flocking;
	}

	public void setFlocking(boolean flocking) {
		this.flocking = flocking;
	}

	public String getRegion() {
		return region;
	}
}
