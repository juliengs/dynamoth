package Dynamoth.Core.Game.Messages;

import Dynamoth.Core.Game.RPlayerFlockInfo;

public class RGameUpdateFlockInfoMessage extends RGameMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4242268726204120457L;

	private RPlayerFlockInfo flockInfo = null;
	
	public RGameUpdateFlockInfoMessage(RPlayerFlockInfo flockInfo) {
		super(-1);
		
		this.flockInfo = flockInfo;
	}

	public RPlayerFlockInfo getFlockInfo() {
		return this.flockInfo;
	}
}
