package Dynamoth.Core.Game;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class RClient {

	private List<RPlayer> players = new ArrayList<RPlayer>();
	
	private int playerStart;
	private int playerCount;
	private int totalPlayerCount;
	
	private double currentRGameTime = 0.0;
	
	private Timer timer = new Timer();
	
	public RClient(int playerStart, int playerCount, int totalPlayerCount) {
		this.playerStart = playerStart;
		this.playerCount = playerCount;
		this.totalPlayerCount = totalPlayerCount;
		
		for (int i=playerStart; i<playerStart + playerCount; i++) {
			//this.players.add(new RPlayer(i, totalPlayerCount));
			RPlayer player = new RPlayer(totalPlayerCount);
			this.players.add(player);
			
			// Allow/disallow subscriptions/publications if needed
			if (RConfig.ONLY_ONE_PUBLISHER && i>playerStart) {
				player.setPublishing(false);
			}
			if (RConfig.ONLY_ONE_SUBSCRIBER && i>playerStart) {
				player.setSubscribing(false);
			}
		}
	}

	/**
	 * Connect to the network and launch all clients
	 */
	public void launch() {
		
		// Launch the appropriate clients
		int index=0;
		for (RPlayer player: players) {
			player.launch();
			System.out.println("Launched " + index + " clients");
			index++;
		}
		
		System.out.println("Launched all clients :-)");
		
		setupClock();
	}
	
	/**
	 * Setup the RGame clock
	 */
	private void setupClock() {
		timer.scheduleAtFixedRate(new TimerTask() {
			
			@Override
			public void run() {
				// Process events
				process(0.1);
			}
		}, 100, 100);
	}
	
	/**
	 * Kernel-like loop to process "events" (ie players moving)
	 */
	public void process(double timeStep) {
		currentRGameTime += timeStep;
		
		// Ask every player to process
		for (RPlayer player: players) {
			if (( (int) (currentRGameTime*1000) % 1000) < 100) {
				if (player.getId() > -1 && player.sentMoveAction == false) {
					System.out.println("$$$ NO MOVE ACTION FOR: " + player.getId());
				}
				player.sentMoveAction = false;
			}
			player.process(timeStep);
		}
	}
}
