package Dynamoth.Core.Game.Replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;


public class RReplicationClient {
	
	public static int GLOBAL_CLIENT_ID = (new Random()).nextInt();

	
	private List<RReplicationPlayer> publishers;
	private List<RReplicationPlayer> subscribers;
	private Timer timer = new Timer();
	private String channelName;
	private byte[] data;
	
	public RReplicationClient(String channelName, int dataSize, int numPublishers, int numSubscribers) {
		this.channelName = channelName;
		this.data = new byte[dataSize];
		this.publishers = new ArrayList<RReplicationPlayer>();
		this.subscribers = new ArrayList<RReplicationPlayer>();

		for (int i=0; i < numPublishers; i++) {
			//this.players.add(new RPlayer(i, totalPlayerCount));
			this.publishers.add(new RReplicationPlayer());
		}
		
		for (int i=0; i < numSubscribers; i++) {
			//this.players.add(new RPlayer(i, totalPlayerCount));
			this.subscribers.add(new RReplicationPlayer());
		}
	}
	

	/**
	 * Connect to the network and launch all clients
	 */
	public void launch() {
		
		// Launch the appropriate clients
		int index=0;
		for (RReplicationPlayer player: publishers) {
			player.launch();
			System.out.println("Launched publisher " + index);
			index++;
		}
		
		for (RReplicationPlayer player: subscribers) {
			player.launch();
			player.subscribe(this.channelName);
			System.out.println("Launched subscriber " + index);
			index++;
		}
		
		System.out.println("Launched all clients :-)");
		
		setupClock();
	}
	
	private void setupClock() {
		timer.scheduleAtFixedRate(new TimerTask() {
			
			@Override
			public void run() {
				// Process events
				process();
			}
		}, 100, 100);
	}


	protected void process() {
		for(RReplicationPlayer player : publishers) {
			player.publish(this.channelName, new RReplicationMessage(GLOBAL_CLIENT_ID, this.data, System.nanoTime()));
		}
	}
	
}
