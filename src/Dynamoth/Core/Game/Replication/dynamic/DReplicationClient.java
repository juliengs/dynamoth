package Dynamoth.Core.Game.Replication.dynamic;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;


public class DReplicationClient {
	
	public static int CURRENT_PUBLISHER = -1;
	public static int GLOBAL_CLIENT_ID = (new Random()).nextInt();
	private static int DATA_SIZE = 250;
	
	private List<DReplicationPlayer> potentialPlayers;
	private List<DReplicationPlayer> publishers;
	private List<DReplicationPlayer> subscribers;
	private Timer timer = new Timer();
	private String channelName;
	private int delay;
	private int burst;
	private int numPublishers;
	private int numSubscribers;
	private boolean decrease;
	
	public DReplicationClient(String channelName, int numPublishers, int numSubscribers, int delay, int burst, boolean decrease) {
		this.channelName = channelName;
		this.numPublishers = numPublishers;
		this.numSubscribers = numSubscribers;
		this.delay = delay;
		this.burst = burst;
		this.decrease = decrease;
		this.publishers = new ArrayList<DReplicationPlayer>();
		this.subscribers = new ArrayList<DReplicationPlayer>();
		this.potentialPlayers = new ArrayList<DReplicationPlayer>();
	}
	
	public void connectPotentialPlayers() {
		for(int i = 0; i < this.numPublishers + this.numSubscribers; i++) {
			DReplicationPlayer player = new DReplicationPlayer(i);
			this.potentialPlayers.add(player);
			player.connect();
		}
	}

	
	/**
	 * Connect to the network and launch all clients
	 */
	public void launch() {
		
		setupClock();
		
		try {
			Thread.sleep(delay * 1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// publishers
		for (int i = 0; i < numPublishers;) {
			synchronized(publishers) {
				for(int j = 0; j < burst && i < numPublishers; j++) {
					DReplicationPlayer player = this.potentialPlayers.remove(0);
					this.publishers.add(player);
					System.out.println("Launched publisher id=" + player.getId());
					i++;
				}
			}
			/*
			try {
				Thread.sleep(delay * 1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/
		}
		
		//subscribers
		for (int i = 0; i < numSubscribers;) {
			for(int j = 0; j < burst && i < numSubscribers; j++) {
				DReplicationPlayer player = this.potentialPlayers.remove(0);
				this.subscribers.add(player);
				player.subscribe(this.channelName);
				System.out.println("Launched subscriber id=" + player.getId());
				i++;
			}
			try {
				Thread.sleep(delay * 1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(decrease) {
			try {
				Thread.sleep(60*1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			undoLaunch();
		}
	}
	
	private void undoLaunch() {
		int index = 0;
		/*for (int i = 0; i < numPublishers;) {
			synchronized(publishers) {
				for(int j = 0; j < burst && i < numPublishers; j++) {
					this.publishers.remove(0);
					System.out.println("Stopped publisher " + index);
					index++;
					i++;
				}
			}
			try {
				Thread.sleep(delay * 1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}*/
		
		//subscribers
		for (int i = 0; i < numSubscribers;) {
			for(int j = 0; j < burst && i < numSubscribers; j++) {
				DReplicationPlayer player = this.subscribers.remove(0);
				player.unsubscribe(this.channelName);
				System.out.println("Stopped subscriber " + index);
				index++;
				i++;
			}
			try {
				Thread.sleep(delay * 1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
		synchronized(publishers){
			for(DReplicationPlayer player : publishers) {
				CURRENT_PUBLISHER = player.getId();
				player.publish(this.channelName, new DReplicationMessage(GLOBAL_CLIENT_ID, new byte[DATA_SIZE], System.nanoTime(), UUID.randomUUID().toString()));
			}
			CURRENT_PUBLISHER = -1;
		}
	}
	
}
