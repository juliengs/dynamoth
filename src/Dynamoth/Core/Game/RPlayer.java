package Dynamoth.Core.Game;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import Dynamoth.Mammoth.NetworkEngine.Exceptions.AlreadyConnectedException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.ChannelExistsException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchChannelException;
import Dynamoth.Core.RPubNetworkEngine;
import Dynamoth.Core.Client.JedisRPubClient;
import Dynamoth.Core.Game.Messages.RGameAcquirePlayerMessage;
import Dynamoth.Core.Game.Messages.RGameActivateMessage;
import Dynamoth.Core.Game.Messages.RGameMoveMessage;
import Dynamoth.Core.Game.Messages.RGameUpdateFlockInfoMessage;
import Dynamoth.Core.LoadAnalyzing.AbstractResponseTimeTracker;
import Dynamoth.Core.LoadBalancing.CostModel.CostAnalyzer;
import Dynamoth.Core.Manager.DynamothRPubManager;
import Dynamoth.Util.Message.Handler;
import Dynamoth.Util.Message.Message;
import Dynamoth.Util.Message.Reactor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RPlayer {

	// Wait times when a player reached destination before generating a new action
	private static double PLAYER_WAIT_INTERVAL_MIN = 1.9;
	private static double PLAYER_WAIT_INTERVAL_MAX = 2.1;
	
	// 0.249->0.251
	// 0.049->0.051
	private static double ACTION_REPEAT_INTERVAL_MIN = 0.499/2.0 * 4;
	private static double ACTION_REPEAT_INTERVAL_MAX = 0.501/2.0 * 4;
		
	// Player ID
	private int id;
	
	// Lock object to prevent sync issues
	private Object idLock = new Object();
	
	// Mammoth-related stuff: engine and reactor
	private RPubNetworkEngine engine;
	private Reactor reactor;
	
	// Some timers / counters used to determine if (when) a new action should be generated
	private double lastActionTimeout = 0.0;
	private double nextActionTime = -1.0;
	private double actionRepeatTimeout = 0.0;
	private double actionRepeatTime = -1.0;
	private int counter = 0;
	
	// Move message message indices for every tile channel
	private Map<String, AtomicInteger> messageIndices = new ConcurrentHashMap<String, AtomicInteger>();

	// Current tile for that player
	private int currentTileX = -1;
	private int currentTileY = -1;
	
	// Active or not
	private AtomicBoolean active = new AtomicBoolean(true);
	
	// Are we publishing / subscribing
	private boolean publishing = true;
	private boolean subscribing = true;
	
	// Our random object
	private Random random = new Random();
	
	// Current flock info
	private RPlayerFlockInfo flockInfo = new RPlayerFlockInfo();
	private Object flockInfoLock = new Object();
	// Currently selected hotspot
	private RPlayerFlockInfoHotspot currentHotspot = null;
	
	// Avatars for all players including mine
	private List<RPlayerAvatar> avatars = new ArrayList<RPlayerAvatar>();
	
	// TIMING - DBG STUFF
	private long systemTimeStart = -1;
	private double rgameCurrentTime = 0.0;
	public boolean sentMoveAction = false; 
	
	/**
	 * Construct RPlayer with no ID; ID will be queried from the server
	 * @param totalPlayerCount Total Player Count
	 */
	public RPlayer(int totalPlayerCount) {
		this.id = -1;
		
		for (int i=0; i<totalPlayerCount; i++) {
			RPlayerAvatar avatar = new RPlayerAvatar();
			avatars.add(avatar);
		}
	}
	
	/**
	 * Construct RPlayer with a specific ID
	 * @param id Specific Player ID
	 * @param totalPlayerCount Total Player Count
	 */
	public RPlayer(int id, int totalPlayerCount) {
		this.id = id;
		
		for (int i=0; i<totalPlayerCount; i++) {
			RPlayerAvatar avatar = new RPlayerAvatar();
			avatars.add(avatar);
		}
	}

	/**
	 * 
	 */
	public void launch() {
		/*
		// Print # threads before launch
		System.out.println("--> Before launch, we have " + Thread.activeCount() + " threads");
		// Print stack info for all threads
		for (Map.Entry<Thread, StackTraceElement[]> entry: Thread.getAllStackTraces().entrySet()) {
			System.out.println("   --> " + entry.getKey().getName());
			for (StackTraceElement element: entry.getValue()) {
				System.out.println("      --> " + element.toString());
			}
		}
		*/
		
		// Connect to the network
		engine = new RPubNetworkEngine();
    	try {
			engine.connect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AlreadyConnectedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	// DEBUG- specify in our plan that all tile_0_0 msgs should go to rpubclientid1 instead of the default (0)
    	DynamothRPubManager dynamoth = (DynamothRPubManager) this.engine.getRPubManager();
    	//dynamoth.DebugForceRerouteTileMessages = true;
    	
    	
    	// Reactor
    	reactor = new Reactor("RClient" + this.id + "Reactor", engine);
    	
    	// Register for incoming RGameMoveMessages
    	reactor.register(RGameMoveMessage.class, new Handler() {
			
			@Override
			public void handle(Message msg) {
				handleGameMoveMessage((RGameMoveMessage)msg);		
			}
		});
    	
    	// Register for incoming RGameAcquirePlayerMessage
    	reactor.register(RGameAcquirePlayerMessage.class, new Handler() {
			
			@Override
			public void handle(Message msg) {
				handleGameAcquirePlayerMessage((RGameAcquirePlayerMessage)msg);		
			}
		});
    	
    	// Register for incoming RGameUpdateFlockInfoMessages
    	reactor.register(RGameUpdateFlockInfoMessage.class, new Handler() {
			
			@Override
			public void handle(Message msg) {
				handleGameUpdateFlockInfoMessage((RGameUpdateFlockInfoMessage)msg);		
			}
		});
    	
    	// Register for incoming RGameActivateMessages
    	reactor.register(RGameActivateMessage.class, new Handler() {
			
			@Override
			public void handle(Message msg) {
				handleGameActivateMessage((RGameActivateMessage)msg);		
			}
		});    	    	
    	
		// Query for player ID if playerId is -1
    	synchronized(this.idLock) {
    		if (this.id == -1) {
    			// Get hostname
    			String hostname = "<Unknown hostname>";
				try {
					hostname = InetAddress.getLocalHost().getHostName();
				} catch (UnknownHostException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
    			try {
					engine.send("acquirePlayers-query", new RGameAcquirePlayerMessage(hostname, this.engine.getId()));
				} catch (ClosedChannelException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NoSuchChannelException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		} else {
    			// Id already set (shouldn't happen), create tile channels
    			createAllTileChannels();
    		}
    	}
    	
		// Subscribe to the rgame-broadcast channel
		try {
			engine.subscribeChannel("rgame-broadcast", engine.getId());
		} catch (NoSuchChannelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    	// Setup fake flockInfo
    	/*
    	for (int i=0; i<RConfig.HOTSPOT_COUNT_X; i++) {
    		for (int j=0; j<RConfig.HOTSPOT_COUNT_Y; j++) {
    			flockInfo.getFlockWeights().put(new RPlayerFlockInfoHotspot(i, j), 1);
    		}
    	}*/
    	//flockInfo.getFlockWeights().put(new RPlayerFlockInfoHotspot(0, 0), 20);
    	//flockInfo.getFlockWeights().put(new RPlayerFlockInfoHotspot(2, 2), 20);
	}
	
	private void createAllTileChannels() {
		int tileCountX = RConfig.getTileCountX();
		int tileCountY = RConfig.getTileCountY();
		
		for (int i=0; i<tileCountX; i++) {
			for (int j=0; j<tileCountY; j++) {
				String tileChannelName = generateTileChannelName(i, j);
				try {
					engine.createChannel(tileChannelName);
					System.out.println("Creating tile channel... " + tileChannelName);
				} catch (ChannelExistsException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}	
		}
	}
	
	private String generateTileChannelName(int tileX, int tileY) {
		return "tile_" + tileX + "_" + tileY;
	}
	
	private void handleGameAcquirePlayerMessage(RGameAcquirePlayerMessage message) {
		// Set ID
		
		// We could check if msg -> getPlayerId != -1
		
		synchronized (this.idLock) {
			this.id = message.getPlayerId();
		}
		
		System.out.println("Acquired player ID " + this.id + " ; Subscribing to move channels...");
		
		// DEBUG : put playerId in Jedis
		DynamothRPubManager dynamoth = (DynamothRPubManager)(engine.getRPubManager());
		for (JedisRPubClient jrc: dynamoth.rpubClients.values()) {
			jrc.playerId = this.id;
		}
		
		// Subscribe to move channels to receive a copy of the messages
		//createAllTileChannels();
		
		// If id is 0 then register as default handler for uncaught exceptions
		/*
		if (this.id == 0) {
			Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
	
				@Override
				public void uncaughtException(Thread t, Throwable e) {
					// Send special message
					try {
						engine.send("dynamoth-debug", new RPlayerCrashedControlMessage());
					} catch (ClosedChannelException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (NoSuchChannelException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				
			});
		}*/
	}

	/**
	 * Update the corresponding avatar
	 * 
	 * @param message
	 */
	private void handleGameMoveMessage(RGameMoveMessage message) {
		RPlayerAvatar avatar = this.avatars.get(message.getPlayerId());
		avatar.setBaseX(message.getBaseX());
		avatar.setBaseY(message.getBaseY());
		avatar.setCurrentX(message.getBaseX());
		avatar.setCurrentY(message.getBaseY());
		avatar.setTargetX(message.getTargetX());
		avatar.setTargetY(message.getTargetY());
		
		// Add the counter
		// TODO - at some point in the future, allow duplicates...?
		avatar.getCounters().add(message.getCounter());
		
		AbstractResponseTimeTracker.getInstance().addMoveMessage();
	
		// Feed msg id
		avatar.feedMessageId(message.getMessageId());
		
		// Compute delta missing messages and message id and feed this to our response time tracker
		int sent = avatar.getCurrentMessageId() - avatar.getPreviousMessageId();
		int missed = avatar.getMissingMessages() - avatar.getPreviousMissingMessages();
		AbstractResponseTimeTracker.getInstance().addStateUpdatesMissed(missed);
		AbstractResponseTimeTracker.getInstance().addStateUpdatesSent(sent);
		
		//System.out.println(AbstractResponseTimeTracker.nanosecondsToMilliseconds(System.nanoTime()) + " [" + this.id + "]" + " Updating player " + message.getPlayerId() + " id=" + message.getMessageId() + " missed=" + avatar.getMissingMessages());
		//System.out.println("[" + this.id + "]" + " Updating player " + message.getPlayerId() + " id=" + message.getMessageId());
	}
	
	private void handleGameUpdateFlockInfoMessage(RGameUpdateFlockInfoMessage message) {
		// Update flocking table
		synchronized( flockInfoLock ) {
			this.flockInfo = message.getFlockInfo();
			System.out.println("*** Replaced Flock Info ***: " + this.flockInfo.toString());
		}
	}
	
	private void handleGameActivateMessage(RGameActivateMessage message) {
		if (this.id >= message.getPlayerStart() && this.id < message.getPlayerEnd()) {
			
			// If region is defined and this is not OUR region, then ignore
			if (message.getRegion().equals("") == false) {
				String ourRegion = System.getProperty("ec2.region", "");
				if (message.getRegion().equals(ourRegion) == false) {
					// Ignore
					return;
				}
			}
			
			this.active.set(message.isActive());
			
			// If DISABLING then UNSUBSCRIBE
			if (message.isActive() == false) {
				// Unregister old tile if not -1
				if (currentTileX >= 0 && currentTileY >= 0) {
					doSubscription(currentTileX, currentTileY, false, true);
				}
			} /* If ENABLING then (RE)SUBSCRIBE */ else {
				
				// Randomize position
				RPlayerAvatar avatar = this.avatars.get(this.id);
				teleportAvatar(avatar, message.isFlocking());
				
				doSubscription(currentTileX, currentTileY, true, true);
			}
		}
	}
	
	/**
	 * Process player actions (ie move)
	 */
	public void process(double timeStep) {
		
		// Time counting
		if (systemTimeStart == -1) {
			systemTimeStart = System.currentTimeMillis();
		}
		rgameCurrentTime += timeStep;
		// Output time drift
		//System.out.println("$$$ Time drift: " + ((System.currentTimeMillis() - systemTimeStart) / 1000.0 - rgameCurrentTime) );
		
		/*
		// Move all avatars according to their speed and their target point. Do it less often because it is quite demanding.
		if ( ((int)rgameCurrentTime * 1000.0) % 1000 < 50 ) {
			System.out.println("XXXX MOVING AVATARS XXXX");
			for (RPlayerAvatar avatar: avatars) {
				avatar.move(timeStep);
			}
		}*/
		
		// Check for tile registration only if id is not -1 and not first move
		if (this.id != -1 && counter > 0) {
			// Move our avatar (since we disabled the moving of all avatars)
			avatars.get(this.id).move(timeStep);
			
			// After moving all avatars including ours, obtain the current tile X / Y from the position and
			// compare to the currentTileXY. Then, if they changed, unregister from the old tile and register to the new tile.
			double avatarCurrentX = avatars.get(this.id).getCurrentX();
			double avatarCurrentY = avatars.get(this.id).getCurrentY();
			//System.out.println("Avatar Current Position (" + avatarCurrentX + ";" + avatarCurrentY + ")");
			int newTileX = RConfig.getTileX(avatarCurrentX);
			int newTileY = RConfig.getTileY(avatarCurrentY);
			if (newTileX != currentTileX || newTileY != currentTileY) {
				// Unregister old tile if not -1
				if (currentTileX >= 0 && currentTileY >= 0) {
					doSubscription(currentTileX, currentTileY, false);
				}
				
				// Update current tile x and y
				currentTileX = newTileX;
				currentTileY = newTileY;
				
				// Register new tile
				doSubscription(currentTileX, currentTileY, true);
			}
		}
		
		// If action repeat time is not set, choose a new one randomly
		if (actionRepeatTime < 0) {
			actionRepeatTime = random.nextDouble() * (ACTION_REPEAT_INTERVAL_MAX - ACTION_REPEAT_INTERVAL_MIN) + ACTION_REPEAT_INTERVAL_MIN;
		}
		
		// If next action time is not set, choose a new one randomly
		if (nextActionTime < 0) {
			nextActionTime = random.nextDouble() * (PLAYER_WAIT_INTERVAL_MAX - PLAYER_WAIT_INTERVAL_MIN) + PLAYER_WAIT_INTERVAL_MIN;
		}
		
		// If id == -1 then return (player id not acquired)
		synchronized(this.idLock) {
			if (this.id == -1) {
				//System.out.println("Warning: cannot issue move message because playerID is not set !");
				return;
			}
		}
		
		// Get the avatar
		RPlayerAvatar avatar = this.avatars.get(this.id);
		
		// Check whether should repeat the next action (if counter > 0, can we remove that)
		if (counter > 0) {
			actionRepeatTimeout += timeStep;
			if (actionRepeatTimeout >= actionRepeatTime) {
				// Repeat (resend) the action
				sendMoveMessage(avatar);
				
				//System.out.println("$$$ Repetition message: " + (System.currentTimeMillis() / 1000) + ":" + this.id);
				
				actionRepeatTimeout = 0.0;
				actionRepeatTime = -1.0;
			}
		}
		
		// If first move or we reached the destination point
		if (counter == 0 || avatar.isDestinationReached()) {
			// Increase the timeout of the last action
			lastActionTimeout += timeStep;
			
			// Choose new target point if first move or timeout >= next action time
			if (counter == 0 ||  lastActionTimeout >= nextActionTime) {
				
				// We will use the Flock Info to generate a new point
				// If we are not in a hotspot OR we should not stay inside the same hotspot, then
				// time to choose a new hotspot
				synchronized( flockInfoLock ) {
					if (currentHotspot == null || flockInfo.shouldStayInsideHotspot(random) == false) {
						// Choose a new hotspot (or random point)
						currentHotspot = flockInfo.generateRandomHotspot(random);
					}
				}
				
				double newX = 0.0, newY = 0.0;
				// If we have a hotspot, then generate inside the hotspot
				if (currentHotspot != null) {
					// Generate a random point in the hotspot
					double points[] = new double[2];
					currentHotspot.generateRandomPointInHotspot(random, points);
					newX = points[0];
					newY = points[1];
					System.out.println("Generating point into hotspot: " + "X=" + currentHotspot.getX() + "Y=" + currentHotspot.getY());
				} else {
					// Otherwise, then generate a move randomly on the map
					newX = random.nextDouble() * (RConfig.MAP_BOUND_X * 2 - 1) - RConfig.MAP_BOUND_X;
					newY = random.nextDouble() * (RConfig.MAP_BOUND_Y * 2 - 1) - RConfig.MAP_BOUND_Y;
					System.out.println("Generating point randomly");
				}
				
				// If counter == 0 (first move)
				// then set base position too!
				if (counter == 0) {
					teleportAvatar(avatar, false);
				}
				
				// We might not do that, it is possible that those values get updated only via the channel
				avatar.setTargetX(newX);
				avatar.setTargetY(newY);
				
				// We have to send a message for the new action
				if (counter>0) {
					sendMoveMessage(avatar);
				}
				
				// Reset counters
				lastActionTimeout = 0.0;
				nextActionTime = -1.0;
				
				// Increment msg counter
				this.counter++;
				/*if (counter>3) {
					counter++;
				}*/
				
				// FOR ALL AVATARS, print missing msgs
				/*
				int index=0;
				for (RPlayerAvatar av: avatars) {
					for (Integer missingCounter : av.getMissingCounters()) {
						System.out.println(id + " | MISSING av#" + index + ",c=" + missingCounter);
					}
					index++;
				}
				*/
				// CAUSES A CONCURR HASH MAP EXCPTN!
			}
		}

	}

	private void teleportAvatar(RPlayerAvatar avatar, boolean fakeFlock) {
		// JGS FAKE FLOCK - generate at 0,0 25% of TIME
		double baseX = 0.0, baseY = 0.0;
		double mapBoundX = RConfig.MAP_BOUND_X, mapBoundY = RConfig.MAP_BOUND_Y;
		if (fakeFlock && RConfig.ENABLE_FAKE_FLOCKING && random.nextDouble() < 0.50) {
			mapBoundX = mapBoundX * 0.20;
			mapBoundY = mapBoundY * 0.20;
		}
		baseX = random.nextDouble() * (mapBoundX * 2 - 1) - mapBoundX;
		baseY = random.nextDouble() * (mapBoundY * 2 - 1) - mapBoundY;
		
		avatar.setBaseX(baseX);
		avatar.setBaseY(baseY);
		avatar.setCurrentX(baseX);
		avatar.setCurrentY(baseY);
	}
	
	private void sendMoveMessage(RPlayerAvatar avatar) {
		// Send only if rgame is active and if avatar is publishing
		if (this.active.get() && isPublishing()) {
			
			// MultiPub - restrict sending from us-east-1
			/*
			String ec2Region = System.getProperty("ec2.region", "");
			if (ec2Region.equals("us-east-1") == false)
				return;
			*/
			
			// Send to the proper channel corresponding to the tile
			String tileChannelName = this.generateTileChannelName(currentTileX, currentTileY);
			//engine.send(tileChannelName, moveMessage);
			// For supporting multiple EC2 regions... TO CHECK LATER!!!!!
			//engine.send(tileChannelName + "|A", moveMessage2);
			//engine.send(tileChannelName + "|B", moveMessage);
			
			// Increment message identifier for this channel...and obtain the new ID
			int messageId = 0;
			if (messageIndices.containsKey(tileChannelName) == false) {
				messageIndices.put(tileChannelName, new AtomicInteger(messageId));
			} else {
				messageId = messageIndices.get(tileChannelName).incrementAndGet();
			}
			
			
			// Generate the message
			RGameMoveMessage moveMessage = new RGameMoveMessage(this.id,
					avatar.getCurrentX(), avatar.getCurrentY(),
					avatar.getTargetX(), avatar.getTargetY(),
					counter, messageId);
			RGameMoveMessage moveMessage2 = new RGameMoveMessage(this.id,
					avatar.getCurrentX(), avatar.getCurrentY(),
					avatar.getTargetX(), avatar.getTargetY(),
					counter, messageId);
			
			// Send the message to the proper tile-channel
			try {
				engine.send(tileChannelName, moveMessage2);
			} catch (ClosedChannelException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchChannelException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//System.out.println("XXX SENDING MOVE ACTION: nID=" + this.engine.getId() + " | PLAYER=" + this.id + " | TILE=" + this.generateTileChannelName(currentTileX, currentTileY));
			
			sentMoveAction = true;
		}
	}

	private void doSubscription(int targetTileX, int targetTileY, boolean subscribe) {
		doSubscription(targetTileX, targetTileY, subscribe, false);
	}
	
	private void doSubscription(int targetTileX, int targetTileY, boolean subscribe, boolean force) {
		if (isSubscribing() == false)
			return;
		
		if (this.active.get() || force) {
			try {
				// Do it for tiles within range
				int boundX = RConfig.SUBSCRIPTION_RANGE;
				int boundY = RConfig.SUBSCRIPTION_RANGE;
				for (int x = targetTileX-boundX; x <= targetTileX+boundX; x++) {
					for (int y = targetTileY-boundY; y <= targetTileY+boundY; y++) {
						
						// First check for the validity of the tile. Ignore if outside of bounds.
						if ( (x < 0) || (y < 0) || x >= RConfig.getTileCountX() || y >= RConfig.getTileCountY())
							continue;
						
						String channel = generateTileChannelName(x, y);
						
						// SUBSCRIBE TO LOW CHANNEL (IF COST ANALYZER SHOULD BE USED)
						if (CostAnalyzer.shouldEnable()) {
							// (D&mn spent so much time fixing this. DeMorgan law - inversion of boolean exp !!!!)
							if (x == targetTileX && y == targetTileY) {
								// Keep original channel name
							} else {
								channel += "_L";
							}
						}
						
						// Localize channel based on region
						String ec2Region = System.getProperty("ec2.region", "");
						/*
						if (ec2Region.contains("us-")) {
							channel += "|A";
						} else if (ec2Region.contains("ap-")) {
							channel += "|B";
						}
						*/
						
						
						if (subscribe) {
							engine.subscribeChannel(channel, engine.getId());
							//engine.subscribeChannel(channel + "DUMMY", engine.getId());
						} else {
							engine.unsubscribeChannel(channel, engine.getId());
						}
					}	
					
				}
			} catch (NoSuchChannelException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private void broadcastMoveMessage(RPlayerAvatar avatar) {
		// Generate the message
		RGameMoveMessage moveMessage = new RGameMoveMessage(this.id,
				avatar.getCurrentX(), avatar.getCurrentY(),
				avatar.getTargetX(), avatar.getTargetY(),
				counter, 0 /* Maybe it should be changed... */);
		
		// Send the message to the proper tile-channel
		try {
			// Send to all tiles :-)
			for (int i=0; i<RConfig.getTileCountX(); i++) {
				for (int j=0; j<RConfig.getTileCountY(); j++) {
					engine.send(this.generateTileChannelName(i, j), moveMessage);
					//System.out.println("XXX SENDING MOVE ACTION: nID=" + this.engine.getId() + " | PLAYER=" + this.id + " | TILE=" + this.generateTileChannelName(i, j));
				}	
			}
		} catch (ClosedChannelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchChannelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		sentMoveAction = true;
	}	

	public int getId() {
		return id;
	}
	
	public boolean isPublishing() {
		return publishing;
	}

	public void setPublishing(boolean active) {
		this.publishing = active;
	}

	public boolean isSubscribing() {
		return subscribing;
	}

	public void setSubscribing(boolean subscribing) {
		this.subscribing = subscribing;
	}
}
