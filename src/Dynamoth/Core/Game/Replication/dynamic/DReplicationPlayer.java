package Dynamoth.Core.Game.Replication.dynamic;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ClosedChannelException;
import java.util.HashSet;
import java.util.Set;

import Dynamoth.Mammoth.NetworkEngine.Exceptions.AlreadyConnectedException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchChannelException;
import Dynamoth.Core.RPubNetworkEngine;
import Dynamoth.Core.Manager.DynamothRPubManager;
import Dynamoth.Core.Manager.Plan.Plan;
import Dynamoth.Util.Message.Handler;
import Dynamoth.Util.Message.Message;
import Dynamoth.Util.Message.Reactor;


public class DReplicationPlayer {

	private RPubNetworkEngine engine;
	private Reactor reactor;
	private DynamothRPubManager manager;
	private Set<String> receivedMessages = new HashSet<String>();
	private int id;
	
	public DReplicationPlayer(int id) {
		this.id = id;
	}

	public void subscribe(String channelName) {
		try {
			engine.subscribeChannel(channelName, engine.getId());
		} catch (NoSuchChannelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	reactor = new Reactor("RClient Reactor", engine);
    	
    	// Register for incoming RGameMoveMessages
    	reactor.register(DReplicationMessage.class, new Handler() {
			
			@Override
			public void handle(Message msg) {
				handleReplicationMessage((DReplicationMessage) msg);
			}

		});

	}
	
	private void handleReplicationMessage(DReplicationMessage msg) {
		boolean isDup = false;
		synchronized(receivedMessages) {
			isDup = receivedMessages.contains(msg.getMessageId());
			if(!isDup)
				receivedMessages.add(msg.getMessageId());
		}
		if( DReplicationClient.GLOBAL_CLIENT_ID == msg.getId() && !isDup) {
			long currentTime = System.nanoTime();
			long delay = currentTime - msg.getTimeStamp();
			Plan currentPlan = manager.getCurrentPlan();
			System.out.println(currentTime + "\t" + delay + "\t" + currentPlan.getMapping("replication-test-dynamic").getShards().length + "\t" + currentPlan.getPlanId().getId());
		}
	}

	public void publish(String channelName, Serializable serializable) {
		System.out.println("publish");
		try {
			engine.send(channelName, serializable);
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
	}

	public void unsubscribe(String channelName) {
		try {
			engine.unsubscribeChannel(channelName, engine.getId());
		} catch (NoSuchChannelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	public int getId() {
		return this.id;
	}

	public void connect() {
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
    	manager = (DynamothRPubManager) engine.getRPubManager();
	}

}
