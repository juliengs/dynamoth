package Dynamoth.Core.Game.Replication;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ClosedChannelException;

import Dynamoth.Mammoth.NetworkEngine.Exceptions.AlreadyConnectedException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchChannelException;
import Dynamoth.Core.RPubNetworkEngine;
import Dynamoth.Util.Message.Handler;
import Dynamoth.Util.Message.Message;
import Dynamoth.Util.Message.Reactor;


public class RReplicationPlayer {

	private long COOLDOWN_PERIOD = 60*1000;

	private long launchTime;
	private boolean inCooldown;
	private RPubNetworkEngine engine;
	private Reactor reactor;

	public void launch() {
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
    	launchTime = System.currentTimeMillis();
    	inCooldown = true;
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
    	reactor.register(RReplicationMessage.class, new Handler() {
			
			@Override
			public void handle(Message msg) {
				handleReplicationMessage((RReplicationMessage) msg);
			}

		});

	}
	
	private void handleReplicationMessage(RReplicationMessage msg) {
		if( RReplicationClient.GLOBAL_CLIENT_ID == msg.getId()) {
			long delay = System.nanoTime() - msg.getTimeStamp();
			if(!inCooldown) {
				System.out.println(delay);
			} else if((System.currentTimeMillis() - this.launchTime) > COOLDOWN_PERIOD) {
				inCooldown = false;
			}
		}
	}

	public void publish(String channelName, Serializable serializable) {
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

}
