/*
 * License...
 */
package Dynamoth.Core.ExternalClient;

import Dynamoth.Core.RPubMessage;
import Dynamoth.Core.RPubNetworkEngine;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.RPubPublishMessage;
import Dynamoth.Core.RPubRawMessage;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.AlreadyConnectedException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchChannelException;
import Dynamoth.Mammoth.NetworkEngine.NetworkEngineID;
import Dynamoth.Mammoth.NetworkEngine.NetworkEngineListener;
import Dynamoth.Util.Message.Reactor;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Supports the handling of external clients.
 * 
 * @author Julien Gascon-Samson
 */
public class ExternalClient {
	
	private RPubNetworkEngine engine;
	private String externalClientChannel = "externalclient";
	
	public ExternalClient() {
	
		
		
	}
	
	public void initialize() {
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
		
		// Subscribe to raw messages
		engine.registerLowLevelListener(new RPubNetworkEngine.LowLevelListener() {
			@Override
			public void messageReceived(String channelName, RPubMessage message, int rawMessageSize) {
				// Make sure this is a raw message and that the channel is appropriate
				if (channelName.equals(externalClientChannel) && message instanceof RPubRawMessage) {
					RPubRawMessage raw = (RPubRawMessage)message;
					
					// Parse the raw messages
					parseAndDispatch(raw.getPayload());
				}
			}
		});
		
		// Create and register a reactor for RPub
		//reactor = new Reactor("RClient" + this.id + "Reactor", engine);
	}
	
	public void parseAndDispatch(String payload) {
		// Split by linebreaks
		String[] lines = payload.split(payload, 4);
		// TODO: check for arg count
		// 1st arg: client id
		int clientId = Integer.parseInt(lines[0]);
		// 2nd arg: operation: connect, disconnect, publish, subscribe, unsubscribe
		String operation = lines[1];
		// Start dispatching connect & disconnect 
		if (operation.equals("connect")) {
			handleConnect(clientId);
		} else if (operation.equals("disconnect")) {
			handleDisconnect(clientId);
		} else {
			// Extract topic name
			String topic = lines[2];
			
			if (operation.equals("publish")) {
				// Extract msg
				String msg = lines[3];
				handlePublish(clientId, topic, msg);
			} else if (operation.equals("subscribe")) {
				handleSubscribe(clientId, topic);
			} else if (operation.equals("unsubscribe")) {
				handleUnsubscribe(clientId, topic);
			}
		}
	}
	
	public void handleConnect(int clientId) {
		
	}
	
	public void handleDisconnect(int clientId) {
	
	}
	
	public void handlePublish(int clientId, String topic, String msg) {
		RPubPublishMessage publish = new RPubPublishMessage(new RPubNetworkID(clientId), msg);
		try {
			engine.send(topic, publish);
		} catch (IOException ex) {
			Logger.getLogger(ExternalClient.class.getName()).log(Level.SEVERE, null, ex);
		} catch (NoSuchChannelException ex) {
			Logger.getLogger(ExternalClient.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	public void handleSubscribe(int clientId, String topic) {
		try {
			engine.subscribeChannel(topic, new RPubNetworkID(clientId));
		} catch (NoSuchChannelException ex) {
			Logger.getLogger(ExternalClient.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	public void handleUnsubscribe(int clientId, String topic) {
		try {
			engine.unsubscribeChannel(topic, new RPubNetworkID(clientId));
		} catch (NoSuchChannelException ex) {
			Logger.getLogger(ExternalClient.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	public voide handlePublication(int clientId, String topic, String msg) {
		
	}
}
