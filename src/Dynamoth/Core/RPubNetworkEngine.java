package Dynamoth.Core;

import Dynamoth.Client.Client;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.List;

import Dynamoth.Mammoth.NetworkEngine.BaseNetworkEngine;
import Dynamoth.Mammoth.NetworkEngine.NetworkEngineID;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.AlreadyConnectedException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.ChannelExistsException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchChannelException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchClientException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NotConnectedException;
import Dynamoth.Core.Availability.FailureDetector;
import Dynamoth.Core.LoadAnalyzing.AbstractResponseTimeTracker;
import Dynamoth.Core.Manager.DynamothRPubManager;
import Dynamoth.Core.Manager.FullyReplicatedRPubManager;
import Dynamoth.Core.Manager.HashedRPubManager;
import Dynamoth.Core.Manager.LLADynamothRPubManager;
import Dynamoth.Core.Manager.RPubManager;
import Dynamoth.Core.Manager.RPubManagerType;
import Dynamoth.Util.Properties.PropertyManager;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;

public class RPubNetworkEngine extends BaseNetworkEngine implements RPubMessageListener {

	private RPubManager rpubManager = null;
	private boolean infrastructure = false;
	
	/**
	 * Low-level message listener
	 * Should some class wish to receive all incoming 'listener' messages
	 * @author Julien Gascon-Samson
	 */
	public interface LowLevelListener {
		void messageReceived(String channelName, RPubMessage message, int rawMessageSize);
	}
	
	private List<LowLevelListener> lowLevelListeners = new LinkedList<LowLevelListener>();
	
	public RPubNetworkEngine() {
		this(false);		
	}
	
	public RPubNetworkEngine(boolean infrastructure) {
		super();
	
		this.setId(new RPubNetworkID());
		this.infrastructure = infrastructure;
		
		rpubManager = new DynamothRPubManager((RPubNetworkID)(this.getId()), this);
		
	}
	
	public RPubNetworkEngine(RPubManagerType managerType) {
		this(managerType, false);
	}
	
	public RPubNetworkEngine(RPubManagerType managerType, boolean infrastructure) {
		super();
		
		this.setId(new RPubNetworkID());
		this.infrastructure = infrastructure;
		
		rpubManager = createManager(managerType);
	}
	
	private RPubManager createManager(RPubManagerType managerType) {
		switch (managerType) {
		case Hashed:
			return new HashedRPubManager((RPubNetworkID)(this.getId()), this);
		case FullyReplicated:
			return new FullyReplicatedRPubManager((RPubNetworkID)(this.getId()), this);
		case Dynamoth:
			return new DynamothRPubManager((RPubNetworkID)(this.getId()), this);
		case LLADynamoth:
			return new LLADynamothRPubManager((RPubNetworkID)(this.getId()), this);
		}
		return null;
	}
	
	@Override
	public NetworkEngineID connect() throws IOException,
			AlreadyConnectedException {
		
		// Add ourself (engine) to the resp time tracker
		AbstractResponseTimeTracker.getInstance().registerNetworkEngine(this);
		
		// Connect to Redis nodes...
		rpubManager.initialize();
		// Eventually we should trap exceptions...
		
		rpubManager.subscribeToBasicChannels();
		
    	// Sleep --> We have to do that because we want to make sure that we are subscribed to our private
		// unicast channel first before subscribing to plan-push-channel
		try {
			Thread.sleep(500);
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		// Subscribe to plan-push-channel (not the ideal place to do this)
		try {
			subscribeChannel("plan-push-channel", this.getId());
		} catch (NoSuchChannelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Broadcast a connect message to let others know that we connected (in [l]Stern, the Hub does that)
		this.sendAll(new RPubConnectMessage((RPubNetworkID)(this.getId()) ));
		
    	// Sleep --> We have to do that because channel binding at LLA might not yet have been done
		// if we are using the Dynamoth LLA. BAD! WE SHOULD wait from confirmation from the manager first... 
    	try {
			Thread.sleep(500);
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		
		System.out.println("Connected NetworkEngine: " + ((RPubNetworkID) this.getId()).getId().toString());
		
		return this.getId();

	}
	
	

	@Override
	public void disconnect() throws IOException, NotConnectedException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isConnected() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void forceDisconnect(NetworkEngineID id)
			throws NoSuchClientException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void send(NetworkEngineID ClientId, Serializable object)
			throws IOException, ClosedChannelException, NoSuchClientException {
		
		RPubDirectMessage message = new RPubDirectMessage((RPubNetworkID)(this.getId()), object);
		if (infrastructure) {
			message.setFromInfrastructure(infrastructure);
		}
		rpubManager.publishToUnicast(ClientId, message);
	}

	@Override
	public void sendAll(Serializable object) throws IOException {
		
		RPubBroadcastMessage message = new RPubBroadcastMessage((RPubNetworkID)(this.getId()), object);
		if (infrastructure) {
			message.setFromInfrastructure(infrastructure);
		}
		rpubManager.publishToBroadcast( message);
	}

	@Override
	public void send(String channelName, Serializable object)
			throws IOException, ClosedChannelException, NoSuchChannelException {

		//System.out.println("SEND-PUBLISH");
		send(channelName, object, false);
	}
	
	public void send(String channelName, Serializable object, boolean forward)
			throws IOException, ClosedChannelException, NoSuchChannelException {
		
		RPubPublishMessage message = new RPubPublishMessage((RPubNetworkID)(this.getId()), object);
		if (forward) {
			message.setForward(forward);
		}
		if (infrastructure) {
			message.setFromInfrastructure(infrastructure);
		}
		rpubManager.publishToChannel(channelName, message);
	}

	@Override
	public void createChannel(String channelName)
			throws ChannelExistsException, IOException {
		// Create channel on jedis
		rpubManager.createChannel(channelName);

	}

	@Override
	public void subscribeChannel(String channelName, NetworkEngineID clientId)
			throws NoSuchChannelException {
			
		// Send subscription to other end
		rpubManager.subscribeClientToChannel(clientId, channelName);
	}

	@Override
	public void unsubscribeChannel(String channelName, NetworkEngineID clientId)
			throws NoSuchChannelException {

		// Send unsubscription to other end
		rpubManager.unsubscribeClientFromChannel(clientId, channelName);
	}

	public RPubManager getRPubManager() {
		return this.rpubManager;
	}
	
	@Override
	public void messageReceived(String channelName, RPubMessage message, int rawMessageSize) {
		
		// If subscription message
		if (message instanceof RPubSubscriptionMessage) {
			// Do nothing - subscription already handled at a higher level
		} else {
			Serializable payload = ((RPubDataMessage)message).getPayload();
			
			// If connection message, then notify the clients, otherwise, queue it
			if ( payload instanceof RPubConnectionMessage) {
				if (payload instanceof RPubConnectMessage) {
					notifyConnect( ((RPubConnectionMessage)payload).getSourceID() );
					//System.out.println("##### NOTIFY CONNECT #####");
				}
			} else {
				// Queue for transmission
				this.queueMessage( payload );
			}
		}
		
		// Notify low level listeners
		notifyLowLevelListeners(channelName, message, rawMessageSize);
	}

	// Low-level listeners
	
	private void notifyLowLevelListeners(String channelName, RPubMessage message, int rawMessageSize) {
		for (LowLevelListener listener: this.lowLevelListeners) {
			listener.messageReceived(channelName, message, rawMessageSize);
		}
	}
	
	public void registerLowLevelListener(LowLevelListener listener) {
		if (this.lowLevelListeners.contains(listener) == false) {
			this.lowLevelListeners.add(listener);
		}
	}
	
	public void unregisterLowLevelListener(LowLevelListener listener) {
		this.lowLevelListeners.remove(listener);
	}

	public boolean isInfrastructure() {
		return infrastructure;
	}
	
	public static int getInfrastructureServer() {
		Properties props = PropertyManager.getProperties(Client.DEFAULT_CONFIG_FILE);
		return Integer.parseInt(StringUtils.strip(props.getProperty("network.rpub.infrastructure_server")));
	}
}
