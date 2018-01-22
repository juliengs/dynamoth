package Dynamoth.Mammoth.NetworkEngine;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.NoSuchElementException;

import Dynamoth.Mammoth.NetworkEngine.Exceptions.AlreadyConnectedException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.ChannelExistsException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchChannelException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchClientException;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NotConnectedException;

/**
 * This network interface describes the minimum functionality provided by all
 * network engines found in Mammoth. An instance of the proper NetworkEngine
 * can be created using the NetworkEngineFactory.
 * 
 * Messages can be sent to other specific NetworkEngines, published to a 
 * channel (and received by subcribers) or broadcasted to all connected 
 * NetworkEngines.
 * 
 * Arriving messages are stored in a message queue, where they can be 
 * retrieved. The engine itself does not differentiate between published,
 * broadcasted or direct messages. Mammoth already takes care of that.
 * 
 * A network connection listener can also be registered with an engine,
 * allowing the monitoring of other engine connection and disconnection.  
 * 
 * @author adenau
 *
 */
public interface NetworkEngine {
	
	/**
	 * Establish a new connection. Depending on the chosen NetworkEngine, this might 
	 * establish a direct connection with another NetworkEngine, or simply connect 
	 * to an intermediate dispatcher.  
	 * 
	 * On some engine, this function will block for a short while to receive 
	 * initialization information. This information can include a unique ID uses to 
	 * identify this engine, or a list of logic channels available that this
	 * engine can subscribe to.
	 */
	public abstract NetworkEngineID connect() throws IOException, AlreadyConnectedException;

	/**
	 * Returns this unique identifier of this engine.
	 * 
	 * @return
	 */
	public NetworkEngineID getId();
	
	/**
	 * Closes the network connection.
	 */
	public abstract void disconnect() throws IOException, NotConnectedException;

	/**
	 * Tells if this engine is currently connected.
	 * 
	 * @return <code> true </code> if the client is connect to a server.
	 */
	public abstract boolean isConnected();
	
	/**
	 * Forces an engine specified by the id to disconnect from the system. Note
	 * that this functionality is not implemented in many of the network engines
	 * found in Mammoth. 
	 * 
	 * @param id Unique ID of the engine to disconnect.
	 */
	public abstract void forceDisconnect(NetworkEngineID id)
			throws NoSuchClientException, IOException;


	/**
	 * Gets a message from the message from the message queue.
	 * 
	 * @return The first message.
	 */
	public abstract Serializable nextMessage()
			throws NoSuchElementException;

	/**
	 * Tells if there are any messages waiting in the message queue.
	 * 
	 * @return <code> true </code> if there are messages.
	 */
	public abstract boolean hasMessage();


	/**
	 * Blocks when there are no messages in the queue. Otherwise returns
	 * when there is at least one message in the message queue.
	 */
	public abstract void waitForMessage() throws InterruptedException;

	/**
	 * Blocks when there are no messages in the queue, until the 
	 * timeout expires. Otherwise returns when there is at least one 
	 * message in the message queue.
	 * 
	 * @param timeout if no message arrive will still return after this time
	 */
	public abstract void waitForMessage(long timeout)
			throws InterruptedException;
	
	/**
	 * Get messages from the message queue or wait till timeout.
	 * 
	 * @param timeout
	 * @return
	 */
	public abstract Serializable getMessageOrWait(int timeout);
	
	/**
	 * Sends a Serializable object to a specific engine given by an ID.
	 * 
	 * @param ClientId
	 * @param object
	 */
	public void send(NetworkEngineID ClientId, Serializable object) throws IOException,
    	ClosedChannelException, NoSuchClientException ;
	
	/**
	 * Sends a Serializable to all connected network engines.
	 * 
	 * @param object
	 */
	public void sendAll(Serializable object) throws IOException;
	
	/**
	 * Sends a Serializable to all NetworkEngines with the appropriate
	 * subscription to a channel. If an engine has no subscription
	 * to the channel, then it does not receive the message.
	 * 
	 * @param channelName
	 * @param subscription
	 * @param object
	 */
	public abstract void send(String channelName, Serializable object) 
		throws IOException, ClosedChannelException, NoSuchChannelException ;
		
	/**
	 * Creates a new logic channel. Note that this not required by all engines.
	 * 
	 * @param channelName
	 */
	public abstract void createChannel(String channelName)
			throws ChannelExistsException, IOException;
	
	/**
	 * Subscribe the given network engine to a particular channel.
	 * 
	 * @param channelName
	 * @param clientId
	 * @throws NoSuchChannelException
	 */
	public void subscribeChannel(String channelName, NetworkEngineID clientId) throws NoSuchChannelException;
	
	/**
	 * Unsubscribe the given network engine to a particular channel.
	 * 
	 * @param channelName
	 * @param clientId
	 * @throws NoSuchChannelException
	 */
	public void unsubscribeChannel(String channelName, NetworkEngineID clientId) throws NoSuchChannelException;
	
	/**
	 * Subscribe the given network engine to a set of channels.
	 * 
	 * @param channels
	 * @param clientId
	 * @throws NoSuchChannelException
	 */
	public void subscribeChannels(Collection<String> channels, NetworkEngineID clientId) throws NoSuchChannelException;
	
	/**
	 * Unsubscribe the given network engine to a set of channels.
	 * 
	 * @param channels
	 * @param clientId
	 * @throws NoSuchChannelException
	 */
	public void unsubscribeChannels(Collection<String> channels, NetworkEngineID clientId) throws NoSuchChannelException;
	
	/**
	 * Registers a new connection listener.
	 * @param listener
	 */
	public void registerListener(NetworkEngineListener listener);
	
	/**
	 * Unregisters a connection listener.
	 * @param listener
	 */
	public void removeListener(NetworkEngineListener listener);
	
	/**
	 * Unregisters all existing connection listener.
	 */
	public void clearListeners();
		
}
