package Dynamoth.Mammoth.NetworkEngine;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchChannelException;

/**
 * The BaseNetworkEngine class provides some of the functionalities found in
 * all NetworkEngines. As such, most new network engine should also extend
 * this abstract class.
 * 
 * @author adenau
 *
 */
public abstract class BaseNetworkEngine implements NetworkEngine {

	private LinkedList<Serializable> messages;	// Message queue
	
	// Lock object used to sync queues and notify of message arrival.
	private Object lock;						
	
	// Network connection listener
	private LinkedList<NetworkEngineListener> listeners;
	
	private NetworkEngineID id;	  // Network unique ID for this engine

	/**
	 * Initialization of common attributes.
	 */
	public BaseNetworkEngine() {

		this.lock = new Object();

		this.messages = new LinkedList<Serializable>();
		this.listeners = new LinkedList<NetworkEngineListener>();
	}

	/**
	 * Add a message to the message queue. This should only be called by the
	 * NetworkEngine implementation.
	 * 
	 * @param message
	 */
	protected void queueMessage(Serializable message) {
		
		synchronized (this.messages) {
			this.messages.addLast(message);
			this.messages.notify();
		}

		synchronized (this.lock) {
			this.lock.notify();
		}
	}

	public boolean hasMessage() {

		synchronized (this.messages) {
			return !this.messages.isEmpty();
		}
	}

	public Serializable nextMessage() throws NoSuchElementException {

		synchronized (this.messages) {
			return this.messages.removeFirst();
		}
	}

	public void waitForMessage() throws InterruptedException {

		synchronized (this.messages) {
			if(this.messages.isEmpty()) {
				try {
					this.messages.wait();
				} catch (InterruptedException e) {
					throw e;
				}
			}
		}
	}

	public void waitForMessage(long timeout) throws InterruptedException {

		synchronized (this.messages) {
			if(this.messages.isEmpty()) {
				try {
					this.messages.wait(timeout);
				} catch (InterruptedException e) {
					throw e;
				}
			}
		}		
	}

	public Serializable getMessageOrWait(int timeout) {

		if (this.hasMessage()) {
			return this.nextMessage();
		}

		synchronized (this.lock) {
			try {
				this.lock.wait(timeout);
			} catch (InterruptedException e) {
				// Dont care
			}	
		}
		

		if (this.hasMessage()) {
			return this.nextMessage();
		}

		return null;
	}

	public void subscribeChannels(Collection<String> channels,
			NetworkEngineID clientId) throws NoSuchChannelException {

		for(String channel: channels) {
			this.subscribeChannel(channel, clientId);
		}
	}

	public void unsubscribeChannels(Collection<String> channels,
			NetworkEngineID clientId) throws NoSuchChannelException {

		for(String channel: channels) {
			this.unsubscribeChannel(channel, clientId);
		}
	}
	
	public void registerListener(NetworkEngineListener listener) {
		synchronized (this.listeners) {
			this.listeners.add(listener);
		}
	}
	
	public void removeListener(NetworkEngineListener listener) {
		synchronized (this.listeners) {
			this.listeners.remove(listener);
		}
	}
	
	public void clearListeners() {
		synchronized (this.listeners) {
			this.listeners.clear();
		}
	}

	/**
	 * Notify all registered listeners of new engine.
	 * 
	 * @param networkId
	 */
	public void notifyConnect(NetworkEngineID networkId) {
		
		// This should rarely happen
		if (this.id == null) return;
		
		if (!this.id.equals(networkId)) {
			for(NetworkEngineListener listener: this.listeners) {
				listener.onConnect(networkId);
			}
		}
	}
	
	/**
	 * Notify all registered listeners of disconnection for another engine.
	 * @param networkId
	 */
	public void notifyDisconnect(NetworkEngineID networkId) {
		
		for(NetworkEngineListener listener: this.listeners) {
			listener.onDisconnect(networkId);
		}		
	}

	public NetworkEngineID getId() {
		return id;
	}

	/**
	 * Set the unique ID of this engine.
	 * 
	 * @param id
	 */
	protected void setId(NetworkEngineID id) {
		this.id = id;
	}

}
