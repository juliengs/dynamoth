package Dynamoth.Mammoth.NetworkEngine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;

/**
 * The SubscriptionList class is used to manage subscriptions and 
 * unsubscriptions for a publisher/subscriber system. Channels are defined
 * as strings, and subscribers can be any type of objects.
 * 
 * Since all network engines in Mammoth provide pub/sub functionalities,
 * this is a very nice class to manage the pub/sub system.
 * 
 * @author adenau
 *
 * @param <E> Type of subscribers
 */
public class SubscriptionList<E> {
	
	// Stores the subscriptions
	private Hashtable<String,ArrayList<E>> subscriptionList;

	/**
	 * Creates an empty subscription list.
	 */
	public SubscriptionList() {
	
		this.subscriptionList = new Hashtable<String, ArrayList<E>>();
	}
	
	/**
	 * Returns a boolean indicating if the supplied channel already exists.
	 * @param channel
	 * @return
	 */
	public boolean channelExists(String channel) {
		
		return this.subscriptionList.containsKey(channel);
	}
	
	/**
	 * Subscribes the specified member to the specified channel.
	 *  
	 * @param channel
	 * @param member
	 */
	public void subscribe(String channel, E member) {
		
		this.getChannel(channel).add(member);
	}
	
	/**
	 * Unsubscribes the specified member from the specified channel.
	 * 
	 * @param channel
	 * @param member
	 */
	public void unsubscribe(String channel, E member) {
		
		this.getChannel(channel).remove(member);
	}
	
	/**
	 * Returns a boolean indicating if the specified member is subscribed to 
	 * the specified channel.
	 * 
	 * @param channel
	 * @param member
	 * @return
	 */
	public boolean isSubscribed(String channel, E member) {
		
		ArrayList<E> channelContent = this.getChannel(channel);
		
		if (channelContent == null) return false;
		
		return channelContent.contains(member);
	}
	
	/**
	 * Returns a list of members subscribed to a channel.
	 * 
	 * @param channel
	 * @return
	 */
	public Iterator<E> getSubscribers(String channel) {
		
		return this.getChannel(channel).iterator();
	}
	
	/**
	 * Unsubscribes a member from all channels.
	 * 
	 * @param subscriber
	 */
	public void removeSubscriber(E subscriber) {
		
		for(ArrayList<E> list: this.subscriptionList.values()) {
			list.remove(subscriber);
		}		
	}
	
	public int getSize() {
		return this.subscriptionList.size();
	}
	
	/**
	 * Returns of all subscribes to a given channel.
	 * 
	 * @param channel
	 * @return
	 */
	private ArrayList<E> getChannel(String channel) {
		
		 ArrayList<E> subscriptions = this.subscriptionList.get(channel);
		 
		 if(subscriptions == null) {
			 subscriptions = new ArrayList<E>();
			 this.subscriptionList.put(channel, subscriptions);
		 }
		 
		 return subscriptions;
	}
	
	/**
	 * Returns a list of all existing channels.
	 */
	public Collection<String> getAllChannels() {
		
		return this.subscriptionList.keySet();
	}
}
