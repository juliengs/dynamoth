package Dynamoth.Core.Client;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;

import Dynamoth.Client.Client;
import Dynamoth.Core.Game.Messages.RGameMoveMessage;
import Dynamoth.Core.RPubMessage;
import Dynamoth.Core.RPubMessageListener;
import Dynamoth.Core.RPubNetworkID;
import Dynamoth.Core.RPubPublishMessage;
import Dynamoth.Core.Util.MultiPubKingDataset;
import Dynamoth.Core.Util.RawKingDataset;
import Dynamoth.Util.Properties.PropertyManager;

public class RPubMessageDelayer implements RPubMessageListener {

	private class DelayerMessage {
		private String channelName;
		private RPubMessage message;
		private int rawMessageSize;
		private long deliveryTime;
		
		public DelayerMessage(String channelName, RPubMessage message, int rawMessageSize, long deliveryTime) {
			super();
			this.channelName = channelName;
			this.message = message;
			this.rawMessageSize = rawMessageSize;
			this.deliveryTime = deliveryTime;
		}

		public String getChannelName() {
			return channelName;
		}

		public RPubMessage getMessage() {
			return message;
		}

		public int getRawMessageSize() {
			return rawMessageSize;
		}

		public long getDeliveryTime() {
			return deliveryTime;
		}
		
	}
	
	/**
	 * Original RPub message listener
	 */
	private RPubMessageListener listener;
	
	/**
	 * Delayed messages
	 */
	private List<DelayerMessage> delayedMessages = new LinkedList<DelayerMessage>();
	
	private Object delayedMessagesLock = new Object();
	
	private float localDelay;
	
	private RPubNetworkID networkId;
	
	private int stats_enqueued = 0;
	private int stats_delivered = 0;
	
	/**
	 * RPubMessageDelayer processing thread
	 */
	private Thread processingThread = new Thread(new Runnable() {
		
		/**
		 * Continuously iterate through the list of delayed messages until
		 * a message's delivery time > current time and then sleep
		 */
		@Override
		public void run() {
		
			while (true) {
				
				synchronized (delayedMessagesLock) {
					ListIterator<DelayerMessage> iterator = delayedMessages.listIterator();
					while (iterator.hasNext()) {
						DelayerMessage delayer = iterator.next();
						long currentTime = System.nanoTime();
						if (delayer.deliveryTime <= currentTime) {
							listener.messageReceived(delayer.channelName, delayer.message, delayer.rawMessageSize);
							iterator.remove();
							stats_delivered++;
							//System.out.println("D/Enqueued=" + stats_enqueued + " | Delivered=" + stats_delivered);						
						}
					}
				}
				
				// Sleep
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
			}
			
		}
		
	}, "RPubMessageDelayer");
	
	public RPubMessageDelayer(RPubMessageListener listener, float localDelay, RPubNetworkID networkId) {
		this.listener = listener;
		this.localDelay = localDelay;
		this.networkId = networkId;
		
		// Start the processing thread
		processingThread.start();
	}
	
	private long getDelayRawKing(String sourceDomain, String destinationDomain, String rpubDomain, RPubMessage message) {
		int kingSampleCount = 0;
		
		// Case 1: SOURCE and DESTINATION NOT IN same domain as RPUB
		if (sourceDomain.equals(rpubDomain) == false && destinationDomain.equals(rpubDomain) == false) {
			
			// No king
			kingSampleCount = 0;
			
		}
		// Case 2: SOURCE IN same domain as RPUB, DESTINATION NOT IN same domain
		else if (sourceDomain.equals(rpubDomain) && destinationDomain.equals(rpubDomain) == false) {
			
			// If infrastructure then 0 king, otherwise 1 king
			if (message.isFromInfrastructure()) {
				kingSampleCount = 0;
			} else {
				kingSampleCount = 1;
			}
			
		}
		// Case 3: SOURCE NOT IN same domain as RPUB, DESTINATION IN same domain
		else if (sourceDomain.equals(rpubDomain) == false && destinationDomain.equals(rpubDomain)) {
			
			// Cannot be infrastructre (well in theory could be, but our framework cannot tell us...)
			kingSampleCount = 1;
			
		}
		// Case 4: SOURCE and DESTINATION IN same domain as RPUB
		else if (sourceDomain.equals(rpubDomain) && destinationDomain.equals(rpubDomain)) {
			
			// If infrastructure then 1 king, otherwise 2 king
			if (message.isFromInfrastructure()) {
				kingSampleCount = 1;
			} else {
				kingSampleCount = 2;
			}
			
		}
		
		// Generate a delay using king
		RawKingDataset king = RawKingDataset.instance();
		
		// Divide all king samples by 2 because king is RTT
		
		// Generate "n" king samples
		float kingValue = 0.0f;
		
		for (int i=0; i<kingSampleCount; i++) {
			kingValue += king.nextFloat() / 2.0f;
			// Remove local delay
			kingValue -= this.localDelay;
		}
		
		long delay = Math.round(kingValue * 1000000);
		return delay;
	}
	
	private long getDelayMultiPub(String sourceDomain, String destinationDomain, String rpubDomain, RPubMessage message) {
		
		long latency = MultiPubKingDataset.getInstance().getFullLatencySample(message.getSourceID().getId(), sourceDomain, rpubDomain, networkId.getId(), destinationDomain);
		
		// Approximation
		latency -= 2*this.localDelay;

		return latency * 1000000;
	}

	@Override
	public void messageReceived(String channelName, RPubMessage message, int rawMessageSize) {

		String sourceDomain = message.getSourceDomain();
		String destinationDomain = System.getProperty("ec2.region", "");
		String rpubDomain = message.getRpubServerDomain();
		
		long delay = 0;
		if (sourceDomain.equals("") || rpubDomain.equals("") || destinationDomain.equals("")) {
			delay = 0;
		} else {	
			delay = /*getDelayRawKing*/ getDelayMultiPub(sourceDomain, destinationDomain, rpubDomain, message);
			//System.out.println(sourceDomain+"->"+rpubDomain+"->"+destinationDomain + " DELAY=" + (delay/1000000));
		}
		
		long deliveryTime = System.nanoTime() + delay;
		
		// Create our delayer message
		DelayerMessage delayer = new DelayerMessage(channelName, message, rawMessageSize, deliveryTime);
		
		// Place it at an appropriate position in our queued list of delayed messages
		synchronized (delayedMessagesLock) {
			if (delayedMessages.size() == 0) {
				delayedMessages.add(delayer);
			} else {
				ListIterator<DelayerMessage> iterator = delayedMessages.listIterator();
				boolean inserted=false;
				while (iterator.hasNext()) {
					DelayerMessage dm = iterator.next();
					if (dm.getDeliveryTime() > deliveryTime) {
						// Insert BEFORE (so call previous before inserting)
						iterator.previous();
						iterator.add(delayer);
						inserted=true;
						break; // and then we just avoided an infinite loop :-)
					}
				}
				if (inserted==false) {
					// Insert at the end
					delayedMessages.add(delayer);
				}
			}
			//System.out.println("Adding msg delay=" + kingValue + " | in queue: " + delayedMessages.size());
			stats_enqueued++;
			//System.out.println("E/Enqueued=" + stats_enqueued + " | Delivered=" + stats_delivered);
		}
	}
	
	public static boolean shouldEnable() {
		return Boolean.parseBoolean(getProperties().getProperty("network.rpub.delayer.enable", "False"));
	}
	
	public static float localDelay() {
		return Float.parseFloat(getProperties().getProperty("network.rpub.delayer.localdelay", "5"));
	}
	
	private static Properties getProperties() {
		return PropertyManager.getProperties(Client.DEFAULT_CONFIG_FILE);
	}

}
