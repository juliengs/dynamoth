package Dynamoth.Util.Message;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import Dynamoth.Mammoth.NetworkEngine.NetworkEngine;

/**
 * Interface of a reactor
 * @author <a href="mailto:jboula2@cs.mcgill.ca">Jean-Sebastien Boulanger</a>
 *
 */
public class Reactor implements Runnable {
	
	private Map<Class<?>, List<Handler>> handlers;

	protected final static int TIMEOUT = 5000;	

	private NetworkEngine ne;

	private Thread reactorThread;

	private String name;
	
	public Reactor(String name, NetworkEngine ne) {
		
		this.name = name;
		this.ne = ne;

		this.handlers = new Hashtable<Class<?>, List<Handler>>();

		reactorThread = new Thread(this);
		reactorThread.setName("Reactor Thread - " + this.name );
		reactorThread.setDaemon(true);
		reactorThread.start();

	}

	public void run() {
		
		while(true) {
			this.slice();
		}
	}

	/**
	 * Should retrieve the messages from
	 * a source and call the handle(Message) method on them.
	 */
	public void slice() {

		if(!ne.hasMessage()){
				try{
					ne.waitForMessage(TIMEOUT);
				}
				catch(InterruptedException e){}
			}
//		} else {
//			
//			//Thread.yield();
//		}

		while(ne.hasMessage()){
			Serializable msg = ne.nextMessage();
			
			try {
				//System.out.println("message " + msg);
				handle((Message)msg);
				
			} catch(Exception e) {				
				e.printStackTrace();
			}
			
		}
		

	}

	public void stop() {
		
//		synchronized (this.reactors) {
//			this.reactors.remove(this);
//		}
	}

	/**
	 * Register a <code>Handler</code> for a particular message type.
	 * Multiple handlers can be registered for the same message type.
	 * They will be executed in the order they were registered.
	 * @param messageType
	 * @param handler
	 */
	public void register(Class<?> messageType, Handler handler) {
		assert handler != null : "cannot register a null handler";

		List<Handler> hand = this.handlers.get(messageType);		
		if(hand == null) {
			hand = new Vector<Handler>();
			this.handlers.put(messageType, hand);
		}
		hand.add(handler);	
	}

	/**
	 * Unregister a <code>Handler</code> from a particular message type.
	 * The method will remove all handlers for the given message type
	 * that match the equals of the handler.
	 * @param messageType
	 * @param handler
	 */
	public void unregister(Class<?> messageType, Handler handler) {
		if(handler == null) return;
		List<Handler> hand = this.handlers.get(messageType);
		if(hand != null) {
			synchronized(hand) {
				for(Iterator<Handler> i = hand.iterator(); i.hasNext();) {
					Object iHandler = i.next();
					if(iHandler.equals(handler)) {
						i.remove();
					}
				}
			}
		}
	}

	/**
	 * Handle the message with the registered handlers.
	 * @param message
	 */
	protected void handle(Message message) {
		Class<?> clazz = message.getClass();

		//log.debug("[REactor] handle " + message + " in Thread: " + Thread.currentThread().getName());

		while(clazz != null && !clazz.equals(Object.class)) {
			List<Handler> hand = this.handlers.get(clazz);


			if(hand != null) {

				//log.debug("[ReAcToR] GoT cLaZz:" + clazz);

				// TODO: JSB20060615 - if multiple threads are doing the handling of messsages, this could potentially become a bottleneck eventually!
				synchronized(hand) {
					for(Handler h : hand) {
						//log.debug("[ReAcToR:" + this + ":" + h + "] handling message (" + h + "): " + message );
						h.handle(message);
					}
				}
				return;
			}
			else {
				clazz = clazz.getSuperclass();
				//log.debug("[ReAcToR] GoT sUpEr ClAzZ: " + clazz);
			}
		}

		//log.debug("[ReAcToR] end handling");
	}


}
