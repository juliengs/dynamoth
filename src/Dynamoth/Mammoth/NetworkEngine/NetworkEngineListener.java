package Dynamoth.Mammoth.NetworkEngine;

/**
 * The network connection listener interface for NetworkEngine.
 * <p>
 * The NetworkEngineServer invokes the <code> onConnect </code> and
 * <code> onDisconnect </code> methods when it detects a new engine 
 * has connected or disconnected respectively.
 * 
 * @author Alfred
 */
public interface NetworkEngineListener {
	
    /**
     * Method that is invoked when a new engine connects.
     * 
     * @param clientId
     */
    public void onConnect(NetworkEngineID clientId);

    /**
     * Method that is invoked when an engine disconnects.
     * 
     * @param clientId
     */
    public void onDisconnect(NetworkEngineID clientId);
    
}
