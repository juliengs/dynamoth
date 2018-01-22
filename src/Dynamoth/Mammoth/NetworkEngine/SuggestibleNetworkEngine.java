package Dynamoth.Mammoth.NetworkEngine;

public interface SuggestibleNetworkEngine extends NetworkEngine {

//	public void suggestConnection(NetworkEngineID id);
//	public void suggestDisconnection(NetworkEngineID id);
	
	public void suggestConnection(NetworkEngineID targetId, NetworkEngineID nodeId);
	public void suggestDisconnection(NetworkEngineID targetId, NetworkEngineID nodeId);
	
	public boolean isConnected(NetworkEngineID id);
	
}
