package Dynamoth.Core.Game.Replication.test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ClosedChannelException;

import Dynamoth.Mammoth.NetworkEngine.NetworkEngineID;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.NoSuchClientException;
import Dynamoth.Core.RPubNetworkEngine;
import Dynamoth.Core.Manager.DynamothRPubManager;

public class RPubNetworkEngineStub extends RPubNetworkEngine {

	private DynamothRPubManager manager;

	public RPubNetworkEngineStub(DynamothRPubManager manager) {
		this.manager = manager;
	}
	
	public DynamothRPubManager getRPubManager() {
		return this.manager;
	}
	
	public void send(NetworkEngineID ClientId, Serializable object)
			throws IOException, ClosedChannelException, NoSuchClientException {
		
	}


}
