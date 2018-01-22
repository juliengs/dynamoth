package Dynamoth.Core.Util;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import Dynamoth.Client.Client;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Util.Properties.PropertyManager;

public class RPubHostInfo {

	private RPubClientId clientId;
	
	private String hostName;
	
	private String domain;
	
	private int port;
	
	private long maxByteIn;
	private long maxByteOut;
	
	/**
	 * Load RPubHostInfo by decoding the props file
	 * @param hostId Host ID as defined in the props file
	 */
	public RPubHostInfo(int hostId) {
		
		// Check through all raw servers and pooled servers
		Properties props = PropertyManager.getProperties(Client.DEFAULT_CONFIG_FILE);
		String rawServers = StringUtils.strip(props.getProperty("network.rpub.dynamoth.initial_servers"));
		String rawPoolServers = StringUtils.strip(props.getProperty("network.rpub.dynamoth.pool_servers"));
		String rawAllServers = rawServers + ";" + rawPoolServers;
		
		for (String server: rawAllServers.split(";")) {
			// Parse ID
			int id = RPubUtil.parseRPubClientId(server);
			
			// If id matches ID in constructor
			if (id == hostId) {
				
				// Build using this string
				buildFromHostString(server);
				break;
				
			}
		}
		
	}
	
	public RPubHostInfo(String hostString) {
		buildFromHostString(hostString);
	}
	
	public RPubHostInfo(RPubClientId clientId, String hostName, int port, long maxByteIn, long maxByteOut) {
		this.clientId   = clientId;
		this.hostName   = hostName;
		this.port       = port;
		this.maxByteIn  = maxByteIn;
		this.maxByteOut = maxByteOut;
	}
	
	private void buildFromHostString(String hostString) {
		this.clientId = new RPubClientId(RPubUtil.parseRPubClientId(hostString));
		this.hostName = RPubUtil.parseRPubHostName(hostString);
		this.domain = RPubUtil.parseRPubHostDomain(hostString);
		this.port = RPubUtil.parseRPubHostPort(hostString);
		this.maxByteIn = RPubUtil.kiloBytesToBytes( RPubUtil.parseRPubHostKByteIn(hostString) );
		this.maxByteOut = RPubUtil.kiloBytesToBytes( RPubUtil.parseRPubHostKByteOut(hostString) );		
	}

	public RPubClientId getClientId() {
		return clientId;
	}

	public String getHostName() {
		return hostName;
	}

	public String getDomain() {
		return domain;
	}

	public int getPort() {
		return port;
	}

	public long getMaxByteIn() {
		return maxByteIn;
	}

	public long getMaxByteOut() {
		return maxByteOut;
	}
}
