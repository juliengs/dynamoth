package Dynamoth.Mammoth.NetworkEngine;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import Dynamoth.Core.RPubNetworkEngine;
import Dynamoth.Util.Properties.PropertyManager;

/**
 * The NetworkEngineFactory provides a unified interface to create
 * various types of NetworkEngine, depending on the configuration
 * found in the properties files.
 * 
 * @version Dec 6, 2007
 * @author Dominik Zindel
 *
 */
public class NetworkEngineFactory {
	
	private static final String DEFAULT_CONFIG_FILE = "mammoth.properties";

	/**
	 * Returns a string indicating which type of engine should be used.
	 * @return
	 */
	public static String getEngineType(){
		Properties props = PropertyManager.getProperties(DEFAULT_CONFIG_FILE);
		return StringUtils.strip(props.getProperty("network.engine").toLowerCase());
	}
		
	/**
	 * Creates a network engine.
	 * 
	 * @return
	 */
	public static NetworkEngine getNetworkEngine(){

		String engineType = getEngineType();
		if(engineType.equals("default")){
			return new RPubNetworkEngine();
		}
		else if (engineType.equals("rpub")){
			return new RPubNetworkEngine();
		}
		else{
			throw new RuntimeException("no valid property for network engine");
		}
	}
	
}
