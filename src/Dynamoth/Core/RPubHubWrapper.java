package Dynamoth.Core;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import Dynamoth.Client.Client;
import Dynamoth.Mammoth.NetworkEngine.Exceptions.AlreadyConnectedException;
import Dynamoth.Core.Client.RPubClientId;
import Dynamoth.Core.ExternalClient.ExternalClient;
import Dynamoth.Core.LoadAnalyzing.LocalLoadAnalyzer;
import Dynamoth.Core.Manager.DynamothRPubManager;
import Dynamoth.Core.Manager.LLADynamothRPubManager;
import Dynamoth.Core.Manager.RPubManagerType;
import Dynamoth.Core.Util.RPubHostInfo;
import Dynamoth.Util.Message.Reactor;
import Dynamoth.Util.Properties.PropertyManager;

public class RPubHubWrapper {

	private String redisPath_ = "";
	private Process process_ = null;
	private String hubId_;
	
	// Network engine - only connects to host 0
	private RPubNetworkEngine engine = null;
	private Reactor reactor = null;
	// Network engine - LLA Dynamoth - for LLA analysis only (host set as cmdline arg)
	private RPubNetworkEngine llaEngine = null;
	
	// HostInfo for this RPubHubWrapper
	private RPubHostInfo hostInfo = null;
	
	// HostInfo for the Default RPub Server [0] / so that we can bind locally
	private RPubHostInfo defaultHostInfo = null;
	
	private static final int DEFAULT_PORT = 6379;
	
	private LocalLoadAnalyzer localLoadAnalyzer = null;
	
	public RPubHubWrapper(String hubId) {
		this.hubId_ = hubId;
	}
	
	public void run() {
		
		// Obtain hostInfo based on hubId
		this.hostInfo = new RPubHostInfo(Integer.parseInt(this.hubId_));
		
		// Load defaultHostInfo
		this.defaultHostInfo = new RPubHostInfo(0);
		
		// Load redis path
		
		Properties props = PropertyManager.getProperties(Client.DEFAULT_CONFIG_FILE);
		this.redisPath_ = props.getProperty("network.rpub.redispath");
		
		// Prepare args
		List<String> args = new ArrayList<String>();
		args.add(this.redisPath_ + "/redis-server");
		if (this.hostInfo.getPort() != DEFAULT_PORT) {
			args.add("--port " + this.hostInfo.getPort());
		}
		args.add("--requirepass ouibsgkbvilbsgksu78272lisfkblb171bksbksv177282");
		
		ProcessBuilder builder = new ProcessBuilder(args);
		builder.directory(new File(this.redisPath_));
		
		// ...
		
		// Launch process
		try {
			this.process_ = builder.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Inherit IO -> java 6 syntax
		inheritIO(process_.getInputStream(), System.out);
	    inheritIO(process_.getErrorStream(), System.err);
	    
	    Long pid = null;
	    
	    // Dirty hack to get pid on Linux
	    try {
        	Class<?> clazz = Class.forName("java.lang.UNIXProcess");
        	
			Field pidField = clazz.getDeclaredField("pid");
			
			pidField.setAccessible(true);
			
			int value = Integer.parseInt(pidField.get(process_).toString());
			
			pid = (long)value;
			
			
		} catch (Throwable e) {
				// Unable to get PID - nothing to worry about
				e.printStackTrace();
		}	    
	    
	    try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    // Wait for some time (10 seconds?) to let redis time to start and spawn all clients
	    System.out.println("=== Please quickly start all RPubHub instances ===");
	    try {
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    
	    // Create our infrastructure network engine
    	this.engine = new RPubNetworkEngine(RPubManagerType.Dynamoth, true);
    	
    	// Connect to instance 0, obtain and configure manager
    	// Retrieve the manager which will be of type LLADynamoth and setup the hostname and port based on our hub ID
    	// important because LLA update messages will be sent to instance 0!
		DynamothRPubManager manager = (DynamothRPubManager) (this.engine.getRPubManager());
		
		// Setup manager port
		//manager.setLocalJedisHost(defaultHostInfo.getHostName());
		//manager.setLocalJedisPort(defaultHostInfo.getPort()); 
    	
		// Connect NE
    	try {
			this.engine.connect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AlreadyConnectedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	   
    	
    	// Reactor
    	this.reactor = new Reactor("RPubHubWrapperReactor", engine);
	    
	    // Setup Local Load Analyzer
	    setupLocalLoadAnalyzer();
	}
	
	private void setupLocalLoadAnalyzer() {
		// Setup our local load analyzer also in infrastructure mode using our special LLADynamoth RPub Manager
		llaEngine = new RPubNetworkEngine(RPubManagerType.LLADynamoth, true);
		
		// If port is different from default port
		if (this.hostInfo.getPort() != DEFAULT_PORT) {
		
			// Retrieve the manager which will be of type LLADynamoth and setup the port based on our hub ID
			LLADynamothRPubManager manager = (LLADynamothRPubManager) (this.llaEngine.getRPubManager());
			
			// Setup manager port
			manager.setLocalJedisPort(this.hostInfo.getPort());
		
		}
		
    	// Reactor
    	Reactor reactor = new Reactor("LLAReactor", this.llaEngine);
		try {
			this.llaEngine.connect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AlreadyConnectedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// The rpub client ID will be the same as the huh id 
		LocalLoadAnalyzer localLoadAnalyzer = new LocalLoadAnalyzer(new RPubClientId(Integer.parseInt(hubId_)), engine, llaEngine, this.reactor);
		this.localLoadAnalyzer = localLoadAnalyzer;
	}
	
	// Comes from: http://stackoverflow.com/a/14168097
	private static void inheritIO(final InputStream src, final PrintStream dest) {
	    new Thread(new Runnable() {
	        public void run() {
	            Scanner sc = new Scanner(src);
	            while (sc.hasNextLine()) {
	                dest.println(sc.nextLine());
	            }
	        }
	    }).start();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// Create a RPubHubWrapper and launch it
		String hubId = "0";
		if (args.length>0) {
			hubId = args[0];
		}
		RPubHubWrapper wrapper = new RPubHubWrapper(hubId);
		wrapper.run();
	}

}
