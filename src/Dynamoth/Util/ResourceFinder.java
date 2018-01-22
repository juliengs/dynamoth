package Dynamoth.Util;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import Dynamoth.Client.Client;
import Dynamoth.Util.Properties.PropertyManager;

public class ResourceFinder {
	
	public static URL find(String resource) {
		
		String[] paths = null;
		
		if (resource.toLowerCase() == "mammoth.properties") {
			paths = new String[] {"./"};
		}
		else {
			Properties props = PropertyManager.getProperties(Client.DEFAULT_CONFIG_FILE);
			paths = props.getProperty("resourcefinder.paths").split(";");
		}
		
		URL url = null;
		
		// 1- Attempt to load resources from a list of predefined paths first
		for (String path: paths) {
			File file = new File(path + resource);
			if (file.exists()) {
				try {
					url = new File(path + resource).toURI().toURL();
				} catch (MalformedURLException e) {
					url = null;
				}
				if (url != null)
					return url;
			}
		}
		
		// 2- Attempt to load resources from the context class loader => might be able to locate resources
		// in default/specific folders and "onejar" archives
		return Thread.currentThread().getContextClassLoader().getResource(resource);
	}

}
