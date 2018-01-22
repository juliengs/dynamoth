package Dynamoth.Client;

import java.lang.reflect.Field;

public class LoadOSLibraries {

	public void LoadLibrary()
	{
		String bits = "32";
		String os = "win";
		if (System.getProperty("os.arch").contains("64"))
		{
			bits = "64";
			
		}
		if (System.getProperty("os.name").equals("Linux"))
		{
			os = "linux";
			
		}
		else if (System.getProperty("os.name").contains("Mac"))
		{
		os = "macosx";
		bits = "";
		}
		System.setProperty( "java.library.path", "lib/native/" + os + bits + "/" );
		
		Field fieldSysPath = null;
		try {
			fieldSysPath = ClassLoader.class.getDeclaredField( "sys_paths" );
		} catch (SecurityException e1) {
			e1.printStackTrace();
		} catch (NoSuchFieldException e1) {
			e1.printStackTrace();
		}
		fieldSysPath.setAccessible( true );
		try {
			fieldSysPath.set( null, null );
		} catch (IllegalArgumentException e1) {
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			e1.printStackTrace();
		}
	}
	
}
