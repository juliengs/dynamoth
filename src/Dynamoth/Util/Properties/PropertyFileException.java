package Dynamoth.Util.Properties;

public class PropertyFileException extends RuntimeException {

	private static final long serialVersionUID = -4789266226735287182L;
	
	private String message;

	public PropertyFileException(String message) {
		super();
		this.message = message;
	}
	
	public String toString() {
		
		return "PropertyFileException " + this.message;
	}
}
