package common;

public class RemoteLocation {
	public String ip; 
	public int port; 
	
	public RemoteLocation(String ipAddress, int portNumber) {
		this.ip = ipAddress;
		this.port = portNumber;
	}
	
	public boolean ifExists(RemoteLocation compareMe)
	{
		return (this.ip == compareMe.ip && this.port == compareMe.port);
	}
	
}
