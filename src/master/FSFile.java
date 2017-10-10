package master;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import common.RemoteLocation;


public class FSFile implements Serializable
{
    private String name;
    private FSDirectory parentDir;
    
    private List<String> chunks;
    private Map<String, List<RemoteLocation>> chunksToChunkServers;
    
    public FSFile(FSDirectory parentDir, String name)
    {
    	this.parentDir = parentDir;
      this.name = name;
      
      chunks = new ArrayList<String>();
      chunksToChunkServers = new HashMap<String, List<RemoteLocation>>();
    }
    
		public String getName()
		{
			return name;
		}
		
		public String getAbsoluteName()
		{
			return parentDir.getAbsoluteName() + name;
		}
		
		public void addNewChunk(String chunkHandle, RemoteLocation chunkServer)
		{
			chunks.add(chunkHandle);
			List<RemoteLocation> locations = new ArrayList<RemoteLocation>();
			locations.add(chunkServer);
			chunksToChunkServers.put(chunkHandle, locations);
		}
		
		public List<String> getChunks()
		{
			return chunks;
		}
		
		public Map<String, List<RemoteLocation>> getChunkMapping()
		{
			return chunksToChunkServers;
		}
		
		public void close()
		{
			//TODO:
		}
}
