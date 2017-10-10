package com.client;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import common.RemoteLocation;

public class FileHandle {
	/*
	 * Contains the chunks for this file, 
	 * these must be ordered so that the first chunk is at index 0, 
	 * and the last chunk
	 * 
	 * is at index size - 1
	 */
	
	String fp;
	List<String> chunkHandles;
	Map<String, List<RemoteLocation>> chunkLocations;
	boolean chunkBool;
	
	public FileHandle() {
		chunkHandles = new ArrayList<String>();
		chunkLocations = new HashMap <String, List<RemoteLocation>>();
	}
	
	/*
	 * Setter and Getter Methods
	 */
	
	//File Path
	public void setFP(String filePath) {
		this.fp = filePath;
	}
	
	public String getFP() {
		return this.fp;
	}
	
	//Remote Locations
	public void setRemoteLoc(Map<String, List<RemoteLocation>> chunkLoc) {
		this.chunkLocations = chunkLoc;
	}
	
	public Map<String, List<RemoteLocation>> getRemoteLoc() {
		return chunkLocations;
	}

	//Chunk Handles
	public void setHandles(List<String> fileChunks) {
		this.chunkHandles = fileChunks;
	}
	
	public List<String> getHandles() {
		return chunkHandles;
	}
	
	//Chunks
	public void setChunk(boolean bool)
	{
		this.chunkBool = bool;
	}
	
	public boolean getChunk()
	{
		return chunkBool;
	}
	

}
