package com.client;

public class RID {
	// Uniquely identifies the record, use for finding next and previous records 
	// The RID is uniquely identified by the chunkHandle, and the offset of the record inside the chunk 
	public String chunkHandle; 
	public int indexInChunk; 
	public int recordSize; 
}
