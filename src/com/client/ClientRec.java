package com.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;
import com.constants.Constants;

import common.RemoteLocation;


/**
 * Data is stored in chunks, and chunk headers contain the slot maps for the size and offset of each record inside the file 
 */

public class ClientRec {
	
	private ObjectInputStream inputStream; 
	private ObjectOutputStream outputStream; 
	
	private String MASTER_IP = "127.0.0.1"; 
	private int MASTER_PORT = 8888; 
	
	private void setupStreamsIfRequired() {
		Socket socket;
		try {
			socket = new Socket(MASTER_IP, MASTER_PORT);
			if (inputStream == null) {
				inputStream = new ObjectInputStream(socket.getInputStream()); 
			}
			if (outputStream == null) {
				outputStream = new ObjectOutputStream(socket.getOutputStream()); 
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * Appends a record to the open file as specified by ofh Returns BadHandle
	 * if ofh is invalid Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 *
	 * Example usage: AppendRecord(FH1, obama, RecID1)
	 */
	public FSReturnVals AppendRecord(FileHandle ofh, byte[] payload, RID RecordID) {
		if (payload.length > ChunkServer.ChunkSize) {
			return FSReturnVals.RecordTooLong; 
		}
		
		String targetChunkHandle = getChunkHandleForRecordAppend(ofh, payload.length); 
		
		if (targetChunkHandle == null) {
			System.out.print("Could not find valid chunkhandle for record append");
			return FSReturnVals.Fail; 
		}
		
		for (RemoteLocation chunkServerLocation : ofh.chunkLocations.get(targetChunkHandle)) {
			ClientRecStreamCommunicator communicator = new ClientRecStreamCommunicator(chunkServerLocation);			
			RecordID.indexInChunk = communicator.appendRecord(targetChunkHandle, payload); 	
		}
		
		RecordID.chunkHandle = targetChunkHandle; 
		
		return FSReturnVals.Success;  
	}

	private String getChunkHandleForRecordAppend(FileHandle ofh, int payloadSize) {			
			for (String currentChunkHandle : ofh.chunkHandles) {
				RemoteLocation primary = ofh.chunkLocations.get(currentChunkHandle).get(0);   
				ClientRecStreamCommunicator comm = new ClientRecStreamCommunicator(primary); 
				
				int currentChunkFilledCapacity = comm.getChunkFilledCapacity(currentChunkHandle); 
				if (currentChunkFilledCapacity + payloadSize < ChunkServer.ChunkSize) {
					return currentChunkHandle; 
				}
			}
			
			// We haven't found a chunk with enough space yet, so we make a new one		
			try {
				setupStreamsIfRequired(); 
				outputStream.writeObject(Constants.CreateChunkTag);
				outputStream.writeObject(ofh.getFP());

				String result = (String) inputStream.readObject(); 

				if (result.equals(Constants.SuccessTag)) {
					String targetChunkHandle = (String) inputStream.readObject(); 
					@SuppressWarnings("unchecked")
					List<RemoteLocation> targetLocations =  (List<RemoteLocation>) inputStream.readObject(); 
					ofh.chunkLocations.get(targetChunkHandle).addAll(targetLocations);  
					return targetChunkHandle; 
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			
			return null; 
	}

	/**
	 * Deletes the specified record by RecordID from the open file specified by
	 * ofh Returns BadHandle if ofh is invalid Returns BadRecID if the specified
	 * RID is not valid Returns RecDoesNotExist if the record specified by
	 * RecordID does not exist.
	 *
	 * Example usage: DeleteRecord(FH1, RecID1)
	 */
	public FSReturnVals DeleteRecord(FileHandle ofh, RID RecordID) {
		String chunkHandleForRecord = getChunkHandleForRecord(ofh, RecordID);
		
		for (RemoteLocation location: this.getLocationsForChunkHandle(chunkHandleForRecord)) {
			ClientRecStreamCommunicator comm = new ClientRecStreamCommunicator(location); 
			byte[] chunkData = comm.readChunk(chunkHandleForRecord, 0, ChunkServer.ChunkSize); 
			
		}
		
		String command = "Delete Record"; 
		try {
			outputStream.writeObject(command); 
			outputStream.writeObject(ofh);
			outputStream.writeObject(RecordID);
			outputStream.flush();
			// TODO: Deleting the file should free up its space in the chunk, but should retain the slot in the 
			// slot map. Subsequent append records should append to the slot map, but can overwrite the freed up 
			// space. Don't reclaim space while the file is open. 
		}
		catch (Exception e) {
			e.printStackTrace();
			return FSReturnVals.Fail
		}
		
		String result = (String) inputStream.readObject(); 
		if (result == "Success") {
			return FSReturnVals.Success; 
		}
		else if (result == "BadRecordID") {
			return FSReturnVals.BadRecID; 
		}
		else if (result == "BadFileHandle") {
			return FSReturnVals.BadHandle; 
		}
		else {
			return FSReturnVals.Fail; 
		}
	}
	
	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 * Example usage: ReadFirstRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec){		
		String firstChunk = ofh.chunkHandles.get(0); 
		RemoteLocation primary = ofh.chunkLocations.get(firstChunk).get(0); 
		
		ClientRecStreamCommunicator comm = new ClientRecStreamCommunicator(primary); 
		Vector<RID> recordsInChunk = comm.readChunkRecordMappings().get(firstChunk);   

		RID firstRecordID = recordsInChunk.get(0); 
		
		byte[] payload = comm.readChunk(firstChunk, 0, firstRecordID.recordSize); 
		rec.setRID(firstRecordID);
		rec.setPayload(payload);
		
		return FSReturnVals.Success; 
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		String lastChunk = ofh.chunkHandles.get(ofh.chunkHandles.size() - 1); 
		RemoteLocation primary = ofh.chunkLocations.get(lastChunk).get(0); 
		ClientRecStreamCommunicator comm = new ClientRecStreamCommunicator(primary); 
		Vector<RID> recordsInChunk = comm.readChunkRecordMappings().get(lastChunk);
		
		RID lastRecordID = recordsInChunk.get(recordsInChunk.size() - 1); 
		byte[] payload = comm.readChunk(lastChunk, lastRecordID.indexInChunk, lastRecordID.recordSize); 
		
		rec.setRID(lastRecordID);
		rec.setPayload(payload);
		
		return FSReturnVals.Success; 
	}

	/**
	 * Reads the next record after the specified pivot of the file specified by
	 * ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadFirstRecord(FH1, tinyRec1) 2. ReadNextRecord(FH1,
	 * rec1, tinyRec2) 3. ReadNextRecord(FH1, rec2, tinyRec3)
	 */
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec){
		RemoteLocation primary = ofh.chunkLocations.get(pivot.chunkHandle).get(0); 
		ClientRecStreamCommunicator comm = new ClientRecStreamCommunicator(primary); 
		
		Vector<RID> chunkRecords = comm.readChunkRecordMappings().get(pivot.chunkHandle); 
		
		//TODO: Implement == in RID 
		if (chunkRecords.get(chunkRecords.size() - 1) == pivot) {
			// We're at the last record in this chunk, we move to the next record 
			int nextChunkIndex = ofh.chunkHandles.indexOf(pivot.chunkHandle); 
			String nextChunk = ofh.chunkHandles.get(nextChunkIndex); 
			RemoteLocation nextChunkFetchLocation = ofh.chunkLocations.get(nextChunk).get(0); 
			ClientRecStreamCommunicator nextCommunicator = new ClientRecStreamCommunicator(nextChunkFetchLocation); 
			Vector<RID> nextChunkRecords = nextCommunicator.readChunkRecordMappings().get(nextChunk);
			RID nextRecord = nextChunkRecords.get(0); 
			rec.setRID(nextRecord);
			byte[] payload = nextCommunicator.readChunk(nextRecord.chunkHandle, nextRecord.indexInChunk, nextRecord.recordSize); 
			rec.setPayload(payload);
		}
		else {
			// We can just return the next record from the current chunk 
			RID nextRecordIndex = chunkRecords.indexOf(pivot) + 1; 
			RID nextRecord = chunkRecords.get(nextRecordIndex); 
			rec.setRID(nextRecord);
			byte[] payload = comm.readChunk(nextRecord.chunkHandle, nextRecord.indexInChunk, nextRecord.recordSize); 
		}
		
		return FSReturnVals.Success; 
	}

	/**
	 * Reads the previous record after the specified pivot of the file specified
	 * by ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadLastRecord(FH1, tinyRec1) 2. ReadPrevRecord(FH1,
	 * recn-1, tinyRec2) 3. ReadPrevRecord(FH1, recn-2, tinyRec3)
	 */
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec rec){
		return null;
	}
}
