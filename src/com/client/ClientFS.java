package com.client;

import master.Master;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.constants.Constants;

import common.RemoteLocation;

public class ClientFS {

    public enum FSReturnVals {
        DirExists, // Returned by CreateDir when directory exists
        DirNotEmpty, //Returned when a non-empty directory is deleted
        SrcDirNotExistent, // Returned when source directory does not exist
        DestDirExists, // Returned when a destination directory exists
        FileExists, // Returned when a file exists
        FileDoesNotExist, // Returns when a file does not exist
        BadHandle, // Returned when the handle for an open file is not valid
        RecordTooLong, // Returned when a record size is larger than chunk size
        BadRecID, // The specified RID is not valid, used by DeleteRecord
        RecDoesNotExist, // The specified record does not exist, used by DeleteRecord
        NotImplemented, // Specific to CSCI 485 and its unit tests
        Success, //Returned when a method succeeds
        Fail; //Returned when a method fails
        //http://codingexplained.com/coding/java/enum-to-integer-and-integer-to-enum

        /*
         * Zazar Code: Might not need this actually
         * 		-Kenny
         */
        /*
        private int returnVal;

        FSReturnVals(int returnVal){
            this.returnVal = returnVal;
        }

        private static Map<Integer, FSReturnVals> returnValMap = new HashMap<Integer, FSReturnVals>();

        static {
            for (FSReturnVals value : FSReturnVals.values()) {
                returnValMap.put(value.returnVal, value);
            }
        }

        public static FSReturnVals valueOf(int returnVal) {
            return returnValMap.get(returnVal);
        }
        */
        /*
         * End of Zazar Codes
         */
    }

    static ObjectOutputStream oos;
    static ObjectInputStream ois;
    static Master myMaster;
    static Socket mySocket;
    static int Master_Port;
    static String Master_IP;

    public static void main(String args[])
    {
    	ClientFS fs = new ClientFS();
    	fs.CreateDir("/", "noel");
    	fs.CreateDir("/", "trivedi");
    	fs.CreateDir("/noel/", "newDir");


    }

    /*
     * In the ClientFS constructor
     *
     * Checks to see if a client is already connected
     * Creates a Socket Connection to the Master's IP & Port
     * Uses this Connection to give the Master Commands
     */
    public ClientFS() {

    	// If there is no existing connection
    	if (mySocket == null) {
    		try{
            	FileReader fr = new FileReader(Master.MasterConfig);
            	BufferedReader br = new BufferedReader(fr);
            	String toParse = br.readLine();
            	String[] IPsplitPort = toParse.split(":");

            	Master_IP = IPsplitPort[0];
            	Master_Port = Integer.parseInt(IPsplitPort[1]);

            	mySocket = new Socket(Master_IP, Master_Port);

            	oos = new ObjectOutputStream(mySocket.getOutputStream());
            	ois = new ObjectInputStream(mySocket.getInputStream());

            	oos.writeObject(Constants.ClientTag);
            	oos.flush(); //Flush any bytes that are still buffered by OOS
            	System.out.println("Successfully connected to Master");
        	} catch (IOException ioe) {
        		System.out.println("Client FS Constructor Fails - File");
        		ioe.printStackTrace();
        	}
    	}
    	return;
    }


    /**
     * Creates the specified dirname in the src directory Returns
     * SrcDirNotExistent if the src directory does not exist Returns
     * DestDirExists if the specified dirname exists
     *
     * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram/",
     * "CSCI485"), CreateDir("/Shahram/CSCI485/", "Lecture1")
     */
    public FSReturnVals CreateDir(String src, String dirname) {

    	try {
    		oos.writeObject(Constants.CreateDirTag);
      	oos.flush();

      	oos.writeObject(src);
      	oos.flush();

      	//Read the input stream and check the protocol it sends to client
      	//SrcDirNotExistent if the src directory does not exist Returns
      	if (((String) ois.readObject()).equals(Constants.SrcDNETag)) return FSReturnVals.SrcDirNotExistent;

      	//if the src exists, write directory
      	oos.writeObject(dirname);
      	oos.flush();

      	//DestDirExists if the specified dirname exists
      	String checkMaster = (String) ois.readObject();
      	if (checkMaster.equals(Constants.DestDirExistsTag)) return FSReturnVals.DestDirExists;

      	//If it gets this far, that means a Directory has been created, return success
      	else if (checkMaster.equals(Constants.SuccessTag)) return FSReturnVals.Success;

    	} catch (IOException ioe) {
    		System.out.println("Create Directory Failed (IO Exception)");
    		ioe.printStackTrace();
    	} catch (ClassNotFoundException cnfe) {
    		System.out.println("Create Directory Failed (Class Not Found)");
    		cnfe.printStackTrace();
    	}

        return null;
    }

    /**
     * Deletes the specified dirname in the src directory Returns
     * SrcDirNotExistent if the src directory does not exist Returns
     * DestDirExists if the specified dirname exists
     *
     * Example usage: DeleteDir("/Shahram/CSCI485/", "Lecture1")
     */
    public FSReturnVals DeleteDir(String src, String dirname) {

    	try {
    		oos.writeObject(Constants.DeleteDirTag);
      	oos.flush();

      	oos.writeObject(src);
      	oos.flush();

      	//Read the input stream and check the protocol it sends to client
      	//SrcDirNotExistent if the src directory does not exist -> Returns
      	if (((String) ois.readObject()).equals(Constants.SrcDNETag)) return FSReturnVals.SrcDirNotExistent;

      	//if the src exists, write directory
      	oos.writeObject(dirname);
      	oos.flush();

      	//DestDirExists if the specified dirname exists
    		//if exists, continue to go and delete
      	if (!((String) ois.readObject()).equals(Constants.DestDirExistsTag)) return FSReturnVals.Success;


      	String checkFinal = (String) ois.readObject();
    		//If it gets this far, that means a Directory needs to be deleted
      	if (checkFinal.equals(Constants.SuccessTag)) return FSReturnVals.Success;
      	else if (checkFinal.equals(Constants.DirNotEmptyTag)) return FSReturnVals.DirNotEmpty;
      	else return FSReturnVals.Fail;

    	} catch (IOException ioe) {
    		System.out.println("Rename Directory Failed (IO Exception)");
    		ioe.printStackTrace();
    	} catch (ClassNotFoundException cnfe) {
    		System.out.println("Rename Directory Failed (Class Not Found)");
    		cnfe.printStackTrace();
    	}

        return null;
    }

    /**
     * Renames the specified src directory in the specified path to NewName
     * Returns SrcDirNotExistent if the src directory does not exist Returns
     * DestDirExists if a directory with NewName exists in the specified path
     *
     * Example usage: RenameDir("/Shahram/CSCI485", "/Shahram/CSCI550") changes
     * "/Shahram/CSCI485" to "/Shahram/CSCI550"
     */
    public FSReturnVals RenameDir(String src, String NewName) {

    	try {
    		oos.writeObject("RenameDir");
      	oos.flush();

      	oos.writeObject(src);
      	oos.flush();

      	//Read the input stream and check the protocol it sends to client
      	//SrcDirNotExistent if the src directory does not exist Returns
      	if (((String) ois.readObject()).equals(Constants.SrcDNETag)) return FSReturnVals.SrcDirNotExistent;

      	//if the src exists, write directory
      	oos.writeObject(NewName);
      	oos.flush();

      	String checkFinal = (String) ois.readObject();

      	//DestDirExists if the specified dirname exists, cannot have duplicate
      	if (checkFinal.equals(Constants.DestDirExistsTag)) return FSReturnVals.DestDirExists;

      	//If it gets this far, that means a Directory has been Renamed, return success
      	else if (checkFinal.equals(Constants.SuccessTag)) return FSReturnVals.Success;

    	} catch (IOException ioe) {
    		System.out.println("Rename Directory Failed (IO Exception)");
    		ioe.printStackTrace();
    	} catch (ClassNotFoundException cnfe) {
    		System.out.println("Rename Directory Failed (Class Not Found)");
    		cnfe.printStackTrace();
    	}

        return null;
    }

    /**
     * Lists the content of the target directory Returns SrcDirNotExistent if
     * the target directory does not exist Returns null if the target directory
     * is empty
     *
     * Example usage: ListDir("/Shahram/CSCI485")
     */
    public String[] ListDir(String tgt) {

    	try {

    		oos.writeObject(Constants.ListDirTag);
      	oos.flush();

      	oos.writeObject(tgt);
      	oos.flush();

        //No need to check if the srcDir exists or anything like that, since Master returns an empty list for nonexistent directories
      	List<String> listDir = (List<String>) ois.readObject();;
      	String[] ret = new String[listDir.size()];
      	for(int i = 0; i < listDir.size(); ++i)
      	{
      		ret[i] = listDir.get(i);
      	}
      	return ret;

    	} catch (IOException ioe) {
    		System.out.println("Create Directory Failed (IO Exception)");
    		ioe.printStackTrace();
    	} catch (ClassNotFoundException cnfe) {
    		System.out.println("Create Directory Failed (Class Not Found)");
    		cnfe.printStackTrace();
    	}

        return null;
    }

    /**
     * Creates the specified filename in the target directory Returns
     * SrcDirNotExistent if the target directory does not exist Returns
     * FileExists if the specified filename exists in the specified directory
     *
     * Example usage: Createfile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
     */
    public FSReturnVals CreateFile(String tgtdir, String filename) {

    	try {
    		oos.writeObject(Constants.CreateFileTag);
      	oos.flush();

      	oos.writeObject(tgtdir);
      	oos.flush();

      	//Read the input stream and check the protocol it sends to client
      	//SrcDirNotExistent if the src directory does not exist Returns
      	if (((String) ois.readObject()).equals(Constants.SrcDNETag)) return FSReturnVals.SrcDirNotExistent;

      	oos.writeObject(filename);
      	oos.flush();

      	//Use a temp variable because a sequential read Objects may screw things up
      	String checkFinal = (String) ois.readObject();
      	if (checkFinal.equals(Constants.FileExistsTag)) return FSReturnVals.FileExists;
      	else if (checkFinal.equals(Constants.SuccessTag)) return FSReturnVals.Success;

    	} catch (IOException ioe) {
    		System.out.println("Create Directory Failed (IO Exception)");
    		ioe.printStackTrace();
    	} catch (ClassNotFoundException cnfe) {
    		System.out.println("Create Directory Failed (Class Not Found)");
    		cnfe.printStackTrace();
    	}

        return null;
    }

    /**
     * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
     * if the target directory does not exist Returns FileDoesNotExist if the
     * specified filename is not-existent
     *
     * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
     */
    public FSReturnVals DeleteFile(String tgtdir, String filename) {

    	try {
    		oos.writeObject(Constants.DeleteFileTag);
      	oos.flush();

      	oos.writeObject(tgtdir);
      	oos.flush();

      	//Read the input stream and check the protocol it sends to client
      	//SrcDirNotExistent if the src directory does not exist Returns
      	if (((String) ois.readObject()).equals(Constants.SrcDNETag)) return FSReturnVals.SrcDirNotExistent;

      	oos.writeObject(filename);
      	oos.flush();

      	//Use a temp variable because a sequential read Objects may screw things up
      	String checkFinal = (String) ois.readObject();
      	if (checkFinal.equals(Constants.FileDNETag)) return FSReturnVals.FileDoesNotExist;
      	else if (checkFinal.equals(Constants.SuccessTag)) return FSReturnVals.Success;

    	} catch (IOException ioe) {
    		System.out.println("Create Directory Failed (IO Exception)");
    		ioe.printStackTrace();
    	} catch (ClassNotFoundException cnfe) {
    		System.out.println("Create Directory Failed (Class Not Found)");
    		cnfe.printStackTrace();
    	}


        return null;
    }

    /**
     * Opens the file specified by the FilePath and populates the FileHandle
     * Returns FileDoesNotExist if the specified filename by FilePath is
     * not-existent
     *
     * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx", FH1)
     */
    public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {

    	try {
    		oos.writeObject(Constants.OpenFileTag);
        	oos.flush();

        	oos.writeObject(FilePath);
        	oos.flush();

        	//Read the input stream and check the protocol it sends to client
        	//SrcDirNotExistent if the src directory does not exist Returns
        	if (((String) ois.readObject()).equals(Constants.FileDNETag)) return FSReturnVals.FileDoesNotExist;

        	//Take the Chunks and map to FileHandle Locations
        	List<String> fileChunks = (List<String>) ois.readObject();
        	ofh.setHandles(fileChunks);
        	Map<String, List<RemoteLocation>> chunkLocations = (Map<String, List<RemoteLocation>>) ois.readObject();
        	ofh.setRemoteLoc(chunkLocations);

    	} catch (IOException ioe) {
    		System.out.println("Create Directory Failed (IO Exception)");
    		ioe.printStackTrace();
    	} catch (ClassNotFoundException cnfe) {
    		System.out.println("Create Directory Failed (Class Not Found)");
    		cnfe.printStackTrace();
    	}

        return null;
    }

    /**
     * Closes the specified file handle Returns BadHandle if ofh is invalid
     *
     * Example usage: CloseFile(FH1)
     */
    public FSReturnVals CloseFile(FileHandle ofh) {

    	try {
    		oos.writeObject(Constants.CloseFileTag);
        	oos.flush();



        	/*
        	 * Not Implemented
        	 *
        	 * Want to see how everything functions first
        	 */


        	if (((String) ois.readObject()).equals("DirCreated")) return FSReturnVals.Success;

    	} catch (IOException ioe) {
    		System.out.println("Create Directory Failed (IO Exception)");
    		ioe.printStackTrace();
    	} catch (ClassNotFoundException cnfe) {
    		System.out.println("Create Directory Failed (Class Not Found)");
    		cnfe.printStackTrace();
    	}

        return null;
    }

}
