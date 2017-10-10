package master;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.constants.Constants;

import common.RemoteLocation;

public class Master {
	public static final String MasterConfig = "MasterConfig.txt";
	private static final String MasterLog = "MasterLog.txt";


	public FSManager fsManager;
	private ServerSocket ss;
	private List<ChunkServerThread> chunkServers; 

	public static void main(String[] args)
	{
		Master m = new Master();
		m.listen();
		m.fsManager.createDir("/Test");
		m.fsManager.createDir("/Test/Nest");
		m.fsManager.createDir("/Test/Nested");

		List<String> contents = m.fsManager.listDir("/Test");
		for(String s : contents) System.out.println(s);
		m.fsManager.printContents();
	}

	public Master()
	{
		fsManager = new FSManager("namespace");
		chunkServers = new ArrayList<ChunkServerThread>();
		initServer();
	}

	private void initServer()
	{
		FileReader fr;
		try
		{
			fr = new FileReader(Master.MasterConfig);
			BufferedReader br = new BufferedReader(fr);

	  	String toParse = br.readLine();
	  	int port = Integer.parseInt(toParse.split(":")[1]);

			this.ss = new ServerSocket(port);
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
			System.out.println("Master already running on this system");
			System.exit(-1);
		}
	}

	public void listen()
	{
		System.out.println("Listening for connections");
		while(true)
		{
			try
			{
				Socket s = ss.accept();
				System.out.println("Received a connection");

				ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
				String type = (String) ois.readObject();
				System.out.println("Received connection of type: " + type);
				if(type.equals(Constants.ClientTag))
				{
					ClientThread ct = new ClientThread(s, ois, oos);
					ct.start();
					//possibly add this to a list of connected clients?
				}
				else if(type.equals(Constants.ChunkServerTag))
				{
					ChunkServerThread st = new ChunkServerThread(s, ois, oos);
					chunkServers.add(st);
				}
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	public ChunkServerThread grabChunkServer()
	{
		//randomly select a chunk server
		//probably evens the load out
		int i = new Random().nextInt() % chunkServers.size();
		return chunkServers.get(i);
	}

	//for debugging
	public void printContents()
	{
		fsManager.printContents();
	}




	public class ClientThread extends Thread
	{
		Socket s;
		ObjectInputStream ois;
		ObjectOutputStream oos;

		ClientThread(Socket s, ObjectInputStream ois, ObjectOutputStream oos)
		{
			this.s = s;
			this.oos = oos;
			this.ois = ois;
			listen();
		}

		private void listen()
		{
			while(true)
			{
				try
				{
					String tag = (String) ois.readObject();
					System.out.println("Received tag: " + tag);
					if(tag.equals(Constants.CreateDirTag))
					{
						createDir();
					}
					else if(tag.equals(Constants.DeleteDirTag))
					{
						deleteDir();
					}
					else if(tag.equals(Constants.RenameDirTag))
					{
						renameDir();
					}
					else if(tag.equals(Constants.ListDirTag))
					{
						listDir();
					}
					else if(tag.equals(Constants.CreateFileTag))
					{
						createFile();
					}
					else if(tag.equals(Constants.DeleteFileTag))
					{
						deleteFile();
					}
					else if(tag.equals(Constants.OpenFileTag))
					{
						openFile();
					}
					else if(tag.equals(Constants.CloseFileTag))
					{
						closeFile();
					}
					else if(tag.equals(Constants.CreateChunkTag))
					{
						createChunk();
					}


//					fsManager.printContents();
				}
				catch (ClassNotFoundException e)
				{
					e.printStackTrace();
				}
				catch (IOException e)
				{
//					fsManager.printContents();
					System.out.println("Client disconnected");
					System.exit(0); //only here to make it easier to test
					break;
				}
			}
		}
		
		private void createChunk() throws ClassNotFoundException, IOException
		{
			String filePath = (String) ois.readObject();
			if(fsManager.fileExists(filePath))
			{
				String chunkHandle = fsManager.generateChunkHandle();
				ChunkServerThread cs = Master.this.grabChunkServer();
				boolean success = cs.createChunk(chunkHandle);
				if(success)
				{
					RemoteLocation loc = cs.getRemoteLocation();
					fsManager.createChunk(filePath, chunkHandle, loc);
					List<RemoteLocation> locations = new ArrayList<RemoteLocation>();
					locations.add(loc);
					
					oos.writeObject(Constants.SuccessTag);
					oos.writeObject(chunkHandle);
					oos.writeObject(locations);
					oos.flush();
					
					return;
				}
			}
			
			oos.writeObject(Constants.FailureTag);
		}

		private void listDir() throws ClassNotFoundException, IOException
		{
			String srcDir = (String) ois.readObject();
			List<String> contents = fsManager.listDir(srcDir);
			System.out.println(srcDir + " has " + contents.size() + " items " + contents.getClass());
			oos.writeObject(contents);
			oos.flush();
		}

		private void createFile() throws ClassNotFoundException, IOException
		{
			String srcDir = (String) ois.readObject();
			if(fsManager.dirExists(srcDir))
			{
				oos.writeObject(Constants.SrcExistsTag);
				oos.flush();
			}
			else
			{
				oos.writeObject(Constants.SrcDNETag);
				oos.flush();
				return;
			}

			String fileName = (String) ois.readObject();
			String filePath = fsManager.combinePath(srcDir, fileName);
			if(fsManager.fileExists(filePath))
			{
				oos.writeObject(Constants.FileExistsTag);
				oos.flush();
			}
			else if(fsManager.createFile(filePath))
			{
				oos.writeObject(Constants.SuccessTag);
				oos.flush();
			}
			else
			{
				oos.writeObject(Constants.FailureTag);
				oos.flush();
			}
		}

		private void deleteFile() throws ClassNotFoundException, IOException
		{
			String srcDir = (String) ois.readObject();
			if(fsManager.dirExists(srcDir))
			{
				oos.writeObject(Constants.SrcExistsTag);
				oos.flush();
			}
			else
			{
				oos.writeObject(Constants.SrcDNETag);
				oos.flush();
				return;
			}

			String fileName = (String) ois.readObject();
			String filePath = fsManager.combinePath(srcDir, fileName);
			if(!fsManager.fileExists(filePath))
			{
				oos.writeObject(Constants.FileDNETag);
				oos.flush();
			}
			else if(fsManager.deleteFile(filePath))
			{
				oos.writeObject(Constants.SuccessTag);
				oos.flush();
			}
			else
			{
				oos.writeObject(Constants.FailureTag);
				oos.flush();
			}
		}

		private void openFile() throws ClassNotFoundException, IOException
		{
			String file = (String) ois.readObject();
			if(fsManager.fileExists(file))
			{
				oos.writeObject(Constants.FileExistsTag);
				oos.flush();
			}
			else
			{
				oos.writeObject(Constants.FileDNETag);
				oos.flush();
				return;
			}
			
			List<String> chunks = fsManager.getFileChunks(file);
			oos.writeObject(chunks);
			oos.flush();
			
			Map<String, List<RemoteLocation>> mapping = fsManager.getFileChunkMapping(file);
			oos.writeObject(mapping);
			oos.flush();
		}

		private void closeFile() throws ClassNotFoundException, IOException
		{
			oos.writeObject(Constants.SuccessTag);
		}


		private void renameDir() throws ClassNotFoundException, IOException
		{
			String srcDir = (String) ois.readObject();
			if(fsManager.dirExists(srcDir))
			{
				oos.writeObject(Constants.SrcExistsTag);
				oos.flush();
			}
			else
			{
				oos.writeObject(Constants.SrcDNETag);
				oos.flush();
				return;
			}

			String newPath = (String) ois.readObject();
			if(fsManager.dirExists(newPath))
			{
				oos.writeObject(Constants.DestDirExistsTag);
				oos.flush();
			}
			else if(fsManager.renameDir(srcDir, newPath))
			{
				oos.writeObject(Constants.SuccessTag);
				oos.flush();
			}
			else
			{
				oos.writeObject(Constants.FailureTag);
				oos.flush();
			}
		}

		private void deleteDir() throws ClassNotFoundException, IOException
		{
			String srcDir = (String) ois.readObject();
			if(fsManager.dirExists(srcDir))
			{
				oos.writeObject(Constants.SrcExistsTag);
				oos.flush();
			}
			else
			{
				oos.writeObject(Constants.SrcDNETag);
				oos.flush();
				return;
			}


			String relativeName = (String) ois.readObject();
			String path = fsManager.combinePath(srcDir, relativeName);
			if(fsManager.dirExists(path))
			{
				oos.writeObject(Constants.DestDirExistsTag);
				oos.flush();
			}
			else
			{
				oos.writeObject(Constants.SrcDNETag);
				oos.flush();
			}

			if(fsManager.isEmpty(path))
			{
				if(fsManager.deleteDir(path))
				{
					oos.writeObject(Constants.SuccessTag);
				}
				else
				{
					oos.writeObject(Constants.FailureTag);
				}
			}
			else
			{
				oos.writeObject(Constants.DirNotEmptyTag);
			}
		}

		private void createDir() throws ClassNotFoundException, IOException
		{
			String srcDir = (String) ois.readObject();

			if(fsManager.dirExists(srcDir))
			{
				oos.writeObject(Constants.SrcExistsTag);
				oos.flush();
			}
			else
			{
				oos.writeObject(Constants.SrcDNETag);
				oos.flush();
				return;
			}

			String dirName = (String) ois.readObject();
			String path = fsManager.combinePath(srcDir, dirName);
			if(fsManager.createDir(path))
			{
				oos.writeObject(Constants.SuccessTag);
				oos.flush();
			}
		}
	}

	public class ChunkServerThread extends Thread{
		Socket s;
		ObjectInputStream ois;
		ObjectOutputStream oos;
		RemoteLocation loc;
		static final long heartbeatInterval = 1000;

		ChunkServerThread(Socket s, ObjectInputStream ois, ObjectOutputStream oos)
		{
			this.s = s;
			this.loc = new RemoteLocation(s.getInetAddress().toString(), s.getPort());
			this.oos = oos;
			this.ois = ois;
			listen();
		}
		private void listen()
		{
			while(true)
			{
				try
				{
					Thread.sleep(heartbeatInterval);
					oos.writeObject(Constants.ChunkRequestTag);
					oos.flush();
					
					
					String tag = (String) ois.readObject();
				}
				catch(InterruptedException ie)
				{
					continue;
				}
				catch (ClassNotFoundException e)
				{
					e.printStackTrace();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}
		
		public RemoteLocation getRemoteLocation()
		{
			return loc;
		}
		
		public boolean createChunk(String chunkHandle)
		{
			//TODO:
			return true;
		}
	}
}
