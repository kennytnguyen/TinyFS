package master;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import common.RemoteLocation;

public class FSManager
{
	private String namespaceFile;
	private FSDirectory srcDir;
	private Long chunkCounter = 0;

	FSManager(String namespaceFile)
	{
		this.namespaceFile = namespaceFile;

		//TODO: delete
		File f = new File(this.namespaceFile);
		f.delete();

		initSrcDir();
	}

	public boolean createDir(String filePath)
	{
		FSDirectory parent = getParentDir(filePath);
		if(parent != null)
		{
			String dirName = getRelativeName(filePath);
			if(parent.addDirectory(dirName))
			{
				System.out.println("Successfully created directory: " + dirName);
				persistSrcDir();
				return true;
			}
			else
			{
				System.out.println("Directory already existed");
				return false;
			}
		}
		else
		{
			System.out.println("Parent DNE");
			return false;
			//TODO: throw some kind of error perhaps?
		}
	}

	public boolean deleteDir(String filePath)
	{
		FSDirectory parent = getParentDir(filePath);
		if(parent != null)
		{
			String dirName = getRelativeName(filePath);
			boolean success = parent.deleteDirectory(dirName);
			//TODO: add deletedDir to a hidden area, a la GFS specifications
			persistSrcDir();
			return success;
		}
		else
		{
			return false;
		}
	}

	public boolean fileExists(String filePath)
	{
		FSDirectory parent = getParentDir(filePath);
		String fileName = getRelativeName(filePath);
		if(parent != null)
		{
			return parent.containsFile(fileName);
		}
		else
		{
			return false;
		}
	}
	
	public void createChunk(String filePath, String chunkHandle, RemoteLocation loc)
	{
		FSFile f = getFile(filePath);
		f.addNewChunk(chunkHandle, loc);
	}
	
	public String generateChunkHandle()
	{
		chunkCounter++;
		return Long.toHexString(chunkCounter);
	}

	public boolean isEmpty(String filePath)
	{
		FSDirectory dir = getDirectory(filePath);
		if(dir != null)
		{
			return dir.isEmpty();
		}
		else
		{
			return true; //logically makes sense?
		}
	}

	public List<String> listDir(String filePath)
	{
		FSDirectory dir = getDirectory(filePath);
		return listDir(dir);
	}

	private List<String> listDir(FSDirectory dir)
	{
		List<String> contents = new ArrayList<String>();
		String dirHeader = dir.getAbsoluteName();
		if(dir != null)
		{
			for(FSDirectory d : dir.listDirectories())
			{
				contents.add(dirHeader + "/" + d.getName());
				List<String> insideContents = listDir(d);
				contents.addAll(insideContents);
			}
			for(FSFile f : dir.listFiles())
			{
				contents.add(dirHeader + "/" + f.getName());
			}
		}
		return contents;
	}

	public boolean renameDir(String filePath, String newPath)
	{
		//TODO: deal with the case that the file rename changes parent directories
		FSDirectory dir = getDirectory(filePath);
		if(dir != null)
		{
			String newDirName = getRelativeName(newPath);
			dir.setName(newDirName);
			persistSrcDir();
			return true;
		}
		else
		{
			return false;
		}

	}

	public boolean createFile(String filePath)
	{
		FSDirectory parentDir = getParentDir(filePath);
		if(parentDir != null)
		{
			String fileName = getRelativeName(filePath);
			boolean success = parentDir.addFile(fileName);
			persistSrcDir();
			return success;
		}
		else
		{
			return false;
		}
	}

	public boolean deleteFile(String filePath)
	{
		FSDirectory parentDir = getParentDir(filePath);
		if(parentDir != null)
		{
			String fileName = getRelativeName(filePath);
			FSFile f = parentDir.deleteFile(fileName);
			//TODO: do something with f
			return f != null;
		}
		else
		{
			return false;
		}
	}

	public List<String> getFileChunks(String filePath)
	{
		FSFile f = getFile(filePath);
		if(f != null)
		{
			return f.getChunks();
		}
		else
		{
			return null;
		}
	}

	public Map<String, List<RemoteLocation>> getFileChunkMapping(String filePath)
	{
		FSFile f = getFile(filePath);
		if(f != null)
		{
			return f.getChunkMapping();
		}
		else
		{
			return null;
		}
	}
	
	public void closeFile(String filePath)
	{
		FSFile f = getFile(filePath);
		if(f != null)
		{
			f.close();
		}
	}

	public boolean dirExists(String filePath)
	{
		return getDirectory(filePath) != null;
	}

	private String getRelativeName(String filePath)
	{
		String[] directories = filePath.split("/");
		String dirName = directories[directories.length-1];
		return dirName;
	}

	private FSFile getFile(String filePath)
	{
		FSDirectory parentDir = getParentDir(filePath);
		if(parentDir != null)
		{
			String fileName = getRelativeName(filePath);
			FSFile f = new FSFile(parentDir, fileName);
			return f;
		}
		else
		{
			return null;
		}
	}

	private FSDirectory getDirectory(String filePath)
	{
		if(filePath.equals("/")) return this.srcDir;

		String[] dirs = filePath.split("/");
		FSDirectory parentDir = this.srcDir;
		for(int i = 1; i < dirs.length; ++i)
		{
			if(dirs[i].equals("")) continue; //skip empty strings (usually leading or trailing /s)
			if(parentDir.containsDirectory(dirs[i]))
			{
				parentDir = parentDir.findChildDirectory(dirs[i]);
			}
			else
			{
				return null;
			}
		}

		return parentDir;
	}

	//Gets the immediate owner of a given path
	private FSDirectory getParentDir(String filePath)
	{
		String[] dirs = filePath.split("/");
		FSDirectory parentDir = this.srcDir;
		for(int i = 1; i < dirs.length-1; ++i)
		{
			if(dirs[i].equals("")) continue; //skip empty strings (usually leading or trailing /s)
			if(parentDir.containsDirectory(dirs[i]))
			{
				parentDir = parentDir.findChildDirectory(dirs[i]);
			}
			else
			{
				return null;
			}
		}

		return parentDir;
	}

	public String combinePath(String srcPath, String nestedPath)
	{
		if(srcPath.equals("/")) srcPath = "";
		String path = srcPath + nestedPath;
		return path;
	}

	//for debugging
	public void printContents()
	{
		printDirContents(this.srcDir);
	}

	private void printDirContents(FSDirectory child)
	{
		System.out.println(child.getAbsoluteName());
		List<FSFile> files = child.listFiles();
		for(FSFile f : files)
		{
			System.out.println(f.getAbsoluteName());
		}

		List<FSDirectory> dirs = child.listDirectories();
		for(FSDirectory d : dirs)
		{
			printDirContents(d);
		}
	}

	private void persistSrcDir()
	{
		FileOutputStream fos = null;
		ObjectOutputStream oos = null;
		try
		{
			fos = new FileOutputStream(namespaceFile);
			oos = new ObjectOutputStream(fos);
			oos.writeObject(this.srcDir);
		}
		catch(FileNotFoundException e)
		{
			System.out.println("error");
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if(fos != null) fos.close();
				if(oos != null) oos.close();
			}
			catch (IOException e)
			{
				//nothing to do about that
			}
		}
	}

	private void initSrcDir()
	{
		FileInputStream fis = null;
		ObjectInputStream ois = null;
		try
		{
			fis = new FileInputStream(namespaceFile);
			ois = new ObjectInputStream(fis);
			this.srcDir = (FSDirectory) ois.readObject();
			System.out.println("Successfully loaded src dir");
		}
		catch (FileNotFoundException e)
		{
			System.out.println("Creating src dir");
			this.srcDir = new FSDirectory(null, "/");
			persistSrcDir();
		}
		catch (IOException e)
		{
			System.out.println(e.getMessage());
			//only if object input stream fails
			//TODO:
		}
		catch (ClassNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if(fis != null) fis.close();
				if(ois != null) ois.close();
			}
			catch (IOException e)
			{
				//nothing to do about that
			}
		}
	}
}
