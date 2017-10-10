package master;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FSDirectory implements Serializable
{
    private FSDirectory parent;
    private String name;
    private List<FSFile> files;
    public void setName(String name)
		{
			this.name = name;
		}

		private List<FSDirectory> directories;

    public FSDirectory(FSDirectory parent, String name)
    {
        this.parent = parent;
        this.name = name;
        this.files = new ArrayList<FSFile>();
        this.directories = new ArrayList<FSDirectory>();
    }

    public boolean addFile(String fileName)
    {
    	if(this.containsFile(fileName))
    	{
    		return false;
    	}
    	else
    	{
  			FSFile f = new FSFile(this, fileName);
  			return this.files.add(f);
    	}
    }
    
    public boolean containsFile(String fileName)
    {
    	boolean contains = false;
    	for(FSFile f : files)
    	{
    		if(f.getName().equals(fileName))
    		{
    			contains = true;
    			break;
    		}
    	}
    	return contains;
    }

    public FSFile deleteFile(String fileName)
    {
    	for(FSFile f : files)
    	{
    		if(f.getName().equals(fileName))
    		{
    			files.remove(f);
    			return f;
    		}
    	}
    	return null;
    }

    public boolean addDirectory(String dirName)
    {
    	if(!this.containsDirectory(dirName))
    	{
    		FSDirectory dir = new FSDirectory(this, dirName);
            return this.directories.add(dir);
    	}
    	else
    	{
    		return false;
    	}
    }

    public String getName()
    {
    	return name;
    }

    public List<FSFile> listFiles()
    {
    	return files;
    }

    public List<FSDirectory> listDirectories()
    {
    	return directories;
    }

    public FSDirectory findChildDirectory(String dirName)
    {
    	for(FSDirectory d : directories)
    	{
    		if(d.getName().equals(dirName))
    		{
    			return d;
    		}
    	}

    	return null;
    }

    public boolean deleteDirectory(String dirName)
    {
    	FSDirectory toDel = findChildDirectory(dirName);
    	this.directories.remove(toDel);
    	return toDel != null;
    }

    public boolean isEmpty()
    {
    	return files.size() == 0 && directories.size() == 0;
    }

    public boolean containsDirectory(String dirName)
    {
    	boolean contains = false;
    	for(FSDirectory d : directories)
    	{
    		if(d.getName().equals(dirName))
    		{
    			contains = true;
    			break;
    		}
    	}
    	return contains;
    }

    String getAbsoluteName()
    {
    	if(parent != null)
    		return parent.getAbsoluteName() + "/" + name;
    	else
    		return ""; //src dir
    }
}
