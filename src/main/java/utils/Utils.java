package utils;

import java.io.File;

public class Utils {
	public static boolean deleteFile(File f){
		if(f==null)
			return true;
		else if(f.isFile())
    		return f.delete();
    	else if(f.isDirectory()){
    		File[] list = f.listFiles();
    		for(File t:list){
    			if(!deleteFile(t))return false;
    		}
    		return f.delete();
    	}
    	else
    		return false;
	}
	public static boolean deletePath(String path){
    	File f = new File(path);
    	return deleteFile(f);
	}
}
