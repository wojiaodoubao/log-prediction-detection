package utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * 按照整个文件Split，文件不可分。
 * 
 * */
public class FullFileTextInputFormat extends TextInputFormat{
	@Override
	public boolean isSplitable(JobContext context,Path path){
		return false;
	}
}
