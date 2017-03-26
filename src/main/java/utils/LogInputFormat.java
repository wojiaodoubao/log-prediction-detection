package utils;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
/**
 * 日志InputFormat：
 * 1.支持split时分割日志文件
 * 2.支持任意编码日志文件
 * 3.压缩输入支持？==FileInputFormat.压缩输入支持
 * 输入：按行划分的日志。
 * 输出：<文件中索引，日志文件名+'\n'+行内容>。
 * */
public class LogInputFormat extends FileInputFormat<LongWritable, Text>{
	//isSplitable默认是true
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new LogRecordReader();
	}	
	/**
	 * 输入：按行划分的日志。
	 * 输出：<文件中索引，日志文件名+'\n'+行内容>。
	 * */
	public static class LogRecordReader extends LineRecordReader{
		private Text value;
		private String logFileName;
		public void initialize(InputSplit inputSplit,
	            TaskAttemptContext taskAttemptContext) throws IOException {
			super.initialize(inputSplit, taskAttemptContext);
			FileSplit fileSplit = (FileSplit)inputSplit;//FileInputFormat.getSplits方法返回值真实类型：List<FileSplit>
			logFileName = fileSplit.getPath().getName();	
		}
		public boolean nextKeyValue() throws IOException {
			boolean label = super.nextKeyValue();
			if(!label)
				return label;
			String oldValue = super.getCurrentValue().toString();
			value =  new Text(logFileName+"\n"+oldValue);
			return label;
		}
		@Override
		public Text getCurrentValue() {
		  return value;
		}	
	}	
}