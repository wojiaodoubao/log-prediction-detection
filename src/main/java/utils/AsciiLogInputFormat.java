package utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
/**
 * 日志InputFormat：
 * 1.支持split时分割日志文件
 * 2.支持ASCII编码日志文件
 * 3.不支持压缩输入
 * 输入：按行划分的日志。
 * 输出：<文件名，行内容>。
 * */
public class AsciiLogInputFormat extends FileInputFormat<Text,Text>{
	//isSplitable默认是true
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new DocRecordReader();
	}		
	/**
	 * 输入：按行划分的日志。
	 * 输出：<文件名，行内容>。
	 * 只支持ASCII编码日志，一个字符一个byte；实现效率低(buffer没有使用好，应该用循环buffer的)；
	 * */
	public static class DocRecordReader extends RecordReader<Text, Text>{
		private final int BUFFER_SIZE = 1024;//目前是1KB
		private final byte SPLIT = 10;//'\n'的ASCII码值是10
		private long start;//Split开始索引
		private long end;//Split末位索引+1
		private long pos;//下一个该读的位置
		private String logFileName;
		private byte[] buffer = new byte[BUFFER_SIZE];
		/*Hadoop API specific variables*/
		private FileSplit fileSplit;
		private Configuration conf;
		private FSDataInputStream in = null;
		/*Mapper input key/value instances*/
		private Text key = new Text();
		private Text value = new Text();
		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
//			System.out.println("DocInputFormat initialize");
			this.fileSplit = (FileSplit)inputSplit;//FileInputFormat.getSplits方法返回值真实类型：List<FileSplit>
			this.conf = taskAttemptContext.getConfiguration();
			start = fileSplit.getStart();
			end = start + fileSplit.getLength();//文件offset是[start,end)，要注意！
			logFileName = fileSplit.getPath().getName();
			FileSystem fs = fileSplit.getPath().getFileSystem(conf);
			this.in = fs.open(fileSplit.getPath());
			this.in.seek(start);
			//找到第一个'\n'，从'\n'下一个开始计start；
			//如果整个Split都不存在'\n'，则令pos>end。
			if(start!=0){
				int readLength = 0;
				while(start<end){
					readLength = in.read(start, buffer, 0, BUFFER_SIZE<(end-start)?BUFFER_SIZE:(int)(end-start));//从in[start]读，写到buffer
					int index = indexOf(buffer,0,readLength,SPLIT);
					if(index<0)
						start += readLength;
					else{
						start += index+1;
						break;
					}
				}
			}
			this.pos = start;
		}
		private int indexOf(byte[] buffer,int start,int end,byte split){
			for(int i=start;i<buffer.length&&i<end;i++){
				if(split==buffer[i])return i;
			}
			return -1;
		}
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(pos>=end)return false;
			key = new Text(logFileName);
			//构造value，截取从pos到第一个'\n'之前的内容。[pos,xxx]'\n'。
			StringBuffer value = new StringBuffer();
			int readLength = 0;
			while(pos<end){
				readLength = in.read(pos, buffer, 0, BUFFER_SIZE<(end-pos)?BUFFER_SIZE:(int)(end-pos));//从in[pos]读，写到buffer
				for(int i=0;i<readLength;i++){
					pos++;
					if(SPLIT==buffer[i]){
						this.value = new Text(value.toString());
						return true;
					}
					value.append((char)buffer[i]);
				}
			}
			return false;
		}
		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}
		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}
		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}
		@Override
		public void close() throws IOException {
			in.close();
		}		
	}	
}