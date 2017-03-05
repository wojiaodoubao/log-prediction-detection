package prediction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.Utils;

/**
 * InputFormat产生的<K,V>格式：
 * 	 AsciiLogInputFormat:<logFileName,RawContent>；LogInputFormat:<文件中索引，logFileName+'\n'+RawContent>
 * Mapper产生的<K,V>格式：
 * 	 <content+logfileName+time,logFileName+time>
 * Reducer产生的<K,V>格式：
 * 	 <content,index[logFileName1(time...),logFileName2(time...)]>
 * */
public class DataPreprocessing extends Configured implements Tool{
	public static void main(String args[]) throws Exception{
		ToolRunner.run(new DataPreprocessing(), args);//ToolRunner.run()方法中为Configuration赋了初值。	
	}
	public int run(String[] allArgs) throws Exception {
	    //设置输出分隔符，conf的设置要先于Job的构造
	    this.getConf().set("mapreduce.output.textoutputformat.separator",SPLIT);
		
		Job job = Job.getInstance(getConf());
	    job.setJarByClass(DataPreprocessing.class);
	    //job.setInputFormatClass(AsciiLogInputFormat.class);
	    job.setInputFormatClass(LogInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    	    
	    job.setMapOutputKeyClass(LogWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    //job.setMapperClass(AsciiMapper.class);
	    job.setMapperClass(PreMapper.class);
	    job.setReducerClass(PreReducer.class);
	    
	    //设置sort-comparator:完全排序
	    job.setSortComparatorClass(SortComparator.class);
	    //设置group-comparator:部分排序
	    job.setGroupingComparatorClass(GroupComparator.class);
	    String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	    if(args==null||args.length<2){
	    	Scanner sc = new Scanner(System.in);
	    	args = new String[2];
	    	args[0] = sc.nextLine();
	    	args[1] = sc.nextLine();
	    	Utils.deletePath(args[1]);
	    }
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));   
	    System.out.println("提交任务");
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }
	}	
	public static final String SPLIT = "\t";
	
	private static class SortComparator extends WritableComparator{
		public SortComparator(){
			super(LogWritable.class,true);//反射构造为LogWritable对象
		}
		//完全排序
		@Override
		public int compare(WritableComparable obj1,WritableComparable obj2){
			LogWritable first = (LogWritable)obj1;
			LogWritable second = (LogWritable)obj2;
			return first.compareTo(second);
		}
	}
	
	private static class GroupComparator extends WritableComparator{
		public GroupComparator(){
			super(LogWritable.class,true);
		}		
		//部分排序
		@Override
		public int compare(WritableComparable obj1,WritableComparable obj2){
			LogWritable first = (LogWritable)obj1;
			LogWritable second = (LogWritable)obj2;
			return first.content.compareTo(second.content);
		}		
	}	

	public static class AsciiMapper extends Mapper<Text,Text,LogWritable,Text>{
		@Override
        public void map(Text key,Text value, Context context) throws IOException, InterruptedException {
			String logFileName = key.toString();
			String content = "";
			String time = "";
			
			String[] s = value.toString().split(","); 
			if(s==null||s.length<2)return;
			time = s[0];
            content = s[1];
            
            LogWritable outkey = new LogWritable(content,logFileName,time);
//			System.out.println(outkey+"\n"+logFileName+":"+time);
            context.write(outkey,new Text(logFileName+"\n"+time));
        }		
	}
	
	public static class PreMapper extends Mapper<Object,Text,LogWritable,Text>{
		@Override
        public void map(Object key,Text value, Context context) throws IOException, InterruptedException {
			String content = "";
			String logFileName = "";
			String time = "";
			
			String[] s = value.toString().split("\n"); 
			if(s==null||s.length<2)return;
			logFileName = s[0];
			int index = s[1].indexOf(',');
            if(index<0)return;
            time = s[1].toString().substring(0, index);
            content = s[1].toString().substring(index+1, s[1].toString().length());
            
            LogWritable outkey = new LogWritable(content,logFileName,time);
//			System.out.println(outkey+"\n"+logFileName+":"+time);
            context.write(outkey,new Text(logFileName+"\n"+time));
		} 		
	}
	private static class PreReducer extends Reducer<LogWritable,Text,Text,Text>{
		//输出格式：
		//log1
		//time0,time1,time2
		//log2
		//
		//log3
		//time0,time1
        @Override
		public void reduce(LogWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
//        	System.out.println("in Reduce()");
        	StringBuffer out = new StringBuffer();
        	String lastLogFile = null;
        	for(Text value:values){
        		String[] s = value.toString().split("\n");
        		if(lastLogFile==null){
        			lastLogFile = s[0];
        			out.append(s[0]+SPLIT+s[1]+",");
        		}
        		else if(s[0].equals(lastLogFile)){
        			out.append(s[1]+",");
        		}
        		else{
        			lastLogFile = s[0];
        			out.deleteCharAt(out.length()-1);//删除最后一个','
        			out.append(SPLIT+s[0]+SPLIT+s[1]+",");
        		}
        	}
        	out.deleteCharAt(out.length()-1);//删除最后一个','
        	context.write(key.content, new Text(out.toString()));
        }		
	}
}