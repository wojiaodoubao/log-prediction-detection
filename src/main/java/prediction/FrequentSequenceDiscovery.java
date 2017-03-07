package prediction;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import prediction.SeqWritable.Index;
import utils.Utils;
/**
 * 频繁模式序列挖掘
 * 
 * 一.MR设计思路
 * 1.Map负责解析出序列(SeqWritable)，并根据sid将序列发送到指定Reducer。
 * 2.第i个Reducer接收所有sid<=i的序列(SeqWritable) <=====> sid序列要发送给从[sid,metaNum-1]这些Reducer。
 * 3.key就是Reducer编号，一个key一个Reducer(OneKeyOneReducerPartitioner)，第i个key发给第i个Reducer。
 * 4.根据收到序列进行频繁序列挖掘。
 * 
 * 二.输出：
 * 0.序列seq频繁，序列seq'是seq的连续子序列，则seq'显然也频繁。如果挖掘结果同时包含seq和seq'，则说结果重复。
 * 1.每个Reducer的输出，单独来看都是无重复的。
 * 2.所有Reducer的输出汇总来看，是会发生重复的。
 * 
 * 三.输入
 * 1.参数输入：
 * 	1）gap是long类型的，单位是毫秒。
 * 	2）frequency是频数，等于支持度*日志数。
 * 2.文件输入：
 * 	MetaFileSplit的输出结果。每行是一条日志及其索引，日志内容已经用Meta替换过。
 * */
public class FrequentSequenceDiscovery extends Configured implements Tool{
	public static void main(String args[]) throws Exception{
		ToolRunner.run(new FrequentSequenceDiscovery(), args);//ToolRunner.run()方法中为Configuration赋了初值。
	}
	private static final String META_NUM = "meta.num";
	private static final String GAP = "gap";
	private static final String FREQUENCY = "frequency";
	public int run(String[] allArgs) throws Exception {
	    String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	    if(args==null||args.length<5){
	    	Scanner sc = new Scanner(System.in);
	    	args = new String[5];
	    	args[0] = sc.nextLine();//input
	    	args[1] = sc.nextLine();//output
	    	args[2] = sc.nextLine();//dict
	    	args[3] = sc.nextLine();//gap
	    	args[4] = sc.nextLine();//frequency = 支持度*日志数
	    	Utils.deletePath(args[1]);
	    }	
	    //获取gap
	    long gap = Long.parseLong(args[3]);
	    getConf().set(GAP, gap+"");
	    int frequency = Integer.parseInt(args[4]);
	    getConf().set(FREQUENCY, frequency+"");
		//获取LogMeta数量
	    int metaNum = 0;
	    Path dictPath = new Path(args[2]);
	    FileSystem fs = dictPath.getFileSystem(getConf());
	    Scanner sc = new Scanner(fs.open(dictPath));
	    while(sc.hasNextLine()){
	    	sc.nextLine();
	    	metaNum++;
	    }
	    getConf().set(META_NUM, metaNum+"");

		Job job = Job.getInstance(getConf());
	    job.setJarByClass(LogToMeta.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    	    
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(SeqWritable.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(NullWritable.class);

	    job.setMapperClass(DeliverSeqMapper.class);
	    job.setPartitionerClass(OneKeyOneReducerPartitioner.class);
	    job.setReducerClass(FrequentSequenceGenerateReducer.class);
	    
	    job.setNumReduceTasks(metaNum);
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));   
	    System.out.println("提交FrequentSequenceDiscovery任务");
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }	    
	}
	private static final String DATE_STRING = "yyyy-MM-dd hh:mm:ss";
	private static class DeliverSeqMapper extends Mapper<LongWritable,Text,IntWritable,SeqWritable>{
		private int metaNum = 0;
		private int frequency = 0;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			this.metaNum = Integer.parseInt(context.getConfiguration().get(META_NUM));
			this.frequency = Integer.parseInt(context.getConfiguration().get(FREQUENCY));
		}
		@Override
        public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
			SimpleDateFormat sdf = new SimpleDateFormat(DATE_STRING);
			String[] s = value.toString().split("\t");
			if(s==null||s.length<=0)return;
			//Construct meta list
			int sid = Integer.parseInt(s[0]);
			List<SeqMeta> list = new ArrayList<SeqMeta>();
			list.add(new SeqMeta(sid));
			//construct indexMap
			Map<String,List<Index>> indexMap = new HashMap<String,List<Index>>();
			for(int i=1;i<s.length-1;i+=2){//s[i]-logName s[i+1]-按','分隔的time
				List<Index> timelist = new ArrayList<Index>();
				for(String time:s[i+1].split(",")){
					Date date = null;
					try {
						date = sdf.parse(time);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					if(date!=null){
						timelist.add(new Index(date.getTime(),date.getTime()));
					}
				}
				indexMap.put(s[i], timelist);
			}
			SeqWritable sw = new SeqWritable(list,indexMap);
			//直接把不频繁的删掉，减少传输量
			if(FrequentSequenceGenerator.frequency(sw)>=frequency){
				for(int i =sid;i<metaNum;i++){//核心代码，管理序列发射到对应reducer
//					System.out.println(i+":"+sw);
					context.write(new IntWritable(i), sw);
				}
			}
		} 		
	}
	/**
	 * 1.每个key只由一个Reducer处理(OneKeyOneReducerPartitioner)，第i个key发给第i个Reducer。
	 * 2.第i个Reducer接收所有sid<=i的序列(SeqWritable)
	 * 3.根据收到序列进行频繁序列挖掘
	 * */
	private static class FrequentSequenceGenerateReducer extends Reducer<IntWritable,SeqWritable,Text,NullWritable>{
		private int gap = 0;
		private int frequency = 0;
		protected void setup(Context context) throws IOException, InterruptedException{
			this.gap = Integer.parseInt(context.getConfiguration().get(GAP));
			this.frequency = Integer.parseInt(context.getConfiguration().get(FREQUENCY));
		}		
        //计算：所有元素都<=key，且序列本身含key的频繁序列
		@Override
		public void reduce(IntWritable key, Iterable<SeqWritable> values, Context context) throws IOException, InterruptedException{
			//这里这里！！！！！！！！！！！！！！！
			Set<SeqWritable> seqSet = new HashSet<SeqWritable>();
			for(SeqWritable sw:values)
				seqSet.add(sw.clone());
			seqSet = FrequentSequenceGenerator.getFrequentSequence(seqSet, gap, frequency);
			for(SeqWritable sw:seqSet){
				String[] split = sw.toString().split("\n");
				context.write(new Text(split[0]+":"+split[1]), NullWritable.get());
			}
        }		
	}
	private static class OneKeyOneReducerPartitioner extends Partitioner<IntWritable,SeqWritable> implements Configurable{
		private Configuration conf;
		public void setConf(Configuration conf) {
			this.conf = conf;
		}
		public Configuration getConf() {
			return this.conf;
		}
		@Override
		//一个key一个Reducer
		public int getPartition(IntWritable key, SeqWritable value, int numPartitions) {			
			return key.get();
		}
	}
}
