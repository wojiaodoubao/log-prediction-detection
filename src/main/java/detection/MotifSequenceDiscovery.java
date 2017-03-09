package detection;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import prediction.FrequentSequenceDiscovery;
import prediction.FrequentSequenceGenerator;
import prediction.FrequentSequenceDiscovery.OneKeyOneReducerPartitioner;
import prediction.SeqWritable.Index;
import prediction.LogToMeta;
import prediction.SeqMeta;
import prediction.SeqWritable;
import utils.Utils;

/**
 * Motif序列挖掘
 * 
 * 一.MR设计思路
 * 1.Map负责解析出序列(SeqWritable)，并根据sid将序列发送到指定Reducer。
 * 2.第i个Reducer接收所有sid<=i的序列(SeqWritable) <=====> sid序列要发送给从[sid,metaNum-1]这些Reducer。
 * 3.key就是Reducer编号，一个key一个Reducer(OneKeyOneReducerPartitioner)，第i个key发给第i个Reducer。
 * 4.根据收到序列进行频繁序列挖掘。
 * 
 * 二.输出：
 * 0.motif seq满足POD+FAR，motif seq'是seq的连续子序列，则seq'显然也满足POD。如果seq'也满足FAR且挖掘结果同时包含seq和seq'，则说结果重复。
 * 1.每个Reducer的输出，单独来看都是无重复的。
 * 2.所有Reducer的输出汇总来看，是可能会发生重复的。abc,bc这样。
 * 
 * 三.输入
 * 1.参数输入：
 * 	1）gap是long类型的，单位是毫秒。
 * 	2）POD是double类型 = TP/(TP+FN)
 *  3）FAR是double类型 = FP/(TP+FP)
 * 2.文件输入：
 * 	MetaFileSplit的输出结果。每行是一条日志及其索引，日志内容已经用Meta替换过。
 * */
public class MotifSequenceDiscovery extends Configured implements Tool{
	public static void main(String args[]) throws Exception{
		ToolRunner.run(new MotifSequenceDiscovery(), args);//ToolRunner.run()方法中为Configuration赋了初值。
	}
	private static final String META_NUM = "meta.num";
	private static final String GAP = "gap";
	private static final String D_POD = "my.pod";
	private static final String D_FAR = "my.far";
	private static final String LOG_LABEL = "log.label";//logName1+','+label1+';'+logName2+','+label2 例如log1,+;log2,-
	public int run(String[] allArgs) throws Exception {
	    String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	    if(args==null||args.length<7){
	    	Scanner sc = new Scanner(System.in);
	    	args = new String[7];
	    	args[0] = sc.nextLine();//input
	    	args[1] = sc.nextLine();//output
	    	args[2] = sc.nextLine();//dict
	    	args[3] = sc.nextLine();//gap
	    	args[4] = sc.nextLine();//POD
	    	args[5] = sc.nextLine();//FAR
	    	args[6] = sc.nextLine();//log label：logName1+','+label1+';'+logName2+','+label2
	    	Utils.deletePath(args[1]);
	    }	
	    //设置参数
	    long gap = Long.parseLong(args[3]);
	    getConf().set(GAP, gap+"");
	    double POD = Double.parseDouble(args[4]);
	    getConf().set(D_POD, POD+"");
	    double FAR = Double.parseDouble(args[5]);
	    getConf().set(D_FAR, FAR+"");
	    getConf().set(LOG_LABEL, args[6]);
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
	    job.setJarByClass(MotifSequenceDiscovery.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    	    
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(SeqWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);

	    job.setMapperClass(DeliverMotifMapper.class);
	    job.setPartitionerClass(OneKeyOneReducerPartitioner.class);
	    job.setReducerClass(MotifSequenceGenerateReducer.class);
	    
	    job.setNumReduceTasks(metaNum);
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));   
	    System.out.println("提交MotifSequenceDiscovery任务");
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }	    
	}

	private static class DeliverMotifMapper extends Mapper<LongWritable,Text,IntWritable,SeqWritable>{
		private int metaNum = 0;
		private double POD;
		private double FAR;
		private Map<String,Boolean> logLabel = new HashMap<String,Boolean>(); 
		private int T;
		private int F;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			this.metaNum = Integer.parseInt(context.getConfiguration().get(META_NUM));
			this.POD = Double.parseDouble(context.getConfiguration().get(D_POD));
			this.FAR = Double.parseDouble(context.getConfiguration().get(D_POD));
			//构造logLabel，计算总正例数，负例数
			T = 0;F = 0;
			for(String t:context.getConfiguration().get(LOG_LABEL).split(";")){
				String[] s = t.split(",");
				if(s==null||s.length<2)continue;
				logLabel.put(s[0], s[1].equals("+"));
				if(s[1].equals("+"))T++;
				else F++;
			}
		}
		@Override
        public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
			//construct sid
			String[] s = value.toString().split("\t");
			if(s==null||s.length<=0)return;
			int sid = Integer.parseInt(s[0]);
			//construct SeqWritable from value
			SeqWritable sw = SeqWritable.deserializeSeqWritableFromString(value.toString());
			if(sw==null)return;
			//直接把POD不合法的删掉
			if(MotifSequenceGenerator.satisfiedPOD(sw, POD, logLabel,T,F)){
				for(int i =sid;i<metaNum;i++){//核心代码，管理序列发射到对应reducer
					context.write(new IntWritable(i), sw);
				}
			} 		
		}
	}
	/**
	 * 1.每个key只由一个Reducer处理(OneKeyOneReducerPartitioner)，第i个key发给第i个Reducer。
	 * 2.第i个Reducer接收所有sid<=i的序列(SeqWritable)
	 * 3.根据收到序列进行motif序列挖掘
	 * */
	private static class MotifSequenceGenerateReducer extends Reducer<IntWritable,SeqWritable,Text,NullWritable>{
		private int metaNum = 0;
		private long gap;
		private double POD;
		private double FAR;
		private Map<String,Boolean> logLabel = new HashMap<String,Boolean>(); 
		private int T;
		private int F;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			this.metaNum = Integer.parseInt(context.getConfiguration().get(META_NUM));
			this.gap = Long.parseLong(context.getConfiguration().get(GAP));
			this.POD = Double.parseDouble(context.getConfiguration().get(D_POD));
			this.FAR = Double.parseDouble(context.getConfiguration().get(D_POD));
			//构造logLabel
			for(String t:context.getConfiguration().get(LOG_LABEL).split(";")){
				String[] s = t.split(",");
				if(s==null||s.length<2)continue;
				logLabel.put(s[0], s[1].equals("+"));
			}
			//计算总正例数，负例数
			T = 0;
			F = 0;
			for(boolean label:logLabel.values()){
				if(label)T++;
				else F++;
			}			
		}		
		@Override
		public void reduce(IntWritable key, Iterable<SeqWritable> values, Context context) throws IOException, InterruptedException{
			Set<SeqWritable> seqSet = new HashSet<SeqWritable>();
			for(SeqWritable sw:values)
				seqSet.add(sw.clone());//很关键啊，values迭代返回的每次都是同一个引用，是同一个对象被反复使用。
			seqSet = new MotifSequenceGenerator(seqSet,gap,POD,FAR,logLabel).getMotifSequence();			
			for(SeqWritable sw:seqSet){
				double[] score = MotifSequenceGenerator.compute_POD_FAR_CSI(sw, logLabel, T, F);
				String out = sw.toString().split("\n")[0];
				out+=":";
				if(score!=null&&score.length>=3){
					out+=score[0]+","+score[1]+","+score[2];
				}
				context.write(new Text(out), NullWritable.get());
			}
        }		
	}
}
