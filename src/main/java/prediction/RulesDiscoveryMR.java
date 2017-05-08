package prediction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import motif.Meta;
import utils.NoIndexSeqWritable;
import utils.SeqMeta;
import utils.TimeMeta;
import utils.Utils;
/**
 * 棒！
 * 
 * 这个分布式计算设计得实在是很聪明，利用"相同算法执行得到相同结果"+"用key来确定自己是第几个Reducer"的方法，
 * 解决了让每一个Reducer都获取对应日志文件+所有频繁序列。
 * 不足是结果还需要再MR一次，以freSeq为key，综合其各个划分在所有日志文件上的打分，来计算最终的划分点。
 * 
 * 1.设置阶段：
 * 	获取日志文件数量，根据日志文件数量设置Reducer数量，一个Reducer对应一个日志文件。
 * 2.Map阶段：
 * 	读入频繁序列文件，解析出频繁序列，将每一条频繁序列都发送给所有Reducer。
 * 	对每一条频繁序列x，都发送reducer_num条k-v对，其中k从[0～reducer_num-1]，v是频繁序列
 * 3.Reduce阶段
 * 	Reducer从key中解析出id，从而知道自己应该读取第id个日志文件。
 * 	由于所有Reducer都使用同样的方法遍历日志目录，且对遍历结果进行排序，因此会获取同样的日志文件序列。
 * 	每一个Reducer按照自己的id，从日志序列中去取对应的日志文件。
 * 
 * 输入：原始日志文件 --使用原始日志文件，而不使用MetaFileSplit处理后的日志文件，是因为原始日志文件中包含的是一个完整序列。
 *      DICT文件
 * 原始日志文件：
 * 2016-06-16 10:45:52,aaaaa
 * 2016-06-16 10:45:53,bbbb
 * MetaFileSplit结果文件：
 * 2	log1	2016-06-16 10:45:55	log2	2016-06-16 10:45:53	log3	2016-06-16 10:45:52,2016-06-16 10:45:54
 * 
 * Map输入(频繁序列)格式：
 * 0,4:log3,1466045153000,1466045156000	log2,1466045152000,1466045154000
 * 2,3,4:log3,1466045152000,1466045156000,1466045154000,1466045156000	log1,1466045155000,1466045158000
 * */
public class RulesDiscoveryMR extends Configured implements Tool{
	public static void main(String args[]) throws Exception{
		ToolRunner.run(new RulesDiscoveryMR(), args);//ToolRunner.run()方法中为Configuration赋了初值。		
	}
	private static final String LOG_PATH = "log.path";
	private static final String REDUCE_NUM = "my.reduce.num";
	private static final String GAP = "my.gap";
	private static final String RULE_GAP = "my.rule.gap";
	private static final String CARDINALITY = "my.cardinality";
	private static final String FIRED_THRESHOLD = "my.fired.threshold";
	private static final String SYMBOL_LINK = "dict";	
	public int run(String[] allArgs) throws Exception {
	    //获取参数
		String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	    if(args==null||args.length<8){
	    	Scanner sc = new Scanner(System.in);
	    	args = new String[8];
	    	args[0] = sc.nextLine();//频繁序列目录
	    	args[1] = sc.nextLine();//输出目录
	    	args[2] = sc.nextLine();//日志文件目录
	    	args[3] = sc.nextLine();//gap
	    	args[4] = sc.nextLine();//ruleGap
	    	args[5] = sc.nextLine();//cardinality
	    	args[6] = sc.nextLine();//firedThreshold
	    	args[7] = sc.nextLine();//Dict文件
	    	Utils.deletePath(args[1]);
	    }
	    List<String> list = filesOfPath(new Path(args[2]),getConf());
	    int reducerNum = list.size();
	    //将日志目录传递到Reducer
	    //Reducer使用filesOfPath方法遍历目录，会得到同样的List<String>
	    getConf().set(LOG_PATH, args[2]);
	    //将reducerNum传递给Mapper，Mapper根据reducerNum生成key
	    getConf().set(REDUCE_NUM, reducerNum+"");
	    //将gap传递给Reducer
	    getConf().set(GAP, args[3]);
	    //将ruleGap传递给Reducer
	    getConf().set(RULE_GAP, args[4]);
	    //将cardinality传给Reducer
	    getConf().set(CARDINALITY, args[5]);
	    //将firedThreshold传给Reducer
	    getConf().set(FIRED_THRESHOLD, args[6]);
	    //分布式缓存字典文件
	    Path dictPath = new Path(args[7]);
	    List<String> filesPaths = RulesDiscoveryMR.filesOfPath(dictPath, getConf());
	    URI[] dict = new URI[filesPaths.size()];
	    for(int i=0;i<filesPaths.size();i++){
	    	dict[i] = new Path(filesPaths.get(i)).toUri();
	    }
	    
		Job job = Job.getInstance(getConf());
		//构造分布式缓存，符号链接默认就是开启(1.0版本的时候需要手动开启，Job.createSymlink())	
		job.setCacheFiles(dict);
	    job.setJarByClass(RulesDiscoveryMR.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    
	    job.setMapOutputKeyClass(IntWritable.class);//日志序号
	    job.setMapOutputValueClass(NoIndexSeqWritable.class);//频繁序列
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    job.setMapperClass(InnerMapper.class);
	    job.setReducerClass(InnerReducer.class);
	    job.setNumReduceTasks(reducerNum);
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.out.println("提交RulesDiscoveryMR任务");
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }	    
	}
	public static class InnerMapper extends Mapper<Object,Text,IntWritable,NoIndexSeqWritable>{
		private int reducer_num = 0;
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			this.reducer_num = Integer.parseInt(context.getConfiguration().get(REDUCE_NUM));
		}		
		@Override
        public void map(Object key,Text value, Context context) throws IOException, InterruptedException {		
			String[] s = value.toString().split(":");
			List<SeqMeta> list = new ArrayList<SeqMeta>();
			for(String t:s[0].split(",")){
				list.add(SeqMeta.getSeqMetaBySID(Long.parseLong(t)));
			}
			NoIndexSeqWritable nsw = new NoIndexSeqWritable(list);
			for(int i=0;i<reducer_num;i++){
				context.write(new IntWritable(i), nsw);
			}
		} 		
	}
	public static class InnerReducer extends Reducer<IntWritable,NoIndexSeqWritable,Text,Text>{
		private long gap = 0;
		private long ruleGap = 0;
		private int cardinality = 0;
		private double firedThreshold = 0.0;
		private List<String> pathList = null;
		private Map<String,Long> dictMap = null;
		protected void setup(Context context) throws IOException, InterruptedException{
			//获取配置信息
			this.gap = Long.parseLong(context.getConfiguration().get(GAP));
			this.ruleGap = Long.parseLong(context.getConfiguration().get(RULE_GAP));
			this.cardinality = Integer.parseInt(context.getConfiguration().get(CARDINALITY));
			this.firedThreshold = Double.parseDouble(context.getConfiguration().get(FIRED_THRESHOLD));
			String path = context.getConfiguration().get(LOG_PATH);
			this.pathList = filesOfPath(new Path(path),context.getConfiguration());
			//构造字典Map
			dictMap = new HashMap<String,Long>();
			long sid = 0;
			Path[] cacheFiles = Job.getInstance(context.getConfiguration()).getLocalCacheFiles();
			for(Path cache:cacheFiles){
//				通过输出信息，可以看到DistributedCache的缓存位置，文件名等信息。				
//				System.out.println(cache.toString());
//				System.out.println(cache.getName());				
				BufferedReader br = new BufferedReader(new FileReader(cache.getName()));
				String s = null;
				while((s=br.readLine())!=null){
					dictMap.put(s, sid++);
				}
				br.close();
			}		
		}		
		@Override
		public void reduce(IntWritable key, Iterable<NoIndexSeqWritable> values, Context context) throws IOException, InterruptedException{
			Path path = new Path(pathList.get(key.get()));
			TimeMeta[] logSeq = null;
			try {
				logSeq = getLogSeq(path,context.getConfiguration(),dictMap);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			if(logSeq==null)return;
			RulesGenerator rd = new RulesGenerator(logSeq,cardinality,gap,ruleGap,firedThreshold);
			for(NoIndexSeqWritable v:values){//对每一个频繁序列
				SeqMeta[] freSeq = new SeqMeta[v.seq.size()];
				for(int i=0;i<v.seq.size();i++)
					freSeq[i] = v.seq.get(i);
				int[] result = rd.scoreAllRules(freSeq);
				StringBuffer s = new StringBuffer();
				for(int i:result)
					s.append(i+",");
				if(s.length()>0)s.deleteCharAt(s.length()-1);
				//构造输出Key
				StringBuffer sb = new StringBuffer();
				for(SeqMeta m:v.seq){
					sb.append(m.getSID()+",");
				}
				if(sb.charAt(sb.length()-1)==',')
					sb.deleteCharAt(sb.length()-1);
				//将频繁序列freSeq的打分结果输出：<freSeq,打分>
				context.write(new Text(sb.toString()), new Text(s.toString()));
			}
        }		
	}
	private static final String DATE_STRING = "yyyy-MM-dd hh:mm:ss";
	/**
	 * 根据path指定的文件，获取日志序列<br />
	 * 日志文件格式：time+","+logContent<br />
	 * 2016-06-16 10:45:52,aaaaa
	 * @throws IOException 
	 * @throws ParseException 
	 * */
	private static TimeMeta[] getLogSeq(Path path,Configuration conf,Map<String,Long> dictMap) throws IOException, ParseException{
		//写完这里之后，再写一个MR，把<freSeq,打分>聚集一下，给出最终结果！
		List<TimeMeta> list = new ArrayList<TimeMeta>();
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_STRING);
	 	FileSystem fs = path.getFileSystem(conf);
		Scanner sc = new Scanner(fs.open(path));
		while(sc.hasNextLine()){	
			String x = sc.nextLine();
			if(x==null||x.equals("")||!x.contains(","))
				continue;
			String[] s = x.split(",");
			Long sid = dictMap.get(s[1]);
			Date date = sdf.parse(s[0]);
			if(sid!=null&&date!=null)
				list.add(new TimeMeta(sid,date.getTime()));
		}
		sc.close();
		TimeMeta[] result = new TimeMeta[list.size()];
		for(int i=0;i<result.length;i++)
			result[i] = list.get(i);
		return result;
	}
	/**
	 * 返回path包含的所有文件，按照递增序<br />
	 * 如果path是文件，返回list只包含path；如果path是目录，递归目录返回所有文件；
	 * @throws IOException 
	 * */
	public static List<String> filesOfPath(Path path,Configuration conf) throws IOException{
	    FileSystem fs = path.getFileSystem(conf);
	    FileStatus status = fs.getFileStatus(path);
	    List<String> filePath = new ArrayList<String>();
	    if(status.isDirectory()){
	    	for(FileStatus tmp:fs.listStatus(path)){
	    		if(tmp.isFile()){
	    			String fileName = tmp.getPath().getName(); 
	    			if(!fileName.matches("_.*")&&!fileName.matches("\\..*"))
	    				filePath.add(tmp.getPath().toString());
	    		}
	    		else
	    			filePath.addAll(filesOfPath(tmp.getPath(),conf));
	    	}
	    }
	    else if(status.isFile()){
			String fileName = status.getPath().getName(); 
			if(!fileName.matches("_.*")&&!fileName.matches("\\..*"))
				filePath.add(status.getPath().toString());

	    }	
	    Collections.sort(filePath);
	    return filePath;
	}
}
