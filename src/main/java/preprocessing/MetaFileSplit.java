package preprocessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.SeqMeta;
import utils.SeqWritable;
import utils.Utils;
import utils.SeqWritable.Index;

public class MetaFileSplit extends Configured implements Tool{
	public static void main(String args[]) throws Exception{
		ToolRunner.run(new MetaFileSplit(), args);//ToolRunner.run()方法中为Configuration赋了初值。	
	}
	private static final String SYMBOL_LINK = "dict";
	public int run(String[] allArgs) throws Exception {		
	    //获取参数
		String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	    if(args==null||args.length<3){
	    	Scanner sc = new Scanner(System.in);
	    	args = new String[3];
	    	args[0] = sc.nextLine();
	    	args[1] = sc.nextLine();
	    	args[2] = sc.nextLine();//缓存字典
	    	Utils.deletePath(args[1]);
	    }		
	    URI dictPath = new URI(args[2]+"#"+SYMBOL_LINK);
	    
		Job job = Job.getInstance(getConf());
		//构造分布式缓存，符号链接默认就是开启(1.0版本的时候需要手动开启，Job.createSymlink())
		job.addCacheFile(dictPath);
	    job.setJarByClass(LogToMeta.class);
//	    job.setInputFormatClass(FullFileTextInputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);

	    job.setMapperClass(MyMapper.class);
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.out.println("提交MetaFileSplit任务");
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }
	}
	public static class MyMapper extends Mapper<Object,Text,Text,NullWritable>{
		private long sid = 0;
		private Map<String,Long> dictMap = null;
		private List<String> dictList = null;
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			//构造字典Map
			dictMap = new HashMap<String,Long>();
			dictList = new ArrayList<String>();
			BufferedReader br = new BufferedReader(new FileReader(SYMBOL_LINK));
			String s = null;
			while((s=br.readLine())!=null){
				dictList.add(s);
				dictMap.put(s, sid++);
			}
			br.close();
		}	
		private SimpleDateFormat sdf = new SimpleDateFormat(utils.StaticInfo.DATE_STRING);
		@Override
        public void map(Object key,Text value, Context context) throws IOException, InterruptedException {
			StringBuffer s = new StringBuffer(value.toString());
			int index = s.indexOf(SeqWritable.SID_LOG_SPLIT);
			if(index<0)return;
			Long logMeta = dictMap.get(s.substring(0, index));
			if(logMeta==null)return;
			Text outK = new Text(logMeta+s.substring(index,s.length()));
			context.write(outK, NullWritable.get());
		} 		
	}
}