package prediction;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 基于DataPreprocessing的输出文件
 * 根据日志内容从小到大排序，并为每条日志分配一个ID(long)，输出<ID-日志>映射表(日志按照ID从0到xxx排列，一行一个)。
 * 
 * 改进之处：现在为了全排序使用的One-Reducer法
 * 可以用Partition分一下从而多Reducer排序，但那样就需要对index大小有一个比较好的估计，否则一个多剩下的少，反而引入了很多无用Reducer输出。
 * */
public class LogToMeta  extends Configured implements Tool{
	public static void main(String args[]) throws Exception{
		ToolRunner.run(new LogToMeta(), args);//ToolRunner.run()方法中为Configuration赋了初值。		
	}
	public int run(String[] allArgs) throws Exception {
		Job job = Job.getInstance(getConf());
	    job.setJarByClass(LogToMeta.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    	    
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);

	    job.setMapperClass(MyMapper.class);
	    job.setReducerClass(MyReducer.class);
	    
	    job.setNumReduceTasks(1);//为了全排序
	    
	    String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	    if(args==null||args.length<2){
	    	Scanner sc = new Scanner(System.in);
	    	args = new String[2];
	    	args[0] = sc.nextLine();
	    	args[1] = sc.nextLine();
	    	File f = new File(args[1]);
	    	if(f.exists()){
	    		boolean label = f.delete();
	    		System.out.println("文件删除:"+label);
	    	}
	    }
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));   
	    System.out.println("提交LogToMeta任务");
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }		
	}
	public static class MyMapper extends Mapper<Object,Text,IntWritable,Text>{
		@Override
        public void map(Object key,Text value, Context context) throws IOException, InterruptedException {
			String s = value.toString();
			int index = s.indexOf(DataPreprocessing.SPLIT);
			if(index<0)return;
			context.write(new IntWritable(s.length()-1-index), new Text(s.substring(0,index)));
		} 		
	}
	private static class MyReducer extends Reducer<IntWritable,Text,Text,NullWritable>{
        @Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        	int k = key.get();
        	for(Text value:values){
        		context.write(value, NullWritable.get());
        	}
        }		
	}
}
