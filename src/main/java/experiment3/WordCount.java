package experiment3;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import log.LogJobRunner;
import log.LogMapper;
import log.LogReducer;

public class WordCount extends Configured implements Tool{
	public static void main(String args[]) throws Exception{
		ToolRunner.run(new WordCount(), args);
	}
	
	public int run(String[] Args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(WordCount.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		String args[] = new GenericOptionsParser(getConf(),Args).getRemainingArgs();
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean status = new LogJobRunner(job).waitForCompletion(true);
		if(status)
			return 0;
		return 1;		
	}
	
	private static class MyMapper extends LogMapper<LongWritable,Text,Text,NullWritable>{
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			super.map(key, value, context);
			String[] s = value.toString().split(" ");
			for(String v:s)
				context.write(new Text(v), NullWritable.get());
		}		
	}
	
	private static class MyReducer extends LogReducer<Text,NullWritable,Text,Text>{
		@Override
		public void reduce(Text key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException{
			super.reduce(key, values, context);
			int sum = 0;
			for(NullWritable v:values)
				sum++;
			context.write(key, new Text(""+sum));
			System.out.println(key+" "+sum);
		}		
	}
	
}
