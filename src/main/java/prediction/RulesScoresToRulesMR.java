package prediction;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.Utils;
/**
 * 输入：规则打分结果集
 * 文件格式：
 * 逗号分割序列+'\t'+逗号分割分数组
 * 1	-2147483648
 * 4,1	-2147483648,-2
 * 2,4,3	-2147483648,-4,-2
 * 输出：最终规则
 * */
public class RulesScoresToRulesMR  extends Configured implements Tool{
	public static void main(String args[]) throws Exception{
		ToolRunner.run(new RulesScoresToRulesMR(), args);//ToolRunner.run()方法中为Configuration赋了初值。
	}
	public int run(String[] allArgs) throws Exception {
	    //获取参数
		String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	    if(args==null||args.length<2){
	    	Scanner sc = new Scanner(System.in);
	    	args = new String[2];
	    	args[0] = sc.nextLine();//规则打分目录
	    	args[1] = sc.nextLine();//输出目录
	    	Utils.deletePath(args[1]);
	    }

		Job job = Job.getInstance(getConf());
	    job.setJarByClass(RulesScoresToRulesMR.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    
	    job.setMapOutputKeyClass(Text.class);//序列
	    job.setMapOutputValueClass(Text.class);//打分结果
	    job.setOutputKeyClass(Text.class);//规则
	    job.setOutputValueClass(NullWritable.class);

	    job.setMapperClass(InnerMapper.class);
	    job.setReducerClass(InnerReducer.class);
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.out.println("提交RulesScoresToRulesMR任务");
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }	    
	}
	public static class InnerMapper extends Mapper<Object,Text,Text,Text>{	
		@Override
        public void map(Object key,Text value, Context context) throws IOException, InterruptedException {		
			String[] s = value.toString().split("\t");
			if(s!=null&&s.length>=2)
				context.write(new Text(s[0]), new Text(s[1]));
		} 		
	}
	public static class InnerReducer extends Reducer<Text,Text,Text,NullWritable>{	
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String[] k = key.toString().split(",");
			int[] score = new int[k.length];
			for(Text v:values){
				int i = 0;
				for(String s:v.toString().split(",")){
					if(i==0&&score[i]>=0)
						score[i] = Integer.MIN_VALUE;
					else{
						score[i] += Integer.parseInt(s);
					}
					i++;
				}
			}
			int maxIndex = 0;
			for(int i=0;i<score.length;i++){
				if(score[i]>score[maxIndex])
					maxIndex = i;
			}
			StringBuffer sb = new StringBuffer();
			int i =0;
			for(i=0;i<maxIndex;i++)
				sb.append(k[i]+",");
			if(sb.length()>1&&sb.charAt(sb.length()-1)==',')sb.deleteCharAt(sb.length()-1);
			sb.append("->");
			for(;i<k.length;i++)
				sb.append(k[i]+",");
			if(sb.length()>1&&sb.charAt(sb.length()-1)==',')sb.deleteCharAt(sb.length()-1);
			context.write(new Text(sb.toString()), NullWritable.get());
        }
	}	
}
