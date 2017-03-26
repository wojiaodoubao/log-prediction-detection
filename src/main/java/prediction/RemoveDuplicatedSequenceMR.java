package prediction;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.LogWritable;

/**
 * 去除频繁序列挖掘结果中的重复子序列
 * 输入数据：
 * 频繁序列挖掘结果，一行一个频繁序列
 * 输出：
 * 去重后的频繁序列挖掘结果。去除所有重复子序列。
 * */
public class RemoveDuplicatedSequenceMR extends Configured implements Tool{
	public static void main(String args[]) throws Exception{
		ToolRunner.run(new RemoveDuplicatedSequenceMR(), args);//ToolRunner.run()方法中为Configuration赋了初值。	
	}
	public int run(String[] args) throws Exception {
		
		return 0;
	}	
	public static class RemoveDuplicateMapper extends Mapper<Text,Text,LogWritable,Text>{
		@Override
        public void map(Text key,Text value, Context context) throws IOException, InterruptedException {
			
        }		
	}
	private static class RemoveDuplicateReducer extends Reducer<LogWritable,Text,Text,Text>{
        @Override
		public void reduce(LogWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        }		
	}	
}
