package prediction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.NoIndexSeqWritable;
import utils.SeqMeta;
import utils.Utils;

/**
 * 通用方法：去除频繁序列挖掘结果中的重复子序列<br />
 * <b>基于依次两个去一个的剪枝法(去除收尾)，存在无法去除的情况，如abcde中无法去除ace。</b>
 * 输入数据：
 * 频繁序列挖掘结果，一行一个频繁序列<br />
 * 输出：
 * 去重后的频繁序列挖掘结果。去除所有重复子序列。
 * 
/home/belan/Desktop/Out2/Sequences
/home/belan/Desktop/Out2/NNNSequences
 * */
public class RemoveDuplicatedSequenceMR extends Configured implements Tool{
	public static void main(String args[]) throws Exception{
		ToolRunner.run(new RemoveDuplicatedSequenceMR(), args);//ToolRunner.run()方法中为Configuration赋了初值。	
	}
	public int run(String[] allArgs) throws Exception {		
		Job job = Job.getInstance(getConf());
	    job.setJarByClass(RemoveDuplicatedSequenceMR.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(NoIndexSeqWritable.class);
	    job.setOutputValueClass(NullWritable.class);

	    //job.setMapperClass(AsciiMapper.class);
	    job.setMapperClass(RemoveDuplicateMapper.class);
	    job.setReducerClass(RemoveDuplicateReducer.class);
	    
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
	    System.out.println("提交任务RemoveDuplicatedSequenceMR");
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }
	}
	
	private static class SortComparator extends WritableComparator{
		public SortComparator(){
			super(Text.class,true);//反射构造为LogWritable对象
		}
		//完全排序
		@Override
		public int compare(WritableComparable obj1,WritableComparable obj2){
			Text first = (Text)obj1;
			Text second = (Text)obj2;
			if(first.equals(second))return 0;
			String[] sf = first.toString().split("\t");
			String[] ss = second.toString().split("\t");
			if(sf[0].equals(ss[0])){
				if(sf[1].equals("son"))return -1;
				else return 1;
			}
			else//字典序
				return first.compareTo(second);
		}
	}
	
	private static class GroupComparator extends WritableComparator{
		public GroupComparator(){
			super(Text.class,true);
		}		
		//部分排序
		@Override
		public int compare(WritableComparable obj1,WritableComparable obj2){
			Text first = (Text)obj1;
			Text second = (Text)obj2;
			int sf = Integer.parseInt(first.toString().split("\t")[0]);
			int ss = Integer.parseInt(second.toString().split("\t")[0]);
			if(sf==ss)return 0;
			else if(sf<ss)return -1;
			else return 1;
		}		
	}	
	
	public static class RemoveDuplicateMapper extends Mapper<LongWritable,Text,Text,Text>{
		private NoIndexSeqWritable nsw = new NoIndexSeqWritable();
		private Set<NoIndexSeqWritable> sonSeqSet = new HashSet<NoIndexSeqWritable>();
		@Override
        public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
			NoIndexSeqWritable.createFromString(nsw, value.toString());
			context.write(new Text(nsw.seq.size()+"\tseq"), new Text("seq\n"+nsw));
			//compute all subsequences of nsw
			Set<NoIndexSeqWritable> levelSet = new HashSet<NoIndexSeqWritable>();
			levelSet.add(nsw);
			while(levelSet.size()>0){
				Set<NoIndexSeqWritable> nextlevelSet = new HashSet<NoIndexSeqWritable>();
				for(NoIndexSeqWritable nw:levelSet){
					NoIndexSeqWritable son = sonSeqCopyWithoutIndex(nw,0);
					if(son.seq.size()>0&&!sonSeqSet.contains(son))
						nextlevelSet.add(son);
					son = sonSeqCopyWithoutIndex(nw,nw.seq.size()-1);
					if(son.seq.size()>0&&!sonSeqSet.contains(son))
						nextlevelSet.add(son);					
				}
				sonSeqSet.addAll(nextlevelSet);
				levelSet = nextlevelSet;
			}
			for(NoIndexSeqWritable nw:sonSeqSet){
				context.write(new Text(nw.seq.size()+"\tson"), new Text("son\n"+nw));
			}
        }
		private NoIndexSeqWritable sonSeqCopyWithoutIndex(NoIndexSeqWritable nsw,int i){
			List<SeqMeta> list = new ArrayList<SeqMeta>();
			for(int j=0;j<nsw.seq.size();j++){
				if(j!=i)
					list.add(nsw.seq.get(j));
			}
			return new NoIndexSeqWritable(list);
		}
	}
	private static class RemoveDuplicateReducer extends Reducer<Text,Text,NoIndexSeqWritable,NullWritable>{
        @Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        	Set<NoIndexSeqWritable> sonSet = new HashSet<NoIndexSeqWritable>();
        	NoIndexSeqWritable nsw = new NoIndexSeqWritable();
        	for(Text value:values){
            	String[] s = value.toString().split("\n");
            	if(s[0].equals("son"))
            		sonSet.add(NoIndexSeqWritable.createFromString(s[1]));
            	else{
            		NoIndexSeqWritable.createFromString(nsw, s[1]);
            		if(!sonSet.contains(nsw))
            			context.write(nsw, NullWritable.get());
            	}
        	}
        }		
	}	
}
