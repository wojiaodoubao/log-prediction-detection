package experiment;
import java.util.*;

import utils.StaticInfo;

import java.io.*;
import java.text.SimpleDateFormat;

public class TestDataGenerator {
	public static void main(String args[]) throws IOException{
		if(args!=null&&args.length>0){
// /home/lijinglun/TimeSeries/experiment-30000000/input 10000000 1000 7000 10 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
			String directory = args[0];
			int num = Integer.parseInt(args[1]);
			int logSum = Integer.parseInt(args[2]);
			int[] logNum = new int[logSum];
			for(int i=0;i<logSum;i++)
				logNum[i] = num;
			long adjacentGap = Long.parseLong(args[3]);
			long miningGap = Long.parseLong(args[4]);
			int maxLengthOfFrequentSequence = Integer.parseInt(args[5]);
			List<String> logContent = new ArrayList<String>();
			for(int i=5;i<args.length;i++){
				logContent.add(args[i]);
			}
			TestDataGenerator tdg = new TestDataGenerator(logNum,miningGap,adjacentGap,maxLengthOfFrequentSequence
					,logContent,StaticInfo.DATE_STRING);
			tdg.generate(directory);			
		}
		else{
			String directory = "/home/belan/Desktop/实验一数据/30000000-1000-7000-10";
			int[] logNum = {10000000,10000000,10000000};
			long adjacentGap = 1000;
			long miningGap = 7000;
			int maxLengthOfFrequentSequence = 10;
			List<String> logContent = new ArrayList<String>();
			logContent.add("aaaaaaaaaa");
			logContent.add("bbbbbbbbbb");
			logContent.add("cccccccccc");
			logContent.add("dddddddddd");
			logContent.add("eeeeeeeeee");
			TestDataGenerator tdg = new TestDataGenerator(logNum,miningGap,adjacentGap,maxLengthOfFrequentSequence
					,logContent,StaticInfo.DATE_STRING);
			tdg.generate(directory);
		}
	}
	private int[] logNum;//每个日志文件的日志条数
	private long adjacentGap;//相邻两条日志的gap值
	private long overGap;//超过挖掘使用的gap值
	private int maxLengthOfFrequentSequence;//最长频繁序列长度，会每这么长就将下一条日志时间戳跳过miningGap
	private List<String> logContent;//日志内容集合
	private String dateFormatString; 
	public TestDataGenerator(int[] logNum,long overGap,long adjacentGap
			,int maxLengthOfFrequentSequence
			,List<String> logContent,String dateFormatString){
		this.logNum = logNum;
		this.overGap = overGap;
		this.adjacentGap = adjacentGap;
		this.maxLengthOfFrequentSequence = maxLengthOfFrequentSequence;
		this.logContent = logContent;
		this.dateFormatString = dateFormatString;
	}
	public void generate(String directory) throws IOException{
		SimpleDateFormat sdf = new SimpleDateFormat(this.dateFormatString);
		for(int i=0;i<logNum.length;i++){
			//create log file
			int recordNum = logNum[i];
			String filePath = directory
					+System.getProperty("file.separator")
					+"log"+i+"-"+recordNum;
			File logFile = new File(filePath);
			if(logFile.exists()){
				System.out.println(filePath+" already exists!");
				return;
			}
			logFile.createNewFile();
			//write log file
			PrintWriter pw = new PrintWriter(logFile);
			StringBuffer sb = new StringBuffer();
			long curTime = new Date().getTime();
			Random rd = new Random(curTime);
			for(int j=0;j<recordNum;j++){
				sb.setLength(0);//clear sb
				sb.append(sdf.format(new Date(curTime)));
				sb.append(",");
				sb.append(this.logContent.get(rd.nextInt(this.logContent.size())));
				pw.println(sb);
				if((j+1)%this.maxLengthOfFrequentSequence==0)
					curTime+=this.overGap;
				else
					curTime+=this.adjacentGap;
			}
			pw.close();
		}
	}
}
