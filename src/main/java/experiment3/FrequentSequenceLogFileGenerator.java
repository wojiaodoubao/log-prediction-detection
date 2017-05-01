package experiment3;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * 1.根据给定的文件日志条数，文件数，支持度，gap，频繁模式，字符集，随机发生测试日志。
 * 2.测试日志中包含满足gap&支持度的频繁模式，和随机发生的其他日志。
 * 3.实验期望：
 * 1）挖掘算法应该能从中挖掘出预设的频繁模式；
 * 2）随着支持度m增大，文件数n增多，挖掘出的频繁模式数量减少，能看出收敛向预设频繁模式的趋势。 
 * */
public class FrequentSequenceLogFileGenerator {
	public static void main(String args[]) throws IOException{
		int logSize = 20;
		int n = 4;
		int m = 4;//支持度
		long gap = 6000;
		long increaseGap = 1000;
		String[] charSet = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		String[] frequentSeq = {"a","b","c","a","d","e"};
//		String[] frequentSeq = {};
		String[] randomSeq = {};
		String dir = "/home/belan/Desktop/实验三数据-预测实验/support-"+m+"-"+n;
		//产生m个文件，包含frequentSeq
		new FrequentSequenceLogFileGenerator(logSize,m,0,gap,increaseGap,charSet,frequentSeq)
			.generate(dir);
		//产生n-m个随机日志文件，不一定包含frequentSeq
		new FrequentSequenceLogFileGenerator(logSize,n-m,m,gap,increaseGap,charSet,randomSeq)
			.generate(dir);
	}
	private int logSize;//日志条数
	private int n;//文件数
	private int fileIndex;//文件编号起始
	private long gap;//gap
	private long increaseGap;//每两个日志间gap值
	private String[] charSet;//字符集
	private String[] frequentSeq;//预设频繁模式
	public FrequentSequenceLogFileGenerator(int logSize,int n,int fileIndex,
			long gap,long increaseGap,String[] charSet,String[] frequentSeq){
		this.logSize = logSize;
		this.n = n;
		this.fileIndex = fileIndex;
		this.gap = gap;
		this.increaseGap = increaseGap;
		this.charSet = charSet;
		this.frequentSeq = frequentSeq;
	}
	public void generate(String dir) throws IOException{
		SimpleDateFormat sdf = new SimpleDateFormat(utils.StaticInfo.DATE_STRING);
		for(int i=0;i<n;i++){
			//create log file
			String filePath = dir
					+System.getProperty("file.separator")
					+"log"+(fileIndex+i);
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
			int freSeqIndex = 0;//下一个要写的freSeq的索引
			long lastFreSeqTimeStamp = -1;
			for(int j=0;j<logSize||freSeqIndex<frequentSeq.length;j++){
				sb.setLength(0);//clear sb
				if(logSize-j<=this.frequentSeq.length-freSeqIndex){//剩余日志记录数不足，追加频繁模式
					sb.append(sdf.format(new Date(curTime))).append(",").append(frequentSeq[freSeqIndex++]);
				}
				else if(freSeqIndex<frequentSeq.length&&
						((lastFreSeqTimeStamp>0&&curTime+increaseGap-lastFreSeqTimeStamp>gap)||
						Math.abs(rd.nextInt())%2==0)){//频繁序列模式没写完 and (下一个必须是freSeq元素 or 随机数是偶数),追加频繁模式
					sb.append(sdf.format(new Date(curTime))).append(",").append(frequentSeq[freSeqIndex++]);
					lastFreSeqTimeStamp = curTime;
				}
				else{//追加随机日志
					sb.append(sdf.format(new Date(curTime))).append(",").append(charSet[Math.abs(rd.nextInt())%charSet.length]);
				}
				pw.println(sb);
				curTime += increaseGap;
			}
			pw.close();
		}		
	}
}
