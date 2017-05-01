package experiment3;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class MotifLogFileGenerator {
	public static void main(String args[]) throws IOException{
		int logSize = 20;
		int n = 2048;//正例个数
		int m = 2048;//负例个数
		long gap = 6000;
		long increaseGap = 1000;
		String[] charSet = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r"};
		String[] motif = {"a","b","c","a","d","e"};
		String[] randomSeq = {};
		String dir = "/home/belan/Desktop/实验三数据-侦测实验/pn-"+n+"-"+m;
		//产生n个文件，包含motif
		new MotifLogFileGenerator(logSize,n,0,gap,increaseGap,charSet,motif)
			.generate(dir);
		//产生m个随机日志文件，不一定包含motif
		new MotifLogFileGenerator(logSize,m,n,gap,increaseGap,charSet,randomSeq)
			.generate(dir);
		StringBuilder sb = new StringBuilder();
		sb.append("/home/belan/Desktop/实验三数据-侦测实验/pn-"+n+"-"+m+"\n")
		.append("/home/belan/Desktop/Out3-detection/pn-"+n+"-"+m+"\n")
		.append("6000\n")
		.append("1\n")
		.append("0.5\n");		
		for(int i=0;i<n+m;i++){
			if(i<n)sb.append("log"+i+",+;");
			else sb.append("log"+i+",-;");
		}		
		System.out.println(sb);
	}
	private int logSize;//日志条数
	private int n;//文件数
	private int fileIndex;
	private long gap;//gap
	private long increaseGap;//每两个日志间gap值
	private String[] charSet;//字符集
	private String[] motif;//预设motif	
	public MotifLogFileGenerator(int logSize,int n,int fileIndex,
			long gap,long increaseGap,String[] charSet,String[] motif){
		this.logSize = logSize;
		this.n = n;
		this.fileIndex = fileIndex;
		this.gap = gap;
		this.increaseGap = increaseGap;
		this.charSet = charSet;
		this.motif = motif;
	} 
	public void generate(String dir) throws IOException{
		SimpleDateFormat sdf = new SimpleDateFormat(utils.StaticInfo.DATE_STRING);
		//create motif log file
		for(int i=0;i<n;i++){
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
			int motifIndex = 0;//下一个要写的motif的索引
			long lastMotifTimeStamp = -1;
			for(int j=0;j<logSize||motifIndex<motif.length;j++){
				sb.setLength(0);//clear sb
				if(logSize-j<=this.motif.length-motifIndex){//剩余日志记录数不足，追加频繁模式
					sb.append(sdf.format(new Date(curTime))).append(",").append(motif[motifIndex++]);
				}
				else if(motifIndex<motif.length&&
						((lastMotifTimeStamp>0&&curTime+increaseGap-lastMotifTimeStamp>gap)||
						Math.abs(rd.nextInt())%2==0)){//频繁序列模式没写完 and (下一个必须是freSeq元素 or 随机数是偶数),追加频繁模式
					sb.append(sdf.format(new Date(curTime))).append(",").append(motif[motifIndex++]);
					lastMotifTimeStamp = curTime;
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
