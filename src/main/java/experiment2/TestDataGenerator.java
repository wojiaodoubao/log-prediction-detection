package experiment2;

import java.util.*;
import java.io.*;

public class TestDataGenerator {
	public static final String directory = "/home/belan/Desktop/实验二数据";
	public static final long SLEEP_TIME = 1;
	public static final boolean SEQ_DATA_SWITCH = true; 
	public static final boolean MOTIF_SWITCH = false;	
	public static void main(String args[]) throws IOException, InterruptedException{
		int sidThreshold = 10;//meta数量
		int seqLength = 1000000;//seq长度
		int motifThreshold = 10;//每个motif长度上限
		int motifLength = 100;//motif数量		
		new TestDataGenerator(sidThreshold,seqLength,motifThreshold,motifLength).generate(directory);
	}
	private long sidThreshold;
	private int seqLength;
	private int motifThreshold;//树高阈值
	private int motifLength;
	public TestDataGenerator(long sidThreshold,int seqLength,int motifThreshold,int motifLength){
		this.sidThreshold = sidThreshold;
		this.seqLength = seqLength;
		this.motifThreshold = motifThreshold;
		this.motifLength = motifLength;
	}	
	public void generate(String directory) throws IOException, InterruptedException{
		File logFile = null;
		PrintWriter pw = null;
		if(TestDataGenerator.SEQ_DATA_SWITCH){
			//产生序列数据
			//create file
			String seqdata = directory+"/seq-data";
			logFile = new File(seqdata);
			if(logFile.exists()){
				System.out.println(seqdata+" already exists!");
				return;
			}
			logFile.createNewFile();		
			//generate seq-data
			pw = new PrintWriter(logFile);
			for(int i=0;i<seqLength;i++){
				Random rd = new Random(new Date().getTime());
				pw.println(Math.abs(rd.nextInt())%sidThreshold+":"+i);
				Thread.sleep(SLEEP_TIME);
			}
			pw.close();
		}
		if(TestDataGenerator.MOTIF_SWITCH){
			//产生motif数据
			//create file
			String motif = directory+"/motif";
			logFile = new File(motif);
			if(logFile.exists()){
				System.out.println(motif+" already exists!");
				return;
			}
			logFile.createNewFile();		
			//generate motif-data
			Set<List<Long>> motifs = new HashSet<List<Long>>();
			pw = new PrintWriter(logFile);
			for(int i=0;i<motifLength;i++){
				List<Long> list = new ArrayList<Long>();
				Random rd = new Random(new Date().getTime());
				int size = Math.abs(Math.abs(rd.nextInt()))%this.motifThreshold+1;
				size = motifThreshold;//全部motif均长度为motifThreshold
				for(int j=0;j<size;j++){
					list.add(Math.abs(rd.nextInt())%sidThreshold);
				}
				boolean flag = true;
				for(List<Long> m:motifs){
					if(TestDataGenerator.isSubSeq(list, m)){
						flag = false;
						break;
					}
				}
				if(flag){
					StringBuffer sb = new StringBuffer();
					for(int j=0;j<list.size();j++){
						if(j==list.size()-1){
							sb.append(list.get(j)+":");
						}
						else{
							sb.append(list.get(j)+",");
						}
					}
					motifs.add(list);
					sb.append("0,0,0");
					pw.println(sb.toString());
				}
				else
					i--;
				Thread.sleep(SLEEP_TIME);
			}
			pw.close();
		}
	}
	/**
	 * a是否是b的子序列
	 * */
	public static boolean isSubSeq(List<Long> a,List<Long> b){
		for(int i=0;i<b.size();i++){
			if(b.size()-i<a.size())return false;
			if(b.get(i)==a.get(0)){
				int j = 0;
				while(j<a.size()){
					if(a.get(j)!=b.get(i+j))break;						
					j++;
				}
				if(j==a.size())
					return true;
			}
		}
		return false;
	}
}
