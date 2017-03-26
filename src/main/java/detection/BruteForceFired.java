package detection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import utils.SIDMeta;
import utils.SeqMeta;
import utils.TimeMeta;
/**
 * 暴力搜索法
 * 根据输入日志序列在motif集合上fired情况，计算概率。
 * */
public abstract class BruteForceFired {
	public static void main(String args[]) throws IOException{//测试
		/**
		 * mTree内容：
			0,1,3:0,0.5,0
			0,1,4:1,0.3,1
			0,2:2,0.2,2 
		 * */
		List<MotifNode> mTree = createMotifTreeFromLog("/home/belan/Desktop/mTree");
		//firstSearch(mTree,0);
		List<TimeMeta> logSeq = new ArrayList<TimeMeta>();
		logSeq.add(new TimeMeta(0,0));
		logSeq.add(new TimeMeta(1,1));
		logSeq.add(new TimeMeta(3,2));
		logSeq.add(new TimeMeta(2,3));
		logSeq.add(new TimeMeta(4,4));
		System.out.println(new BRF1(mTree,10,1).scoreForLogSeq(logSeq));
		System.out.println(new BRF2(mTree,10,0.5).scoreForLogSeq(logSeq));
	}
	public static class MotifNode{
		SeqMeta meta;//起始点meta==null，其他点meta不为空
		Map<SeqMeta,Integer> sons;
		double[] score;
		public MotifNode(SeqMeta meta){
			this.meta = meta;
			score = null;
		}
	}
	private List<MotifNode> mTree = null;//MotifTree
	private long gap;
	protected double alpha;
	public BruteForceFired(List<MotifNode> mTree,long gap,double alpha){
		this.mTree = mTree;
		this.gap = gap;
		this.alpha = alpha;
	}
	//计算日志序列分类为+例的可能性打分
	public double scoreForLogSeq(List<TimeMeta> logSeq){
		List<Double> probList = new ArrayList<Double>();
		for(int start=0;start<logSeq.size();start++){
			List<Double> list = probability(logSeq,start,0);
			probList.addAll(list);
		}
		Collections.sort(probList);//递增序
		return marking(probList);
	}
	/**
	 * probList是递增序的1-FAR
	 * */
	public abstract double marking(List<Double> probList);
	/**
	 * 递归匹配<br />
	 * logSeq[i]从mTree[nodeIndex]出发(即从mTree[nodeIndex].sons开始匹配)，返回所有匹配成功节点对应的(1-FAR)值
	 * */
	public List<Double> probability(List<TimeMeta> logSeq,int i,int nodeIndex){
		List<Double> res = new ArrayList<Double>();
		MotifNode node = mTree.get(nodeIndex);
		if(node==null||node.sons==null||node.sons.get(logSeq.get(i))==null){
			return res;
		}
		int nextNodeIndex = node.sons.get(logSeq.get(i));
		node = mTree.get(nextNodeIndex);//获取匹配logSeq.get(i)的节点
		if(node!=null&&node.score!=null&&node.score.length>=3)
			res.add(1-node.score[1]);
		for(int j=i+1;j<logSeq.size();j++){
			if(logSeq.get(j).getTime()-logSeq.get(i).getTime()>0
					&&logSeq.get(j).getTime()-logSeq.get(i).getTime()<=gap){
				res.addAll(probability(logSeq,j,nextNodeIndex));
			}
			else
				break;
		}
		return res;
	}
	/**
	 * 从Motif文件构造MotifTree
	 * */
	public static List<MotifNode> createMotifTreeFromLog(String logPath) throws IOException{
		List<MotifNode> mTree = new ArrayList<MotifNode>();
		mTree.add(new MotifNode(null));//添加起始点
		BufferedReader br = new BufferedReader(new FileReader(logPath));
		String s = null;
		while((s=br.readLine())!=null){
			String[] split = s.split(":");
			SeqMeta[] smeta = deserializeSeqMeta(split[0]);
			double[] score = deserializeScore(split[1]);
			int nodeIndex = 0;
			int j = 0;
			for(j=0;j<smeta.length;j++){//add each SeqMeta to tree
				MotifNode curNode = mTree.get(nodeIndex);
				if(curNode.sons==null)
					curNode.sons = new HashMap<SeqMeta,Integer>();
				if(curNode.sons.get(smeta[j])==null){
					MotifNode newNode = new MotifNode(smeta[j]);
					mTree.add(newNode);
					curNode.sons.put(smeta[j],mTree.size()-1);
					nodeIndex = mTree.size()-1;
				}
				else
					nodeIndex = curNode.sons.get(smeta[j]);
			}
			mTree.get(nodeIndex).score = score;
		}
		br.close();
		return mTree;
	}
	private static void firstSearch(List<MotifNode> mTree,int i){
		MotifNode node = mTree.get(i);
		if(node.meta==null)System.out.print("null"+":");
		else System.out.print(node.meta+":");
		if(node.score==null)System.out.println();
		else {
			String s = "";
			for(double d:node.score)
				s+=","+d;
			System.out.println(s);
		}
		if(node.sons!=null){
			for(Entry<SeqMeta, Integer> entry:node.sons.entrySet()){
				firstSearch(mTree,entry.getValue());
			}
		}
	}
	//字符串"POD,FAR,CSI"->double[] POD,FAR,CSI
	public static double[] deserializeScore(String s){
		if(s==null)return null;
		String[] split = s.split(",");
		if(split==null||split.length<3)return null;
		double[] res = new double[3];
		res[0] = Double.parseDouble(split[0]);
		res[1] = Double.parseDouble(split[1]);
		res[2] = Double.parseDouble(split[2]);
		return res;
	}
	//字符串"sid1,sid2,sid3,..."->SeqMeta[]
	public static SeqMeta[] deserializeSeqMeta(String s){
		if(s==null)return null;
		String[] split = s.split(",");
		SeqMeta[] mArray = new SeqMeta[split.length];
		for(int i=0;i<split.length;i++)
			mArray[i] = SeqMeta.getSeqMetaBySID(Long.parseLong(split[i]));
		return mArray;
	}
	//字符串"sid1,sid2,sid3,..."->List<SeqMeta>
	public static List<SeqMeta> deserializeSeqMetaToArray(String s){
		if(s==null)return null;
		String[] split = s.split(",");
		List<SeqMeta> mList = new ArrayList<SeqMeta>();
		for(int i=0;i<split.length;i++)
			mList.add(SeqMeta.getSeqMetaBySID(Long.parseLong(split[i])));
		return mList;
	}	
}
