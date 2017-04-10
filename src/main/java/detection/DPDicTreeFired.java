package detection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import utils.SeqMeta;
import utils.TimeMeta;

/**
 * 基于字典树和动态规划的序列匹配算法
 * */
public abstract class DPDicTreeFired {
	public static class DDNode{//ddTree(Dynamic Programming Dictionary Tree)的树节点
		public SeqMeta meta;//起始点meta==null，其他点meta不为空
		public int father;//起始点father==-1，其余点father>=0
		public Map<SeqMeta,Integer> sons;
		public double[] score;
		public DDNode(SeqMeta meta,int father,double[] score){
			this.meta = meta;
			this.father = father;
			this.score = score;
			this.sons = new HashMap<SeqMeta,Integer>();
		}		
	}
	public static class MatchInfo{//表示匹配了ddTree[startNode]->...->ddTree[endNode]；树任意两点间有且只有一条路
		public int startNode;
		public int endNode;
		public MatchInfo(int startNode,int endNode){
			this.startNode = startNode; 
			this.endNode = endNode;
		}
		@Override
		public boolean equals(Object obj){
			if(obj == this)return true;
			else if(obj instanceof MatchInfo){
				MatchInfo mi = (MatchInfo)obj;
				if(this.startNode == mi.startNode&&this.endNode == mi.endNode)
					return true;
			}
			return false;
		}
		@Override
		public int hashCode(){
			return this.startNode+this.endNode;
		}
	}
	public static class DPBlock{//待匹配序列TimeMeta的DP信息块
		public Map<MatchInfo,Integer> matchInfos = new HashMap<MatchInfo,Integer>();//匹配信息-匹配次数
		public int seqIndex;//此DPBlock对应的TimeMeta在序列上的索引
		public DPBlock(int seqIndex){
			this.seqIndex = seqIndex;
		}
		@Override
		public String toString(){
			StringBuffer s = new StringBuffer();
			for(Entry<MatchInfo, Integer> entry:matchInfos.entrySet()){
				s.append(entry.getKey().startNode+"->"+entry.getKey().endNode+":"+entry.getValue());
			}
			return s.toString();
		}
	}
	public static class QueueNode{//队列节点
		public DPBlock block = null;
		public QueueNode next = null;
		public QueueNode(DPBlock block){
			this.block = block;
		}
	}
	
	private List<DDNode> ddTree = null;//motif序列集字典树
	private List<Integer> leaves = null;//ddTree的全部叶子节点
	private long gap;//序列时间戳gap约束
	protected double alpha;//概率系数

	public DPDicTreeFired(List<DDNode> ddTree,long gap,double alpha){
		this.ddTree = ddTree;
		this.gap = gap;
		this.alpha = alpha;
		this.leaves = new ArrayList<Integer>();
		getLeavesFromDDTree(0);
	}	
	/**
	 * 构造this.leaves，计算ddTree的所有叶子节点
	 * */
	private void getLeavesFromDDTree(int i){
		DDNode node = ddTree.get(i);
		if(node==null)
			return;
		else if(node.sons!=null&&node.sons.size()>0){
			for(Entry<SeqMeta, Integer> entry:node.sons.entrySet()){
				getLeavesFromDDTree(entry.getValue());
			}
		}		
		else if(node.score!=null){
			this.leaves.add(i);
		}		
	}
	/**
	 * 计算日志序列logSeq分类为+例的概率
	 * */
	public double scoreForLogSeq(List<TimeMeta> logSeq){
		List<Double> list = dpFired(logSeq);
		Collections.sort(list);//递增序
//		System.out.println(list.size());
//		for(Double d:list)
//			System.out.println(d);
		return marking(list);
	}
	/**
	 * probList是递增序的1-FAR
	 * */
	public abstract double marking(List<Double> probList);
	/**
	 * DP计算logSeq在ddTree上的fired结果，每次fired就将fired motif的1-FAR值加入List<Double></br>
	 * 从后向前扫描，queue保存满足gap约束的logSeq上meta的DPBlock；logSeq上每个meta根据queue和ddTree叶子节点产生自身DPBlock
	 * */
	public List<Double> dpFired(List<TimeMeta> logSeq){
		//DP计算fired结果
		List<Double> result = new ArrayList<Double>();
		QueueNode queue = new QueueNode(null);//DP队列，队列满足按照时间戳从小到大排列
		for(int i=logSeq.size()-1;i>=0;i--){//从后向前DP
			DPBlock curBlock = new DPBlock(i);
			//对每一个叶子节点，计算MatchInfo
			for(int j:leaves){
				DDNode leaf = ddTree.get(j);
				if(leaf.meta.equals(logSeq.get(i))){//add to DPBlock				
					MatchInfo mif = new MatchInfo(j,j);
					Integer times = curBlock.matchInfos.get(mif);
					if(times==null)times=0;
					curBlock.matchInfos.put(mif, times+1);
				}
			}
			//枚举queue，计算MatchInfo
			QueueNode node = queue;
			while(node.next!=null){
				DPBlock queBlock = node.next.block;
				if(logSeq.get(queBlock.seqIndex).getTime()-logSeq.get(i).getTime()>gap
						||logSeq.get(queBlock.seqIndex).getTime()-logSeq.get(i).getTime()<0){//不满足gap约束，则node.next开始全部截掉
					node.next = null;
				}				
				else{//满足gap约束，计算MatchInfo
					for(Entry<MatchInfo, Integer> entry:queBlock.matchInfos.entrySet()){
						int father = ddTree.get(entry.getKey().startNode).father;
						if(ddTree.get(father).meta.equals(logSeq.get(i))){
							if(ddTree.get(father).father==0){//match
								result.add(1-ddTree.get(entry.getKey().endNode).score[1]);//match叶子节点的1-FAR值加入结果list
							}
							else{//add to DPBlock
								MatchInfo mif = new MatchInfo(father,entry.getKey().endNode);
								Integer times = curBlock.matchInfos.get(mif);
								if(times==null)times=0;
								curBlock.matchInfos.put(mif, times+1);
							}
						}
					}
					node = node.next;
				}
			}
			//追加curBlock到队列首
			node = new QueueNode(curBlock);
			node.next = queue.next;
			queue.next = node;
		}
		return result;
	}
	/**
	 * 从Motif文件构造MotifTree
	 * */
	public static List<DDNode> createDDTreeFromLog(String logPath) throws IOException{
		List<DDNode> ddTree = new ArrayList<DDNode>();
		ddTree.add(new DDNode(null,-1,null));//添加起始点
		BufferedReader br = new BufferedReader(new FileReader(logPath));
		String s = null;
		while((s=br.readLine())!=null){
			String[] split = s.split(":");
			SeqMeta[] smeta = BruteForceFired.deserializeSeqMeta(split[0]);
			double[] score = BruteForceFired.deserializeScore(split[1]);
			int nodeIndex = 0;
			int j = 0;
			for(j=0;j<smeta.length;j++){//add each SeqMeta to tree
				DDNode curNode = ddTree.get(nodeIndex);
				if(curNode.sons==null)
					curNode.sons = new HashMap<SeqMeta,Integer>();
				if(curNode.sons.get(smeta[j])==null){
					DDNode newNode = new DDNode(smeta[j],nodeIndex,null);
					ddTree.add(newNode);
					curNode.sons.put(smeta[j],ddTree.size()-1);
					nodeIndex = ddTree.size()-1;
				}
				else
					nodeIndex = curNode.sons.get(smeta[j]);
			}
			ddTree.get(nodeIndex).score = score;
		}
		br.close();
		return ddTree;
	}	
}
