package experiment2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import detection.DPDicTreeFired;
import detection.DPDicTreeFired.DDNode;
import detection.DPDicTreeFired.DPBlock;
import detection.DPDicTreeFired.MatchInfo;
import detection.DPDicTreeFired.QueueNode;
import utils.SeqMeta;
import utils.TimeMeta;

public class DDPFired extends Fired{
	private List<Integer> leaves = null;//ddTree的全部叶子节点	
	public DDPFired(List<DDNode> ddTree,long gap){
		super(ddTree,gap);
		this.leaves = new ArrayList<Integer>();
		getLeavesFromDDTree(0);
	}	
	//构造this.leaves，计算ddTree的所有叶子节点
	private void getLeavesFromDDTree(int i){
		DDNode node = ddTree.get(i);
		if(node==null)
			return;
		else if(node.sons!=null&&node.sons.size()>0){
			for(Entry<SeqMeta, Integer> entry:node.sons.entrySet()){
				getLeavesFromDDTree(entry.getValue());
			}
		}		
		if(node.score!=null){
			this.leaves.add(i);
		}		
	}
	//输出ddTree的所有叶子节点
	private void showLeavesFromDDTree(int i,List<Integer> list){
		DDNode node = ddTree.get(i);
		if(node==null)
			return;
		else if(node.sons!=null&&node.sons.size()>0){
			list.add(i);
			for(Entry<SeqMeta, Integer> entry:node.sons.entrySet()){
				showLeavesFromDDTree(entry.getValue(),list);
			}
			list.remove(list.size()-1);
		}		
		if(node.score!=null){
			for(int k:list)
				System.out.print(ddTree.get(k).meta+",");
			System.out.println();
		}		
	}	
	/**
	 * DP计算logSeq在ddTree上的fired结果，每次fired就将fired motif的1-FAR值加入List<Double></br>
	 * 从后向前扫描，queue保存满足gap约束的logSeq上meta的DPBlock；logSeq上每个meta根据queue和ddTree叶子节点产生自身DPBlock
	 * */
	public Map<Integer,Integer> fired(List<TimeMeta> logSeq){
		//DP计算fired结果
		Map<Integer,Integer> result = new HashMap<Integer,Integer>();
		QueueNode queue = new QueueNode(null);//DP队列，队列满足按照时间戳从小到大排列
		for(int i=logSeq.size()-1;i>=0;i--){//从后向前DP
			DPBlock curBlock = new DPBlock(i);
			//对每一个叶子节点，计算MatchInfo
			for(int j:leaves){
				DDNode leaf = ddTree.get(j);
				if(leaf.meta.equals(logSeq.get(i))){//add to DPBlock	
					if(leaf.father==0){//该motif长度为1，直接产生结果
						Integer times = result.get(j);
						if(times==null)times=0;
						result.put(j, times+1);
					}
					else{
						MatchInfo mif = new MatchInfo(j,j);
						Integer times = curBlock.matchInfos.get(mif);
						if(times==null)times=0;
						curBlock.matchInfos.put(mif, times+1);
					}
				}
			}
			//枚举queue，计算MatchInfo
			QueueNode node = queue;
			while(node.next!=null){
				DPBlock queBlock = node.next.block;
				if(logSeq.get(queBlock.seqIndex).getTime()-logSeq.get(i).getTime()>gap
						||logSeq.get(queBlock.seqIndex).getTime()-logSeq.get(i).getTime()<=0){//不满足gap约束，则node.next开始全部截掉
					node.next = null;
				}				
				else{//满足gap约束，计算MatchInfo
					for(Entry<MatchInfo, Integer> entry:queBlock.matchInfos.entrySet()){
						int father = ddTree.get(entry.getKey().startNode).father;
						if(ddTree.get(father).meta.equals(logSeq.get(i))){
							if(ddTree.get(father).father==0){//match
								Integer times = result.get(entry.getKey().endNode);
								if(times==null)
									times=0;
								result.put(entry.getKey().endNode, times+entry.getValue());
							}
							else{//add to DPBlock
								MatchInfo mif = new MatchInfo(father,entry.getKey().endNode);
								Integer times = curBlock.matchInfos.get(mif);
								if(times==null)times=0;
								curBlock.matchInfos.put(mif, times+entry.getValue());
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
//			System.out.println(i+"*******");
//			node = queue.next;
//			while(node!=null){
//				System.out.println(node.block);
//				node = node.next;
//			}
		}
		return result;
	}	
	
	public static void main(String args[]) throws IOException{
		List<DDNode> ddTree = DPDicTreeFired.createDDTreeFromLog(TestDataGenerator.directory+"/motif");
		List<TimeMeta> logSeq = Fired.createLogSeq(TestDataGenerator.directory+"/seq-data");
		long gap = Fired.GLOBAL_GAP;
		
		DDPFired ddpf = new DDPFired(ddTree,gap);		
		long time = System.currentTimeMillis();
		Map<Integer,Integer> ddpfR = ddpf.fired(logSeq);
		System.out.println("DDPFired耗时:"+(System.currentTimeMillis()-time));
		for(Entry<Integer, Integer> entry:ddpfR.entrySet()){
			System.out.println(entry.getKey()+":"+entry.getValue());
		}
	}	
}
