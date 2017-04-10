package experiment2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import detection.DPDicTreeFired;
import detection.DPDicTreeFired.DDNode;
import utils.TimeMeta;

public class BruteForceFiredWithDicTree extends Fired{	
	public BruteForceFiredWithDicTree(List<DDNode> ddTree, long gap) {
		super(ddTree, gap);
	}

	@Override
	public Map<Integer, Integer> fired(List<TimeMeta> logSeq) {
		Map<Integer, Integer> result = new HashMap<Integer,Integer>();
		for(int start=0;start<logSeq.size();start++){
			Map<Integer, Integer> map = iteration(logSeq,start,0);
			for(Entry<Integer, Integer> entry:map.entrySet()){
				Integer times = result.get(entry.getKey());
				if(times==null)
					times=0;
				result.put(entry.getKey(), entry.getValue()+times);
			}
		}		
		return result;
	}
	/**
	 * 递归匹配<br />
	 * logSeq[i]从ddTree[nodeIndex]出发(即从ddTree[nodeIndex].sons开始匹配)，返回所有匹配成功节点计数Map
	 * */
	public Map<Integer, Integer> iteration(List<TimeMeta> logSeq,int i,int nodeIndex){
		Map<Integer, Integer> res = new HashMap<Integer,Integer>();
		DDNode node = ddTree.get(nodeIndex);
		if(node!=null&&node.sons!=null&&node.sons.get(logSeq.get(i))!=null){//ddTree[nodexIndex].sons包含logSeq[i]
			int nextNodeIndex = node.sons.get(logSeq.get(i));
			node = ddTree.get(nextNodeIndex);//获取下一节点
			if(node.sons==null||node.sons.size()==0||node.score!=null){//node是终止节点
				Integer times = res.get(nextNodeIndex);
				if(times==null)times=0;
				res.put(nextNodeIndex, times+1);
			}
			if(node.sons!=null&&node.sons.size()>0){
				for(int j=i+1;j<logSeq.size();j++){//对于所有满足gap约束的logSeq[j]，递归计算
					if(logSeq.get(j).getTime()-logSeq.get(i).getTime()>0
							&&logSeq.get(j).getTime()-logSeq.get(i).getTime()<=gap){
						Map<Integer, Integer> map = iteration(logSeq,j,nextNodeIndex);
						for(Entry<Integer, Integer> entry:map.entrySet()){
							Integer times = res.get(entry.getKey());
							if(times==null)times=0;
							res.put(entry.getKey(), entry.getValue()+times);
						}					
					}
					else
						break;
				}
			}
		}
		return res;
	}

	public static void main(String args[]) throws IOException{
		List<DDNode> ddTree = DPDicTreeFired.createDDTreeFromLog(TestDataGenerator.directory+"/motif");
		List<TimeMeta> logSeq = Fired.createLogSeq(TestDataGenerator.directory+"/seq-data");
		long gap = Fired.GLOBAL_GAP;

		BruteForceFiredWithDicTree bfdic = new BruteForceFiredWithDicTree(ddTree,gap);		
		long time = System.currentTimeMillis();
		Map<Integer,Integer> bfdicR = bfdic.fired(logSeq);
		System.out.println("BruteForceFiredWithDicTree耗时:"+(System.currentTimeMillis()-time));
		for(Entry<Integer, Integer> entry:bfdicR.entrySet()){
			System.out.println(entry.getKey()+":"+entry.getValue());
		}
	}
}
