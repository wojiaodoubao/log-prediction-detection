package experiment2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import detection.DPDicTreeFired.DDNode;
import utils.SeqMeta;
import utils.TimeMeta;
import java.io.*;

public abstract class Fired {
	protected List<DDNode> ddTree = null;//motif序列集字典树
	protected long gap;//序列时间戳gap约束
	public Fired(List<DDNode> ddTree,long gap){
		this.ddTree = ddTree;
		this.gap = gap;
	}
	public abstract Map<Integer,Integer> fired(List<TimeMeta> logSeq);
	public static List<TimeMeta> createLogSeq(String dir) throws IOException{
		List<TimeMeta> res = new ArrayList<TimeMeta>();
		BufferedReader br = new BufferedReader(new FileReader(dir));
		String s = null;
		while((s=br.readLine())!=null){
			String[] split = s.split(":");
			res.add(new TimeMeta(Long.parseLong(split[0]),Long.parseLong(split[1])));
		}
		br.close();
		return res;
	}
	public static long GLOBAL_GAP = 10; 
	/**
	 * 返回motif在ddTree中对应的节点编号，motif不在ddTree时返回-1<br>
	 * 1.树中任意两点间有且只有一条路径；
	 * 2.motif对应从根节点出发的一条路径；
	 * 3.因此节点编号可以唯一表示一条路径，表示一个motif。
	 * */
	public static int findMotifNodeIndex(List<DDNode> ddTree,List<SeqMeta> motif){
		int index = 0;
		for(int i=0;i<motif.size();i++){
			DDNode node = ddTree.get(index);
			if(node==null||node.sons==null||node.sons.get(motif.get(i))==null)
				return -1;
			index = node.sons.get(motif.get(i));
		}
		return index;
	}	
}
