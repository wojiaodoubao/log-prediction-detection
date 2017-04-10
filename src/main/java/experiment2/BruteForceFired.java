package experiment2;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import detection.DPDicTreeFired;
import detection.DPDicTreeFired.DDNode;

import java.io.*;

import utils.SeqMeta;
import utils.TimeMeta;

public class BruteForceFired{	
	List<TimeMeta> logSeq = null;
	List<List<SeqMeta>> motifs = null;
	List<Integer> motifIDs = null;
	long gap;
	public BruteForceFired(List<TimeMeta> logSeq,List<List<SeqMeta>> motifs,long gap,List<Integer> motifIDs){
		this.logSeq = logSeq;
		this.motifs = motifs;
		this.gap = gap;
		this.motifIDs = motifIDs;
	}
	public Map<Integer,Integer> fired(){
		Map<Integer,Integer> res = new HashMap<Integer,Integer>();
		Set<Integer> doneSeqSet = new HashSet<Integer>();
		//bruteforce fire
		for(int k=0;k<motifs.size();k++){
			List<SeqMeta> motif = motifs.get(k);
			int index = motifIDs.get(k);
			if(doneSeqSet.contains(index)){
				continue;
			}
			int sum = 0;
			for(int i=0;i<logSeq.size();i++){
				List<Integer> route = new ArrayList<Integer>();
				sum += match(motif,0,i,route);
			}
			if(sum>0){
//				System.out.println(motif+":"+sum);
				res.put(index, sum);
			}
			doneSeqSet.add(index);
		}	
		return res;
	}
	//i:motif Index   j:logSeq Index
	public int match(List<SeqMeta> motif,int i,int j,List<Integer> route){
		if(!motif.get(i).equals(logSeq.get(j)))
			return 0;
		else{
//			route.add(j);
		}
		if(i==motif.size()-1){
//			System.out.println(route);
			return 1;
		}
		else if(j==logSeq.size()-1)
			return 0;
		else{
			int sum = 0;
			int l = j+1;
			for(;l<logSeq.size();l++){
				if(logSeq.get(l).getTime()-logSeq.get(j).getTime()>0
						&&logSeq.get(l).getTime()-logSeq.get(j).getTime()<=gap)
					sum+=match(motif,i+1,l,route);
				else
					break;
			}
			return sum;
		}
	}
	
	public static void main(String args[]) throws IOException{
		long gap = Fired.GLOBAL_GAP;
		//create logSeq
		List<TimeMeta> logSeq = Fired.createLogSeq(TestDataGenerator.directory+"/seq-data");
		//create motifs
		List<List<SeqMeta>> motifs = new ArrayList<List<SeqMeta>>();
		String path = TestDataGenerator.directory+"/motif";
		BufferedReader br = new BufferedReader(new FileReader(path));
		String s = null;
		while((s=br.readLine())!=null){
			String[] split = s.split(":");
			List<SeqMeta> list = detection.BruteForceFired.deserializeSeqMetaToArray(split[0]);
			motifs.add(list);
		}
		br.close();
		//create motifIDs
		List<DDNode> ddTree = DPDicTreeFired.createDDTreeFromLog(TestDataGenerator.directory+"/motif");
		List<Integer> motifIDs = new ArrayList<Integer>();
		for(List<SeqMeta> motif:motifs){
			int index = Fired.findMotifNodeIndex(ddTree, motif);
			if(index<0){
				System.out.println("找不到motif对应的node节点！");
				return;
			}
			motifIDs.add(index);
		}
		//BruteForceFired
		BruteForceFired bff = new BruteForceFired(logSeq,motifs,gap,motifIDs);
		long time = System.currentTimeMillis();
		Map<Integer,Integer> res = bff.fired();
		System.out.println("BruteForceFired:"+(System.currentTimeMillis()-time));
		for(Entry<Integer, Integer> entry:res.entrySet()){
			System.out.println(entry.getKey()+":"+entry.getValue());
		}
	}	
}
