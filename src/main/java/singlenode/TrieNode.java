package singlenode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import motif.Meta;

public class TrieNode {
	Map<Meta,Integer> children;//儿子节点
	Meta content;//节点内容
	Map<Integer,List<Integer>> indexMap;//叶子节点的序列出现标记位置索引-<example_id,位置索引>
	public TrieNode(Meta content){
		this.children = new HashMap<Meta,Integer>();
		this.content = content;
		this.indexMap = new HashMap<Integer,List<Integer>>();
	}
	@Override
	public String toString(){
		String s = "";
		s+="{"+content+" ";
		if(indexMap!=null){
			for(Entry<Integer,List<Integer>> entry:indexMap.entrySet()){
				s+="<"+entry.getKey()+":";
				for(int i:entry.getValue())
					s+=i+" ";
				s+=">";
			}
		}
		s+="}";
		return s;
	}
}
