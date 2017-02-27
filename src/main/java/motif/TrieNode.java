package motif;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
/**
 * 线程不安全
 * */
class TrieTree{
	List<TrieNode> tree;
	public TrieTree(){
		tree = new ArrayList<TrieNode>();
		tree.add(new TrieNode(new LogMeta(-1)));//根节点
	}
	/**
	 * 给节点node添加子节点，内容是content
	 * */
	public int addChild(int node,Meta content){
		if(tree==null)return -1;
		TrieNode tnode = tree.get(node);
		if(tnode==null)return -1;
		if(tnode.children.get(content)==null){
			tree.add(new TrieNode(content));
			tnode.children.put(content, tree.size()-1);
			return tree.size()-1;
		}			
		else{
			return tnode.children.get(content);
		}
	}
	/**
	 * 给节点node添加位置索引-indexMap
	 * */
	public void addIndexToNode(int node,int example_id,int index){
		if(tree==null)return;
		TrieNode tnode = tree.get(node);
		if(tnode==null)return;
		List<Integer> list = tnode.indexMap.get(example_id);
		if(list==null){
			list = new ArrayList<Integer>();
			tnode.indexMap.put(example_id, list);
		}
		list.add(index);
	}
	/**
	 * 返回叶子节点构成的Motif list。
	 * */
	public List<Motif> leafNodeToMotif(Map<Integer,Boolean> example_label){
		List<Motif> list = new ArrayList<Motif>();
		LinkedList<Meta> seqStack = new LinkedList<Meta>();
		leafNodeToMotif(list,0,seqStack,example_label);
		return list;
	}
	public void leafNodeToMotif(List<Motif> list,int node,LinkedList<Meta> seqStack,Map<Integer,Boolean> example_label){
		if(list==null||tree==null)return;
		TrieNode tnode = tree.get(node);
		if(tnode==null)return;
		seqStack.addLast(tnode.content);
		if(tnode.children.size()==0){
//			list.add(new Motif(seqStack.toArray(new Meta[1]),tnode.indexMap,example_label));
			Meta[] m = new Meta[seqStack.size()-1];
			for(int i=0;i<seqStack.size()-1;i++){
				m[i] = seqStack.get(i+1);
			}
			list.add(new Motif(m,tnode.indexMap,example_label));
		}
		for(Entry<Meta,Integer> entry:tnode.children.entrySet()){
			leafNodeToMotif(list,entry.getValue(),seqStack,example_label);
		}
		seqStack.removeLast();
	}
	/**
	 * 先根遍历
	 * */
	public void rootFirstSearch(int node){
		TrieNode tnode = tree.get(node);
		if(tnode==null){
			System.out.println("Wrong node:"+node);
			return;
		}
		System.out.print(tnode);
		for(Entry<Meta,Integer> entry:tnode.children.entrySet()){
			rootFirstSearch(entry.getValue());
		}
	}
	/**
	 * 后根遍历
	 * */
	public void rootLastSearch(int node){
		TrieNode tnode = tree.get(node);
		if(tnode==null){
			System.out.println("Wrong node:"+node);
			return;
		}
		for(Entry<Meta,Integer> entry:tnode.children.entrySet()){
			rootLastSearch(entry.getValue());
		}
		System.out.print(tnode);		
	}	
}
