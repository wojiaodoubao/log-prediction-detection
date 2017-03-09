package prediction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import detection.BruteForceFired;

/**
 * 错误预警
 * 实现了控制台的错误预警
 * */
public abstract class ErrorPrediction {
	public static void main(String args[]) throws IOException{
		/**
		 * 手动输入，需要"输出当前状态集合"那行，默认是注释掉的
		 * 注：根-4是节点7，根到2是节点3
		 * 如0,2->2,4->3,2观察走到头自动删除
			0,2
			[0:, 3:0]
			2,4
			-----------------------------
			[4]
			[[1]]
			-----------------------------
			[2, 4]
			[[3]]
			[0:, 3:0, 5:2, 7:2]
			3,2
			[0:, 3:3, 7:2]
		 * 如0,2->11,4观察超时自动删除
			0,2
			[0:, 3:0]
			11,4
			-----------------------------
			[4]
			[[1]]
			[0:, 7:11]
		 * */
		List<RuleNode> rTree = createRuleTreeFromLog("/home/belan/Desktop/Rules/part-r-00000");
//		firstSearch(rTree,0);
		Map<String,Long> logToSidDict = new HashMap<String,Long>();
		Map<Long,String> sidToLogDict = new HashMap<Long,String>();
		logToSidDict.put("0", (long)0);
		logToSidDict.put("1", (long)1);
		logToSidDict.put("2", (long)2);
		logToSidDict.put("3", (long)3);
		logToSidDict.put("4", (long)4);
		sidToLogDict.put((long)0, "0");
		sidToLogDict.put((long)1, "1");
		sidToLogDict.put((long)2, "2");
		sidToLogDict.put((long)3, "3");
		sidToLogDict.put((long)4, "4");
		long ruleGap = 60000;
		long gap = 10;
		new EP1(rTree,logToSidDict,sidToLogDict,ruleGap,gap)
			.errorPredictionByConsole(System.in);
	}
	private List<RuleNode> rTree = null;//RuleTree
	public static class RuleNode{
		SeqMeta meta;//起始点meta==null，其他点meta不为空
		Map<SeqMeta,Integer> sons;
		Set<List<SeqMeta>> suffixSet;
		List<SeqMeta> prefix;
		public RuleNode(SeqMeta meta){
			this.meta = meta;
			suffixSet = new HashSet<List<SeqMeta>>();
			sons = null;
		}
	}
	private long ruleGap;//规则间时间间隔
	private long gap;//meta间时间间隔
	private Map<String,Long> logToSidDict;//日志-sid map
	private Map<Long,String> sidToLogDict;//sid-日志 map
	private SimpleDateFormat sdf =//格式化时间 
			new SimpleDateFormat(FrequentSequenceDiscovery.DATE_STRING);
	/**
	 * rTree:规则前缀构造的规则树<br />
	 * ruleGap:规则产生时使用的ruleGap
	 * */
	public ErrorPrediction(List<RuleNode> rTree,Map<String,Long> logToSidDict,
			Map<Long,String> sidToLogDict,long ruleGap,long gap){
		this.rTree = rTree;
		this.logToSidDict = logToSidDict;
		this.sidToLogDict = sidToLogDict;
		this.ruleGap = ruleGap;
		this.gap = gap;
	}
	private static class Status{
		//上一状态--lastMeta-->当前状态
		TimeMeta lastMeta;//上一个Meta 
		int curNode;//当前状态
		public Status(TimeMeta lastMeta,int curNode){
			this.lastMeta = lastMeta;
			this.curNode = curNode;
		}
		@Override
		public boolean equals(Object obj){
			if(this==obj)return true;
			else if(obj instanceof Status){
				Status s = (Status)obj;
				if(this.curNode==s.curNode)return true;
			}
			return false;
		}
		@Override
		public int hashCode(){
			if(lastMeta!=null)
				return lastMeta.hashCode()+curNode;
			return curNode;
		}
		@Override
		public String toString(){
			String s = curNode+":";
			if(lastMeta!=null)
				s+=lastMeta.time;
			return s;
		}
	}
	/**
	 * 根据输入流预警
	 * @throws IOException 
	 * */
	public void errorPredictionByConsole(InputStream in) throws IOException{
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String inLine = null;
		Set<Status> curStatus = new HashSet<Status>();
		curStatus.add(new Status(null,0));//添加树根状态
		while((inLine=br.readLine())!=null){
			try{//持续工作，忽略非法输入
				String[] s = inLine.split(",");
				Long time = Long.parseLong(s[0]);
				Long sid = logToSidDict.get(s[1]);
				TimeMeta curM = new TimeMeta(sid,time);//构造当前输入meta
				Set<Status> newStatus = new HashSet<Status>();
				Set<Status> deleteStatus = new HashSet<Status>();
				for(Status sta:curStatus){
					if(sta.curNode==0){//特殊处理树根节点
						RuleNode staN = rTree.get(sta.curNode);//获取当前RuleNode
						if(staN.sons==null||staN.sons.get(curM)==null)
							continue;
						else{
							RuleNode nextNode = rTree.get(staN.sons.get(curM));//下一状态
							if(nextNode.suffixSet!=null&&nextNode.suffixSet.size()>0){//匹配成功
								//give a prediction
								ruleFiredInfo(nextNode.prefix,nextNode.suffixSet,curM,ruleGap);
							}
							newStatus.add(new Status(curM,staN.sons.get(curM)));							
						}
					}
					else{//对于其他状态
						RuleNode staN = rTree.get(sta.curNode);//获取当前RuleNode
						if(sta.lastMeta==null//此状态非法
								||(curM.time-sta.lastMeta.time)<0
								||(curM.time-sta.lastMeta.time)>gap//此状态超时
								||staN.sons==null){//此状态已走到头
							deleteStatus.add(sta);
							continue;
						}
						if(staN.sons.get(curM)==null)//状态没能匹配
							continue;
						RuleNode nextNode = rTree.get(staN.sons.get(curM));//可--curM-->下一状态
						if(nextNode.suffixSet!=null&&nextNode.suffixSet.size()>0){//匹配成功
							//give a prediction
							ruleFiredInfo(nextNode.prefix,nextNode.suffixSet,curM,ruleGap);
						}
						//这里不会删除状态sta，因为sta即使能匹配成功，也可以选择不匹配而去匹配后面的
						//只有非法状态，超时状态，走到头的状态会被删掉！！！其他只会更新状态lastMeta
						newStatus.add(new Status(curM,staN.sons.get(curM)));
					}
				}
				curStatus.removeAll(deleteStatus);//delete状态
				for(Status sta:newStatus){//add&update状态
					curStatus.remove(sta);
					curStatus.add(sta);
				}
			}catch(Exception e){
				e.printStackTrace();
			}
//			System.out.println(curStatus);//输出当前状态集合！！！！！
		}
	}
	public abstract void ruleFiredInfo(List<SeqMeta> prefix,Set<List<SeqMeta>> suffixSet,TimeMeta curM,long ruleGap);
	/**
	 * 从规则文件构造RuleTree
	 * */
	public static List<RuleNode> createRuleTreeFromLog(String logPath) throws IOException{
		List<RuleNode> rTree = new ArrayList<RuleNode>();
		rTree.add(new RuleNode(null));//添加起始点
		BufferedReader br = new BufferedReader(new FileReader(logPath));
		String s = null;
		while((s=br.readLine())!=null){
			String[] split = s.split("->");
			if(split==null||split.length<2
					||split[0]==null||split[0].length()<=0)
				continue;
			List<SeqMeta> prefix = BruteForceFired.deserializeSeqMetaToArray(split[0]);
			List<SeqMeta> suffix = BruteForceFired.deserializeSeqMetaToArray(split[1]);
			int nodeIndex = 0;
			int j = 0;
			for(j=0;j<prefix.size();j++){//add each SeqMeta of prefix to tree
				RuleNode curNode = rTree.get(nodeIndex);
				if(curNode.sons==null)
					curNode.sons = new HashMap<SeqMeta,Integer>();
				if(curNode.sons.get(prefix.get(j))==null){
					RuleNode newNode = new RuleNode(prefix.get(j));
					rTree.add(newNode);
					curNode.sons.put(prefix.get(j),rTree.size()-1);
					nodeIndex = rTree.size()-1;
				}
				else
					nodeIndex = curNode.sons.get(prefix.get(j));
			}
			rTree.get(nodeIndex).prefix = prefix;
			rTree.get(nodeIndex).suffixSet.add(suffix);
		}
		br.close();
		return rTree;
	}	
	private static void firstSearch(List<RuleNode> rTree,int i){
		RuleNode node = rTree.get(i);
		if(node.meta==null)System.out.print("null"+":");
		else System.out.print(node.meta+":");
		if(node.suffixSet==null
				||node.suffixSet.size()<=0)System.out.println();
		else{
			System.out.print(node.prefix+"\t");
			for(List<SeqMeta> list:node.suffixSet)
				System.out.print("{"+list+"}");
			System.out.println();
		}
		if(node.sons!=null){
			for(Entry<SeqMeta, Integer> entry:node.sons.entrySet()){
				firstSearch(rTree,entry.getValue());
			}
		}
	}	
}
