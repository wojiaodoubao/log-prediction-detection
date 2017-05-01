import java.util.Scanner;

import detection.MotifSequenceDiscovery;
import preprocessing.DataPreprocessing;
import preprocessing.LogToMeta;
import preprocessing.MetaFileSplit;
/**
/home/belan/Desktop/实验三数据-侦测实验/pn-4-4
/home/belan/Desktop/Out3-detection/pn-4-4
6000
1
0.5
log0,+;log1,+;log2,+;log3,+;log4,-;log5,-;log6,-;log7,-;
 * */
public class Detection {
	public static void main(String arg[]) throws Exception{
		if(arg==null||arg.length<6){
			arg = new String[6];
			Scanner sc = new Scanner(System.in);
			arg[0] = sc.nextLine();
			arg[1] = sc.nextLine();
			arg[2] = sc.nextLine();//gap
	    	arg[3] = sc.nextLine();//POD
	    	arg[4] = sc.nextLine();//FAR
	    	arg[5] = sc.nextLine();//log label：logName1+','+label1+';'+logName2+','+label2			
		}
		String inputPath = arg[0];
		String outputPath = arg[1];
		String gap = arg[2];
		String POD = arg[3];
		String FAR = arg[4];
		String logLabel = arg[5];
		//产生Dict文件
		//预处理-DataPreprocessing
		String[] DataPreprocessingArgs = new String[2];
		DataPreprocessingArgs[0] = inputPath;
		DataPreprocessingArgs[1] = outputPath+"/DataPreprocessing";
		//预处理-LogToMeta
		String[] LogToMetaArgs = new String[2];
		LogToMetaArgs[0] = DataPreprocessingArgs[1];
		LogToMetaArgs[1] = outputPath+"/Dict";
		//预处理-MetaFileSplit
		String[] MetaFileSplitArgs = new String[3];
		MetaFileSplitArgs[0] = DataPreprocessingArgs[1];
		MetaFileSplitArgs[1] = outputPath+"/MetaFileSplit";
		MetaFileSplitArgs[2] = LogToMetaArgs[1];		
		//MotifSequenceDiscovery
		String[] MotifSequenceDiscoveryArgs = new String[7];
		MotifSequenceDiscoveryArgs[0] = MetaFileSplitArgs[1];
		MotifSequenceDiscoveryArgs[1] = outputPath+"/Motifs";
		MotifSequenceDiscoveryArgs[2] = LogToMetaArgs[1];
		MotifSequenceDiscoveryArgs[3] = gap;
		MotifSequenceDiscoveryArgs[4] = POD;
		MotifSequenceDiscoveryArgs[5] = FAR;
		MotifSequenceDiscoveryArgs[6] = logLabel;
		//执行
		long time = System.currentTimeMillis();
		DataPreprocessing.main(DataPreprocessingArgs);
		System.out.println("DataPreprocessing:"+(System.currentTimeMillis()-time));
		
		time = System.currentTimeMillis();
		LogToMeta.main(LogToMetaArgs);
		System.out.println("LogToMeta:"+(System.currentTimeMillis()-time));
		
		time = System.currentTimeMillis();		
		MetaFileSplit.main(MetaFileSplitArgs);
		System.out.println("MetaFileSplit:"+(System.currentTimeMillis()-time));		

		time = System.currentTimeMillis();
		MotifSequenceDiscovery.main(MotifSequenceDiscoveryArgs);
		System.out.println("MotifSequenceDiscovery:"+(System.currentTimeMillis()-time));		
	}
}
