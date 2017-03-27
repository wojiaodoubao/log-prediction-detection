import java.util.Scanner;

import prediction.FrequentSequenceDiscoveryMR;
import prediction.RemoveDuplicatedSequenceMR;
import prediction.RulesDiscoveryMR;
import prediction.RulesScoresToRulesMR;
import preprocessing.DataPreprocessing;
import preprocessing.LogToMeta;
import preprocessing.MetaFileSplit;
/**
/home/belan/Desktop/实验一数据
/home/belan/Desktop/Out

/home/belan/Desktop/实验数据2
/home/belan/Desktop/Out2
 * */
public class Main {
	public static void main(String arg[]) throws Exception{
		if(arg==null||arg.length<2){
			arg = new String[2];
			Scanner sc = new Scanner(System.in);
			arg[0] = sc.nextLine();
			arg[1] = sc.nextLine();
		}
		String inputPath = arg[0];
		String outputPath = arg[1];
		//预处理-DataPreprocessing
		String[] DataPreprocessingArgs = new String[2];
		DataPreprocessingArgs[0] = inputPath;
		DataPreprocessingArgs[1] = outputPath+"/DataPreprocessing";
		//预处理-LogToMeta
		String[] LogToMetaArgs = new String[2];
		LogToMetaArgs[0] = DataPreprocessingArgs[1]+"/part-r-00000";
		LogToMetaArgs[1] = outputPath+"/Dict";
		//预处理-MetaFileSplit
		String[] MetaFileSplitArgs = new String[3];
		MetaFileSplitArgs[0] = LogToMetaArgs[0];
		MetaFileSplitArgs[1] = outputPath+"/MetaFileSplit";
		MetaFileSplitArgs[2] = LogToMetaArgs[1]+"/part-r-00000";
		//FrequentSequenceDiscovery
		String[] FrequentSequenceDiscoveryArgs = new String[5];
		FrequentSequenceDiscoveryArgs[0] = MetaFileSplitArgs[1]+"/part-r-00000";
		//FrequentSequenceDiscoveryArgs[1] = outputPath+"/Sequences";
		FrequentSequenceDiscoveryArgs[1] = outputPath+"/DuplicatedSequences";
		FrequentSequenceDiscoveryArgs[2] = LogToMetaArgs[1]+"/part-r-00000";
		FrequentSequenceDiscoveryArgs[3] = "60000";
		FrequentSequenceDiscoveryArgs[4] = "2";
		//RemoveDuplicatedSequenceMR
		String[] RemoveDuplicatedSequenceMRArgs = new String[2];
		RemoveDuplicatedSequenceMRArgs[0] = FrequentSequenceDiscoveryArgs[1];
		RemoveDuplicatedSequenceMRArgs[1] = outputPath+"/Sequences";
		//DistributedRuleGenerator
		String[] DistributedRuleGeneratorArgs = new String[8];
		DistributedRuleGeneratorArgs[0] = FrequentSequenceDiscoveryArgs[1];
		DistributedRuleGeneratorArgs[1]	= outputPath+"/RuleScore";
		DistributedRuleGeneratorArgs[2] = DataPreprocessingArgs[0];
		DistributedRuleGeneratorArgs[3] = "5";
		DistributedRuleGeneratorArgs[4] = "8";
		DistributedRuleGeneratorArgs[5] = "4";
		DistributedRuleGeneratorArgs[6] = "0.5";
		DistributedRuleGeneratorArgs[7] = LogToMetaArgs[1]+"/part-r-00000";
		//RuleScoreToRules
		String[] RuleScoreToRulesArgs = new String[2];
		RuleScoreToRulesArgs[0] = DistributedRuleGeneratorArgs[1];
		RuleScoreToRulesArgs[1] = outputPath+"/Rules";
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
		FrequentSequenceDiscoveryMR.main(FrequentSequenceDiscoveryArgs);
		System.out.println("FrequentSequenceDiscovery:"+(System.currentTimeMillis()-time));
		
		time = System.currentTimeMillis();
		RemoveDuplicatedSequenceMR.main(RemoveDuplicatedSequenceMRArgs);
		System.out.println("RemoveDuplicatedSequenceMR:"+(System.currentTimeMillis()-time));
		
		time = System.currentTimeMillis();		
		RulesDiscoveryMR.main(DistributedRuleGeneratorArgs);
		System.out.println("RuleDiscoveryMR:"+(System.currentTimeMillis()-time));
		
		time = System.currentTimeMillis();		
		RulesScoresToRulesMR.main(RuleScoreToRulesArgs);
		System.out.println("RulesScoresToRulesMR:"+(System.currentTimeMillis()-time));
		
		time = System.currentTimeMillis();		
	}
}
