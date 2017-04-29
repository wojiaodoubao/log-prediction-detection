package log;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;

public class LogJobRunner {
	private static final Log LOG = LogFactory.getLog(LogJobRunner.class);
	private Job job = null;
	public LogJobRunner(Job job){
		this.job = job;
	}
	public boolean waitForCompletion(boolean verbose) throws ClassNotFoundException, IOException, InterruptedException{
		LOG.info(""+job.getJobName()+" start!");
		boolean status = job.waitForCompletion(verbose);
		LOG.info(""+job.getJobName()+" complete!");
		return status;
	}
}
