package log;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;

public class LogReducer <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
	
	protected static final Log LOG = LogFactory.getLog(LogReducer.class.getName());
	
	protected void setup(Context context
	                   ) throws IOException, InterruptedException {
		//log info
		LOG.info("Reducer setup!");
		super.setup(context);
	}
	
	@SuppressWarnings("unchecked")
	protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context
	                    ) throws IOException, InterruptedException {
		//log info
		LOG.info("Reducer setup!");
		//super.reduce(key, values, context);	
	}
	
	protected void cleanup(Context context
	                     ) throws IOException, InterruptedException {
		//log info
		LOG.info("Reducer cleanup!");
		super.cleanup(context);
	}
	
	public void run(Context context) throws IOException, InterruptedException {
		//log info
		super.run(context);
	}
}
