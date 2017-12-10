import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.util.GenericOptionsParser;



public class PDA_GPPrescribingPatternsDrv {

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//if(otherArgs.length != 1) {
		//	System.err.println("Usage: PDA_GPPrescribingPatternsDrv <out>");
		//	System.exit(2);
		//}
		
		System.out.println("Starting Job");
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);
		Job job = Job.getInstance(conf, "PDA_GPPrescribingPatterns Get Avg Script Exp");
		job.setJarByClass(PDA_GPPrescribingPatternsDrv.class);
		TableMapReduceUtil.initTableMapperJob("GPPracAvgCosts", scan, PDA_GPPrescribingPatternsMap.class, Text.class, DoubleWritable.class, job);

		TableMapReduceUtil.initTableReducerJob("GPPracTypeAvgCosts", PDA_GPPrescribingPatternsRed.class, job);
		job.setNumReduceTasks(25);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
