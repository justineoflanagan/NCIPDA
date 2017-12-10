import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;



public class PDA_GPPractSumItemsDrv {

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//if(otherArgs.length != 1) {
		//	System.err.println("Usage: PDA_GPPractAvgCostsDrv <out>");
		//	System.exit(2);
		//}
		
		System.out.println("Starting Job");
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);
		Job job = Job.getInstance(conf, "PDA_GPPractSumItemsDrv Get Avg Script Exp");
		job.setJarByClass(PDA_GPPractSumItemsDrv.class);
		TableMapReduceUtil.initTableMapperJob("GPScriptData", scan, PDA_GPPractSumItemsMap.class, Text.class, IntWritable.class, job);

		TableMapReduceUtil.initTableReducerJob("GPPracSumItems", PDA_GPPractSumItemsRed.class, job);
		job.setNumReduceTasks(25);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
