import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;


public class PDA_GPPractAvgCostsMap extends
		TableMapper<Text, Text> { 

		private Text outPracCode= new Text(); // Will be new or established
		private Text outPopnActCost = new Text();
		
	public void map(ImmutableBytesWritable row, Result columns,  Context context)
			throws IOException, InterruptedException {
			String strPracCode = new String(columns.getValue("cf1".getBytes(), "GPPracCode".getBytes()));
			
			// Get Act Cost data 
			String stractcost = new String(columns.getValue("cf1".getBytes(), "ActCost".getBytes()));
			String strpracpop = new String(columns.getValue("cf1".getBytes(), "TotalPop".getBytes()));
			
			//System.out.println("In Reducer, inputs " + _key);
			System.out.println("In Mapper Practice Code is " + strPracCode);
		
				// Need some code to handle blank or invalid Practice code?
				outPopnActCost.set(stractcost + "," + strpracpop);
				outPracCode.set(strPracCode);	
			System.out.println("Ending Mapper Practice type " + strPracCode + " and actual cost is " + stractcost + " pop " + strpracpop);
			context.write(outPracCode, outPopnActCost);
			
	}

}
