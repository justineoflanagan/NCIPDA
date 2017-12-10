import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;


public class PDA_GPPrescribingPatternsMap extends
		TableMapper<Text, DoubleWritable> { 

		private Text outPracType= new Text(); // Will be new or established
		private DoubleWritable outActCost = new DoubleWritable();
		private char initPrac;
		
	public void map(ImmutableBytesWritable row, Result columns,  Context context)
			throws IOException, InterruptedException {
			//String strPracCode = new String(columns.getValue("cf1".getBytes(), "GPPracCode".getBytes()));
			String strPracCode = new String(row.get());
			initPrac = strPracCode.charAt(0);
			// Get Act Cost data 
			String stractcost = new String(columns.getValue("cf1".getBytes(), "avgactcost".getBytes()));
			double dblactcost = 0;
			//System.out.println("In Reducer, inputs " + _key);
			System.out.println("In Mapper Practice Code is " + strPracCode);
			//percentage = Double.parseDouble(strPercentage) / 100.0;
			if(initPrac == 'Y')
			{
				outPracType.set("new");
				dblactcost = Double.parseDouble(stractcost);
				outActCost.set(dblactcost); 
				
			}
			else
			{
				// Need some code to handle blank or invalid Practice code?
				outPracType.set("established");
				dblactcost = Double.parseDouble(stractcost);
				outActCost.set(dblactcost);
			}
			System.out.println("Ending Mapper Practice type " + initPrac + " and actual cost is " + dblactcost);
			context.write(outPracType, outActCost);
			
			
	}

}
