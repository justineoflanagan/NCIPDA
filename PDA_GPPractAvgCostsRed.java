import java.io.IOException;



import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;


public class PDA_GPPractAvgCostsRed extends
		TableReducer<Text, Text, ImmutableBytesWritable> {

	private DoubleWritable result = new DoubleWritable();
	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		// need to get a sum and a count to calc the average
		
		Double avg = new Double(1.32f);
		String strval = new String();
		Double dactcosts = new Double(0);
		Double dtotalcosts = new Double (0);
		int intpopn = 0;
		
		
		System.out.println("In Reducer ");
		for (Text val : values) {
			// parse out the actual cost value and the population value
			//String[] arrLine = ivalue.toString().split(",");
	        //field2 = arrLine[1];
	        //field3 = arrLine[2];
	        //field6 = arrLine[5];

			String[] arrLine = val.toString().split(",");
			System.out.println("doing strval now" + strval);
			// caret is a special char in regex so enclose in [] to make it a literal
		
			//DEBUG HERE
			dactcosts = Double.parseDouble(arrLine[0]);
			dtotalcosts += dactcosts;
			
			intpopn = Integer.parseInt(arrLine[1]);
			System.out.println("we have split out into " + arrLine[0] + " and " + arrLine[1]);
			// Not keeping a count.  Foreach practice code the population should be the same.
		}
		System.out.println("Done iterating round values for " + _key);
		
		if(intpopn > 0 && dtotalcosts > 0)
		{
			avg = dtotalcosts/intpopn;
			
		}
		else 
		{
			avg = 0.0;
		}
		
		
		System.out.println("Returning from reducer " + _key + " Average cost " + avg);
		Put put = new Put(Bytes.toBytes(_key.toString()));
		put.addColumn("cf1".getBytes(), "avgactcost".getBytes(), Bytes.toBytes(Double.toString(avg)));
		context.write(null, put);
	}

}
