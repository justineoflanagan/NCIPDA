import java.io.IOException;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import java.util.ArrayList;
import java.util.Collections;

public class PDA_GPPrescribingPatternsRed extends
		TableReducer<Text, DoubleWritable, ImmutableBytesWritable> {

	private ArrayList<Float> AvgCostList = new ArrayList<Float>();
	
	private DoubleWritable result = new DoubleWritable();
	public void reduce(Text _key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		// need to get a sum and a count to calc the average
		float sum = 0;
		float count = 0.0f;
		float max = 0.0f;
		float mean = 0.0f;
		float median = 0.0f;
		float sumofsquares = 0.0f;
		float stddev = 0.0f;
		float min = 0.0f;
		String strval = new String();
		AvgCostList.clear();
		
		
		System.out.println("In Reducer ");
		for (DoubleWritable val : values) {
			AvgCostList.add((float) val.get());
			sum += val.get();
			++count;
		}
		Collections.sort(AvgCostList);
		max = Collections.max(AvgCostList);
		min = Collections.min(AvgCostList); 
		
		if(count % 2 == 0)
		{
			median = AvgCostList.get(((int) count / 2) - 1) + AvgCostList.get((int) count/2) / 2.0f;
		}
		else
		{
			median = AvgCostList.get((int) count / 2);
		}	
	
		System.out.println("Done iterating round values for " + _key);
		
		if(count > 0 && sum >0)
		{
			mean = sum/count;
		}
		else 
		{
			mean = 0.0f;
		}
		
		//Calc std dev
		for (Float f : AvgCostList) {
			sumofsquares += (f - mean) * (f - mean);
		}
		stddev = (float) Math.sqrt(sumofsquares / (count -1));

		System.out.println("Returning from reducer " + _key + " Average cost " + mean + "Median " + median);
		Put put = new Put(Bytes.toBytes(_key.toString()));
		put.addColumn("cf1".getBytes(), "avgactcost".getBytes(), Bytes.toBytes(Float.toString(mean)));
		put.addColumn("cf1".getBytes(), "medianactcost".getBytes(), Bytes.toBytes(Float.toString(median)));
		put.addColumn("cf1".getBytes(), "maxactcost".getBytes(), Bytes.toBytes(Float.toString(max)));
		put.addColumn("cf1".getBytes(), "minactcost".getBytes(), Bytes.toBytes(Float.toString(min)));
		put.addColumn("cf1".getBytes(), "countpractices".getBytes(), Bytes.toBytes(Float.toString(count)));
		put.addColumn("cf1".getBytes(), "stddev".getBytes(), Bytes.toBytes(Float.toString(stddev)));
		context.write(null, put);
	}

}
