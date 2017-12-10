import java.io.IOException;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;


public class PDA_GPPractSumItemsRed extends
		TableReducer<Text, IntWritable, ImmutableBytesWritable> {

	
	public void reduce(Text _key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		// need to get a sum 
		int sum = 0;
		
		for (IntWritable val : values) {
			sum += val.get();
		}
		
		System.out.println("Returning from reducer " + _key + " Total num items " + sum);
		Put put = new Put(Bytes.toBytes(_key.toString()));
		put.addColumn("cf1".getBytes(), "TotalNumItems".getBytes(), Bytes.toBytes(Integer.toString(sum)));
		context.write(null, put);
	}

}
