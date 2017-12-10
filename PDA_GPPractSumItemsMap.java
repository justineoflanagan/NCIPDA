import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;


public class PDA_GPPractSumItemsMap extends
		TableMapper<Text, IntWritable> { 

		private Text outPracCode= new Text(); // Will be new or established
		private IntWritable outNumItems = new IntWritable();
		
	public void map(ImmutableBytesWritable row, Result columns,  Context context)
			throws IOException, InterruptedException {
			String strPracCode = new String(columns.getValue("cf1".getBytes(), "GPPracCode".getBytes()));
			// String strPracCode = new String(row.get());
			// Get NumItems data 
			String strnumitems = new String(columns.getValue("cf1".getBytes(), "NumItems".getBytes()));
			int intnumitems = 0;
		
			System.out.println("In Mapper Practice Code is " + strPracCode);
			//percentage = Double.parseDouble(strPercentage) / 100.0;
		
				intnumitems = Integer.parseInt(strnumitems);
				outNumItems.set(intnumitems); 
		
				
				
				outPracCode.set(strPracCode);	
			System.out.println("Ending Mapper Practice type " + strPracCode + " and numitems is " + intnumitems);
			context.write(outPracCode, outNumItems);
			
	}

}
