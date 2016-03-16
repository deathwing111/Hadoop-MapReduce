import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Rohini Yadav
 * This is a mapper which just passes the output emitted from the Reducer
 *
 */

public class DummyMapper3 extends Mapper<LongWritable, Text, Text, Text>  
{
	@Override
	protected void map(LongWritable key, Text value, Context context)
	{
		String line = value.toString();
		
		//Split the input by spaces
		String lineSplitArr[] = line.split("\\s+");
	
		try 
		{
			context.write(new Text(lineSplitArr[0]), new Text(lineSplitArr[1]));
		} 
		catch (IOException | InterruptedException e) 
		{
			e.printStackTrace();
		}
	}
}
