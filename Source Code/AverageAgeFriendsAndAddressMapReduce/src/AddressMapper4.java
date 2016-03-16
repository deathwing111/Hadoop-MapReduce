import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Rohini Yadav
 * This Mapper reads the user data file and emits <user Id> <Address of the user>
 * 
 */

public class AddressMapper4 extends Mapper<LongWritable, Text, Text, Text>
{
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String line = value.toString();
		
		String lineSplitArr[] = line.split(",");

		// Get the user id 
		Text userIdKey = new Text(lineSplitArr[0]);
		
		if (lineSplitArr.length == 10)
		{
			// Form the user address from the user data file
			Text userAddressVal = new Text(lineSplitArr[3] + "," + lineSplitArr[4] + "," + lineSplitArr[5] + "," 
					+ lineSplitArr[6] + "," + lineSplitArr[7]);
			
			// Emit <user Id> <Address of the user>
			context.write(userIdKey,userAddressVal);
		}
		
	}
}
