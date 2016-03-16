import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Rohini Yadav
 * This is the first mapper which reads the file containing user id and its friend list.
 * It emits the <user Id> <Each friend + "frnd">
 *
 */
public class FriendsMapper1 extends Mapper<LongWritable, Text, Text, Text> 
{
	@Override
	protected void map(LongWritable key, Text value, Context context)
	{
		try 
		{
			String line = value.toString();
			// Split the input line to get the user id and list of his friends' user ids
			String lineSplitArr[] = line.split("\t");

			Text userId = new Text(lineSplitArr[0]);

			if (lineSplitArr.length == 2) 
			{
				// For each friend in the friend list emit <user Id> <Single friend user id "frnd">
				for (String friend : lineSplitArr[1].split(",")) 
				{
					context.write(userId, new Text(friend + "f"));

				}
			}
		}
		catch (IOException | InterruptedException e) 
		{
			e.printStackTrace();
		}
	}
}
