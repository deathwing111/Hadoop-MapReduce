import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * @author Rohini Yadav
 * This is the reducer class which uses reduce function to calculate the count of the friends
 * in the friend list of the passed user ids.
 * When the count is more than 1 (it can be 1 or 2), then that user id is the mutual friend.
 * The cleanup function is used after the reduce function reduces the keys and writes  the
 * count. This function then emits the mutual friends as per calculated by the reducer
 *
 */
public class MutualFriendsReducer extends Reducer<Text, IntWritable, Text, Text> 
{
	private static String userId1;
	private static String userId2;
	private Text usersKey = new Text();
	private Text mutualFriendsTextVal = new Text();
	StringBuffer mutualFrndsString = new StringBuffer();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	{
		int count = 0;
		
		// As the same user id appears then increment the count
		for (IntWritable value : values)
		{
			count = count + value.get();
		}
		
		// If the count is more than 1, then it is the intersection of the two user ids
		if (count>1)
		{
			mutualFrndsString.append(key+",");
		}
	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context)
	{
		// Get the user ids passed in the argument to form the key
		Configuration conf = context.getConfiguration();
		userId1 = conf.get("userId1");
		userId2 = conf.get("userId2");
		
		// Set the key as the two user Ids provided as the input
		usersKey.set(userId1+","+userId2);
				
		// Set the mutual friends value from the reduce function
		String mutualFrndsVal = mutualFrndsString.toString();
		mutualFriendsTextVal.set(mutualFrndsVal);
		
		
		try 
		{
			context.write(usersKey, mutualFriendsTextVal);
		} catch (IOException | InterruptedException e) 
		{
			e.printStackTrace();
		}
	}
	
}