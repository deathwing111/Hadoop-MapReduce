import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Rohini Yadav
 * This is the Mapper which emits each friend in the friend list of the user
 * ids mentioned in the arguments (extracted from the configuration) as key
 * and "one" as value.
 * <user id> <one> for each friend in the friends list of the passed user id
 *  
 */

public class MutualFriendsMapper extends Mapper<Object, Text, Text, IntWritable> 
{
	private Text userIdKey = new Text();
	private static String userId1;
	private static String userId2;
	// Constant used to set as value for each friend in the friends list of the user ids provided
	private IntWritable ONE = new IntWritable(1);

	public void map(Object key, Text value, Context context) 
	{
		// Get the user Ids passed to Driver class in the arguments
		Configuration conf = context.getConfiguration();
		userId1 = conf.get("userId1");
		userId2 = conf.get("userId2");

		// Split the user id and the list of frnds Id
		String line = value.toString();
		String[] lineSplit = line.split("\t");
		String userId = lineSplit[0];

		// Ignore the users with no friends records
		if(lineSplit.length==2)
		{
			// Check if the user is the one gives by the user in arguments
			if(userId.compareTo(userId1) == 0 || userId.compareTo(userId2) == 0)
			{
				// Split the friends list
				String[] frndsArr = lineSplit[1].split(",");
				
				// For each friend in the user id's friends emit id as key and "one" as value
				for(String singleFriendId : frndsArr) 
				{
					userIdKey.set(singleFriendId);
					try {
						context.write(userIdKey, ONE);
					} catch (IOException | InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
