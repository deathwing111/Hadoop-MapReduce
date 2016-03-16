import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PeopleRecommenderMapper extends Mapper<LongWritable, Text, IntWritable, Text>
{

	@Override
	public void map (LongWritable key, Text val, Context context)
	{
		String line = val.toString();

		// Split the line to get user id and friends user id
		String userFriendsArray[] = line.split("\t");

		// Check for the incorrect input
		try {
			if (userFriendsArray.length == 2)
			{
				// Get the user id
				String user = userFriendsArray[0];
				IntWritable userKey = new IntWritable(Integer.parseInt(user));

				// Get the user's friend's list
				String friendsArray[] = userFriendsArray[1].split(",");
				String frnd1, frnd2;
				IntWritable frnd1Key = new IntWritable();
				IntWritable frnd2Key = new IntWritable();
				Text frnd1Val = new Text();
				Text frnd2Val = new Text();
				
				int i = 0, j = 0;
				while (i < friendsArray.length)
				{
					frnd1 = friendsArray[i];

					// The user is all ready friend with all the friends in his list
					// hence mark it as T = for all ready friend
					frnd1Key.set(Integer.parseInt(frnd1));
					frnd1Val.set("T," + frnd1);

					context.write(userKey, frnd1Val);
					
					// Reset the value of the friend1
					frnd1Val.set("F," + frnd1);
					
					j = i + 1;
					while (j < friendsArray.length)
					{
						frnd2 = friendsArray[j];
						frnd2Key.set(Integer.parseInt(frnd2));
						frnd2Val.set("F," + frnd2);

						// Enter value as not friend for the friends in the list of user, as they have one mutual friend
						context.write(frnd1Key, frnd2Val);
						context.write(frnd2Key, frnd1Val);
						
						j++;
					}
					i++;
				}
			}
		}
		catch (IOException | InterruptedException e) 
		{
			e.printStackTrace();
		}
	}
}
