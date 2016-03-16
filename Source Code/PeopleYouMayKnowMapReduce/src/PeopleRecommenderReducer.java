import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

public class PeopleRecommenderReducer extends Reducer<IntWritable, Text, IntWritable, Text> 
{

	public void reduce (IntWritable key, Iterable<Text> val, Context context)
	{
		HashMap<String, Integer> keyValHash = new HashMap<String, Integer>();
		String valueArray[];

		for (Text singleValue : val)
		{
			valueArray = singleValue.toString().split(",");

			// If the users are all ready friend then the record is of no use
			if (valueArray[0].equals("T"))
			{
				keyValHash.put(valueArray[1], -1);
			} 
			// If the users are not friends
			else if (valueArray[0].equals("F"))
			{
				// If the user is all ready added to the hashmap
				if (keyValHash.containsKey(valueArray[1]))
				{
					// If the users are not all ready friends
					if (keyValHash.get(valueArray[1]) != -1)
					{
						// Increment the count of mutual friends as entry is found again
						keyValHash.put(valueArray[1],keyValHash.get(valueArray[1]) + 1);
					}
				}
				else
				{
					keyValHash.put(valueArray[1], 1);
				}
			}
		}

		// Convert hashmap to arraylist for sorting
		ArrayList<Entry<String, Integer>> keyValList = new ArrayList<Entry<String, Integer>>();
		for (Entry<String, Integer> entry : keyValHash.entrySet()) {
			// Ignoring the -1 value
			if (entry.getValue() != -1) 
			{
				keyValList.add(entry);
			}
		}

		// Sort the list according to the highest no. of mutual friends
		// Break the tie with the small user id
		PeopleRecommenderComparator comparator = new PeopleRecommenderComparator();
		Collections.sort(keyValList, comparator);

		try 
		{
				// Only output the top 10 records with highest no. of mutual friends first
				ArrayList<String> maxMutualFriendsList = new ArrayList<String>();
				
				int i = 0;
				while (i < Math.min(10, keyValList.size())) 
				{
					maxMutualFriendsList.add(keyValList.get(i).getKey());
					i++;
				}

				context.write(key, new Text(StringUtils.join(",", maxMutualFriendsList)));
		} 
		catch (IOException | InterruptedException e) 
		{
			e.printStackTrace();
		}
	}
}