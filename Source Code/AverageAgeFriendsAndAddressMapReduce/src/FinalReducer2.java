import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Rohini Yadav
 * Final Reducer which sorts the entries as per the descending order of average age
 *
 */

public class FinalReducer2 extends Reducer<Text, Text, Text, Text>
{
	ArrayList<Entry<String, Long>> keyValList = new ArrayList<Entry<String, Long>>();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException 
			{
		String address = null;
		long friendsAgeSum = 0;
		long noOfFriends = 0;

		// For each value check if the input is from the mapper emitting address
		// or age of the friends
		for (Text singleValue : values) 
		{	
			String inputString = singleValue.toString();

			// If input is address then it will contain ","
			if (!inputString.contains(",")) 
			{
				// Add the ages of all the friends
				friendsAgeSum += Long.parseLong(inputString);
				// Keep the count of friends to calculate average age
				noOfFriends++;
			}
			// If it is address just save it
			else 
			{
				address = inputString;
			}
		}

		// Calculate the average age and sort the average age in descending order
		//  <user Id:address> <average age of friends> 

		if (noOfFriends != 0)
		{
			Entry<String, Long> entry = new MyEntry(key.toString() + ", " + address, friendsAgeSum / noOfFriends);
			keyValList.add(entry);
		}
		
		// Sort the list according to the highest average age of friends
		// Break the tie with the small user id
		AvgAgeComparator comparator = new AvgAgeComparator();
		Collections.sort(keyValList, comparator);

}

	/**
	 * Process used to emit the final output
	 * <User id : his address> <Average age of his friends>
	 * Top 20 as per the average age in descending order
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException 
	{
		int count = 0;

		for (int i = 0; i < keyValList.size(); i++)
		{
			context.write(new Text(keyValList.get(i).getKey()), new Text(keyValList.get(i).getValue().toString()));
			count++;
			// Get only the top 20
			if (count == 20) 
			{
				break;
			}
		}


	}
}
