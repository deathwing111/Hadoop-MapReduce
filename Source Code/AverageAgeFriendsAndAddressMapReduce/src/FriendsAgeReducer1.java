import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Rohini Yadav
 * This Reducer works on the output of the FriendsMapper1 and AgeMapper2
 * It checks if the input is from the FriendsMapper1 then it creates a list of friends of the user.
 * If the input is form the AgeMapper2 then it takes the age of the user.
 * It emits <Each friends Id> <age of the user(subject)> 
 * 
 */
public class FriendsAgeReducer1 extends Reducer <Text, Text, Text, Text>
{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
	{
		try 
		{
			List<String> friendsList = new ArrayList<>();
			String age = "";
			
			for (Text value : values) 
			{
				String inputString = value.toString();
				
				if (inputString.endsWith("f")) 
				{
					// Add the friend id and subtract the "frnd" suffix from the friend id
					friendsList.add(inputString.substring(0, inputString.length() - 1));
				}
				else 
				{
					//age = age + inputString;
					// Get the age of the user
					age = inputString;
				}
			}
			
			Text userAgeVal = new Text(age);
			
			// For each friend of the user emit <friend's Id> <age of the user(subject)>
			for (String singleFriend : friendsList)
			{
				context.write(new Text(singleFriend),userAgeVal); 
			}
		}
		catch (IOException | InterruptedException e) 
		{
			e.printStackTrace();
		}
	}	

}
