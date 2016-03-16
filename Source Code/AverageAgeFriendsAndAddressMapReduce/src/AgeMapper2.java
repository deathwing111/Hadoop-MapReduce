import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Rohini Yadav
 * This Mapper works in parallel with the FriendsMapper1
 * It reads the user data file and emits the <user Id> <age of the user>
 */
public class AgeMapper2 extends Mapper<LongWritable, Text, Text, Text> 
{
	@Override
	protected void map(LongWritable key, Text value, Context context) 
	{
		try 
		{
			String line = value.toString();

			String lineSplitArr[] = line.split(",");
			
			Text userId = new Text(lineSplitArr[0]);
			
			// Only take the records which are valid (of length 10)
			if (lineSplitArr.length == 10)
			{
				// Get the birth year of the user
				int userBirthYear = Integer.parseInt(lineSplitArr[9].split("/")[2]);
				
				// Get the current year
				int currentYear = Calendar.getInstance().get(Calendar.YEAR); 

				// Calculate the age of the user
				int userAge = currentYear - userBirthYear;

				// Emit <user Id> <user's age>
				context.write(userId, new Text(String.valueOf(userAge)));
			}					
		}
		catch (IOException | InterruptedException e) 
		{
			e.printStackTrace();
		}
	}
}
