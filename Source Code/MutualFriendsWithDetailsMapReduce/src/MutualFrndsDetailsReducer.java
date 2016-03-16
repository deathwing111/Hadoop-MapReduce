import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Rohini Yadav
 * This is the Reducer which appends the name:zipcode pair for mutual friends
 *
 */
public class MutualFrndsDetailsReducer extends Reducer<Text,Text,Text,Text> 
{	
	public void reduce(Text key, Iterable<Text> values,Context context ) 
	{
		StringBuffer frndsList = new StringBuffer();
		
		for (Text val : values)
		{
			frndsList.append(val+", ");
		}
		
		String frndListWithDetails = frndsList.toString();
		String valToSet = "[ "+frndListWithDetails+" ]";
		
		Text finalVal = new Text();
		finalVal.set(valToSet);
		
		// Emit <key as it is from mapper (userId1+userId2)> <comma separated mutual friends details>
		try {
			context.write(key,finalVal);
		} 
		catch (IOException | InterruptedException e) 
		{
			e.printStackTrace();
		}
	}
}