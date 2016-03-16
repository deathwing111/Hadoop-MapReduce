import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Rohini Yadav
 * This is the mapper which reads the friends list form in memory and then emit
 *  <user1Id + user2Id> <name:zipcodes of friends>
 *
 */

public class MutualFrndsDetailsMapper extends Mapper<Object, Text, Text, Text>
{

	Configuration conf;
	HashMap<String,String> userHashMap;

	public void map(Object key, Text val, Context context)
	{
		// Get the users id passed to the driver as arguments
		conf = context.getConfiguration();
		
		String user1=conf.get("userId1");
		String user2=conf.get("userId2");

		String line = val.toString();
		String[] userDetails = line.split(",");
		String userId = userDetails[0];

		// Get the users friends list from the in memory data of friends list
		String user1Frnds = new String(userHashMap.get(user1));
		String user2Frnds = new String(userHashMap.get(user2));

		// Get the array of friends of both the users
		String[] user1FrndsArr = user1Frnds.split(",");
		String[] user2FrndsArr = user2Frnds.split(",");
		
		ArrayList<String> user1FrndsList = new ArrayList<String>(Arrays.asList(user1FrndsArr));
		ArrayList<String> user2FrndsList = new ArrayList<String>(Arrays.asList(user2FrndsArr));

		Text mapperOutputKey = new Text();   
		Text mapperOutputVal = new Text();

		// Take the intersection of the two user's friends' lists
		user1FrndsList.retainAll(user2FrndsList);

		if(user1FrndsList.contains(userId))
		{
			// Create the key as the concatenated string of user1 and user2
			String usersKey = user1+" "+user2;
			mapperOutputKey.set(usersKey);

			// Get the user name and zip code from the userdata file
			String frndsDetails = userDetails[1]+":"+userDetails[6];
			mapperOutputVal.set(frndsDetails);
			
			try {
				// Emit <userid1 + userid2> <name:zipcode of mutual friends>
				context.write(mapperOutputKey, mapperOutputVal);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This method is used to put the friends list in memory
	 * It reads the file containing user id and friends list
	 */ 
	@Override
	protected void setup(Context context) 
	{
		try 
		{
			super.setup(context);

			conf = context.getConfiguration();
			String frndsListInMemory = conf.get("friendList");

			FileSystem fs = FileSystem.get(conf);
			Path path=new Path(frndsListInMemory);
			FileStatus[] fileStatusArr = fs.listStatus(path);

			userHashMap = new HashMap<String,String>();
			
			int i = 0;
			while (i<fileStatusArr.length) 
			{
				Path path1 = fileStatusArr[i].getPath();		        
				BufferedReader buff=new BufferedReader(new InputStreamReader(fs.open(path1)));
				
				String line;
				line=buff.readLine();
				
				while (line != null)
				{
					String userNFrnds[]=line.split("\t");
					line=buff.readLine();
					if(userNFrnds.length == 2){
						userHashMap.put(userNFrnds[0].trim(), userNFrnds[1].trim());
					}else{
						userHashMap.put(userNFrnds[0].trim(), null);
					}
				}
				
				i++;
			} 
		} 
		catch (IOException | InterruptedException e) 
		{
			e.printStackTrace();
		}
	}
}