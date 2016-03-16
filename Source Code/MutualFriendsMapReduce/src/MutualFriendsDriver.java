import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MutualFriendsDriver
{
	public static int main (String args[])
	{
		int exitCode = 0;
		
		// Check if the arguments are passed correctly otherwise throe error and exit
		if (args.length !=4)
		{
			System.err.println("Usage: MutualFriendsDriver <input path> <output path> <userId1> <userId2>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job;
		
		// Set the user ids passed in arguments in configuration
		conf.set("userId1", args[2]);
		conf.set("userId2", args[3]);
		
		try {
			job = new Job(conf);
			job.setJobName("MutualFriends");

			// Set the mapper and reducer classes
			job.setJarByClass(MutualFriendsDriver.class); 
			job.setMapperClass(MutualFriendsMapper.class);
			job.setReducerClass(MutualFriendsReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			exitCode = job.waitForCompletion (true) ? 0 : 1;
		}
		catch (ClassNotFoundException | InterruptedException e) 
		{
			e.printStackTrace();
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		
		return exitCode;
	}
}