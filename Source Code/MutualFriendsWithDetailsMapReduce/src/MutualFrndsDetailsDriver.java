import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class MutualFrndsDetailsDriver
{
	public static int main(String[] args) throws Exception 
	{

		int exitCode = 0;

		// Check if the arguments are passed correctly otherwise throe error and exit
		if (args.length !=5)
		{
			System.err.println("Usage: MutualFrndsDetailsDriver <User's friends list file> <User data file> <output path> <userId1> <userId2>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		// Get the command line arguments
		String[] frndList = new GenericOptionsParser(conf, args).getRemainingArgs();		
		
		conf.set("userId1", args[3]);
		conf.set("userId2", args[4]);
		conf.set("friendList", frndList[0]);
		
		Job job = new Job(conf);
		// Set Mapper and Reducer classes
		job.setJobName("MutualFriendsWithDetails");
		job.setJarByClass(MutualFrndsDetailsDriver.class);
		job.setMapperClass(MutualFrndsDetailsMapper.class);
		job.setReducerClass(MutualFrndsDetailsReducer.class);
		
		// Set the output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(frndList[1]));
		FileOutputFormat.setOutputPath(job, new Path(frndList[2]));
		
		// Submit the job and wait for it to complete
		exitCode = job.waitForCompletion (true) ? 0 : 1;
		
		return exitCode;
	}
}
