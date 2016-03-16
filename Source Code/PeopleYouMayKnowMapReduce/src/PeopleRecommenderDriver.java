import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;



public class PeopleRecommenderDriver {
	public static int main(String[] args)
	{
		int exitCode = 0;
		
		// Check if the arguments are passed correctly otherwise throe error and exit
		if (args.length !=2)
		{
			System.err.println("Usage: MutualFriendsDriver <input path> <output path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();

		Job job;
		try {
			job = new Job (conf);
			job.setJobName("FriendsRecommender");

			// This will tell Hadoop to ship around the jar file containing 
			// "PeopleRecommenderDriver.class" to all of the different
			// nodes so that they can run the job
			job.setJarByClass(PeopleRecommenderDriver.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(PeopleRecommenderMapper.class);
			job.setReducerClass(PeopleRecommenderReducer.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			// Submit the job and wait for it to complete
			exitCode = job.waitForCompletion (true) ? 0 : 1;
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		
		return exitCode; 
	}
}