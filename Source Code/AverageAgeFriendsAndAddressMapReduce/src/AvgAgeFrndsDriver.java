import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AvgAgeFrndsDriver {

	public static int main(String[] args)
	{
		int exitCode = 0;
		Configuration conf = new Configuration();

		try 
		{
			Job job1 = new Job(conf);
			job1.setJobName("friendsAndAge");

			job1.setJarByClass(AvgAgeFrndsDriver.class);

			MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, FriendsMapper1.class);
			MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, AgeMapper2.class);
			job1.setReducerClass(FriendsAgeReducer1.class);

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			// Give the path for the result produced by job1 and store it to be used by job2
			String hdfsPathForIntermediate = "/rsy140030/temp" + System.currentTimeMillis();
			FileOutputFormat.setOutputPath(job1, new Path(hdfsPathForIntermediate));

			if (job1.waitForCompletion(true) == true) {

				// Create another job
				Job job2 = new Job(conf);
				job2.setJobName("getTop20AvgAgeFriendsAndAddress");
				// Set the driver, mapper and reducer classes
				// Set the input paths for each mapper
				job2.setJarByClass(AvgAgeFrndsDriver.class);

				// DummyMapper3 reads the result produced by job1
				MultipleInputs.addInputPath(job2, new Path(hdfsPathForIntermediate), TextInputFormat.class, DummyMapper3.class);
				// AddressMapper4 reads the data form the userdata file and emits the address of the user
				MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, AddressMapper4.class);
				// Set the reducer class
				job2.setReducerClass(FinalReducer2.class);

				// Set the output key and value 
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				// Set the final output file path from the argument passed
				FileOutputFormat.setOutputPath(job2, new Path(args[2]));

				// Submit the job and wait for it to complete
				exitCode = job2.waitForCompletion (true) ? 0 : 1;
			}
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}
		return exitCode;
	}
}