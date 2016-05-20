import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.regex.*;


public class CrimeData20142015 {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		// private final static IntWritable one = new IntWritable(1);
		// private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			int Count = 0;

			int found = 0;

			String Key = null;

			String line = value.toString();
			
			int Is_PrimaryType_THEFT = 0;
			int Is_Arrest_True = 0;

			for (String retval : line.split(",")) {
				// System.out.println(retval);
				Count = Count + 1;

				// Data = 3
				// Arrest = 9
				if ((Count == 3)) {
					String pattern = "(\\d+)/(\\d+)/(\\d+)";
					Pattern p = Pattern.compile(pattern);
					Matcher m = p.matcher(retval);

					if (m.find()) {
						int month = Integer.parseInt(m.group(1));
						int Year = Integer.parseInt(m.group(3));

						if (((month >= 10) && (Year == 2014)) || ((month <= 10) && (Year == 2015))) {
							System.out.println("month" + month);
							System.out.println("Year" + Year);
							Is_PrimaryType_THEFT = 1;
						}

					}

				}

				if ((Count == 6) && (retval.matches("^THEFT$"))) {
					// System.out.println(retval);

					Is_PrimaryType_THEFT = 1;
				}

				if ((Is_PrimaryType_THEFT == 1) && ((Count == 9) && (retval.matches("^true$")))) {
					Is_Arrest_True = 1;
				}

				if ((Is_PrimaryType_THEFT == 1) && (Is_Arrest_True == 1) ) {
					// System.out.println(retval);
					Key = "Theft between 2014 and 2015";
					//System.out.println(Key);

					Is_PrimaryType_THEFT = 0;
					Is_Arrest_True = 0;
					found = 1;
				}

			}
			if (found == 1) {
				// System.out.println(Key + " " + 1);
				Count = 0;
				found = 0;
				context.write(new Text(Key), new IntWritable(1));
			}
			
		}
	}

	// Reducer function after collecting the intermediate data count the values
	// according to each state
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum = sum + val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		// Configuration conf = new Configuration();

		Job job = new Job();
		job.setJarByClass(CrimeData20142015.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}