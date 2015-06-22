package itemBasedRec;

import hdfs.HDFSDao;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class ItemBasedRecStep3 {

	public static final Pattern DELIMITER = Pattern.compile("[\t,]");

	public static class Step31_UserVectorSplitterMapper extends MapReduceBase
			implements Mapper<LongWritable, Text, IntWritable, Text> {

		private final static IntWritable k = new IntWritable();
		private final static Text v = new Text();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			String[] tokens = DELIMITER.split(value.toString());
			for (int i = 1; i < tokens.length; i++) {
				String[] vector = tokens[i].split(":");
				int itemID = Integer.parseInt(vector[0]);
				String pref = vector[1];

				k.set(itemID);
				v.set(tokens[0] + ":" + pref);
				output.collect(k, v);
			}
		}

	}

	public static void run1(Map<String, String> path) throws IOException {

		Configuration configuration = new Configuration();
		JobConf job = new JobConf(configuration, ItemBasedRecStep3.class);

		String input = path.get("Step3Input1");
		String output = path.get("Step3Output1");

		HDFSDao hdfsDao = new HDFSDao(configuration);

		hdfsDao.rmr(output);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step31_UserVectorSplitterMapper.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		RunningJob runningJob = JobClient.runJob(job);
		while (!runningJob.isComplete()) {
			runningJob.waitForCompletion();
		}
	}

	public static class Step32_CooccurrenceColumnWrapperMapper extends
			MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static Text k = new Text();
		private final static IntWritable v = new IntWritable();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String[] tokens = DELIMITER.split(value.toString());
			k.set(tokens[0]);
			v.set(Integer.parseInt(tokens[1]));
			output.collect(k, v);
		}

	}

	public static void run2(Map<String, String> path) throws IOException {

		Configuration configuration = new Configuration();
		JobConf job = new JobConf(configuration, ItemBasedRecStep3.class);

		String input = path.get("Step3Input2");
		String output = path.get("Step3Output2");

		HDFSDao hdfsDao = new HDFSDao(configuration);

		hdfsDao.rmr(output);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Step32_CooccurrenceColumnWrapperMapper.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		RunningJob runningJob = JobClient.runJob(job);
		while (!runningJob.isComplete()) {
			runningJob.waitForCompletion();
		}
	}
}
