package userSimilarity;

import hdfs.HDFSDao;

import java.io.IOException;
import java.util.Iterator;
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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/*
 * 统计一个物品的热门程度，即被多少用户喜欢过。
 */
public class UserSimilarityStep3 {

	public static final Pattern DELIMITER = Pattern.compile("[\t:,]");

	public static class Step3MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		private IntWritable k = new IntWritable();
		private IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {

			String[] tokens = DELIMITER.split(value.toString());
			for (int i = 1; i < tokens.length; i++) {
				k.set(Integer.parseInt(tokens[i]));
				output.collect(k, one);
			}
		}

	}

	public static class Step3ReduceClass extends MapReduceBase implements
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		private IntWritable v = new IntWritable();

		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {

			int result = 0;
			while (values.hasNext()) {
				result += Integer.parseInt(values.next().toString());
			}
			v.set(result);
			output.collect(key, v);
		}

	}

	public static void run(Map<String, String> path) throws IOException {

		String input = path.get("SimilarityStep3Input");
		String output = path.get("SimilarityStep3Output");

		Configuration configuration = new Configuration();
		JobConf job = new JobConf(configuration, UserSimilarityStep3.class);

		HDFSDao hdfsDao = new HDFSDao(configuration);
		hdfsDao.rmr(output);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Step3MapClass.class);
		job.setCombinerClass(Step3ReduceClass.class);
		job.setReducerClass(Step3ReduceClass.class);

		job.set("mapred.textoutputformat.separator", ":");

		RunningJob runningJob = JobClient.runJob(job);
		while (!runningJob.isComplete()) {
			runningJob.waitForCompletion();
		}
	}
}
