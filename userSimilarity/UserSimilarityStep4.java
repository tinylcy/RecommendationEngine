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

public class UserSimilarityStep4 {

	public static final Pattern DELIMITER = Pattern.compile("[:,]");

	public static class Step4MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private Text k = new Text();
		private IntWritable v = new IntWritable();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String[] tokens = DELIMITER.split(value.toString());
			for (int i = 1; i < tokens.length; i++) {
				String userID1 = tokens[i];
				for (int j = 1; j < tokens.length; j++) {
					String userID2 = tokens[j];
					k.set(userID1 + ":" + userID2);
					if (i != j) {
						v.set(Integer.parseInt(tokens[0]));
						output.collect(k, v);
					}
				}
			}
		}
	}

	public static class Step4ReduceClass extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, Text> {

		private Text v = new Text();

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			StringBuilder sBuilder = new StringBuilder();
			while (values.hasNext()) {
				sBuilder.append("," + values.next().toString());
			}
			v.set(sBuilder.toString().replaceFirst(",", ""));
			output.collect(key, v);
		}
	}

	public static void run(Map<String, String> path) throws IOException {

		Configuration conf = new Configuration();
		JobConf job = new JobConf(conf, UserSimilarityStep4.class);

		String input = path.get("SimilarityStep4Input");
		String output = path.get("SimilarityStep4Output");

		HDFSDao hdfsDao = new HDFSDao(conf);
		hdfsDao.rmr(output);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step4MapClass.class);
		job.setReducerClass(Step4ReduceClass.class);

		job.set("mapred.textoutputformat.separator", ":");

		RunningJob runningJob = JobClient.runJob(job);
		while (!runningJob.isComplete()) {
			runningJob.waitForCompletion();
		}
	}
}
