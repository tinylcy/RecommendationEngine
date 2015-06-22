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
 * 生成"物品-用户"倒排表，如：物品1：用户1，用户2，用户3...
 */
public class UserSimilarityStep2 {

	public static final Pattern DELIMITER = Pattern.compile("[:,]");

	public static class Step2MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {

		private IntWritable k = new IntWritable();
		private Text v = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			String[] tokens = DELIMITER.split(value.toString());
			v.set(tokens[0]);
			for (int i = 1; i < tokens.length; i++) {
				k.set(Integer.parseInt(tokens[i]));
				output.collect(k, v);
			}
		}
	}

	public static class Step2ReduceClass extends MapReduceBase implements
			Reducer<IntWritable, Text, IntWritable, Text> {

		private Text v = new Text();

		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			StringBuilder sBuilder = new StringBuilder();
			while (values.hasNext()) {
				sBuilder.append("," + values.next());
			}
			v.set(sBuilder.toString().replaceFirst(",", ""));
			output.collect(key, v);
		}

	}

	public static void run(Map<String, String> path) throws IOException {

		Configuration conf = new Configuration();
		JobConf job = new JobConf(conf, UserSimilarityStep2.class);

		String input = path.get("SimilarityStep2Input");
		String output = path.get("SimilarityStep2Output");

		HDFSDao hdfsDao = new HDFSDao(conf);
		hdfsDao.rmr(output);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step2MapClass.class);
		job.setReducerClass(Step2ReduceClass.class);

		job.set("mapred.textoutputformat.separator", ":");

		RunningJob runningJob = JobClient.runJob(job);
		while (!runningJob.isComplete()) {
			runningJob.waitForCompletion();
		}
	}

}
