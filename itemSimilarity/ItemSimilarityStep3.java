package itemSimilarity;

import hdfs.HDFSDao;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * 输入数据为Step1的输出数据，数据格式为【itemID：userID1，userID2，userID3...】
 * map输出为【userID，itemID】，
 * reduce输出【userID，itemID1，itemID2...】
 * 即统计每个用户观看过哪些电影
 */
public class ItemSimilarityStep3 {

	public static final Pattern DELIMITER = Pattern.compile("[\t:,]");

	public static class Step3MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text k = new Text();
		private Text v = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String[] tokens = DELIMITER.split(value.toString());
			for (int i = 1; i < tokens.length; i++) {
				k.set(tokens[i]);
				v.set(tokens[0]);
				output.collect(k, v);
			}

		}

	}

	public static class Step3ReduceClass extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private Text v = new Text();

		public void reduce(Text key, Iterator<Text> values,
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

		String input = path.get("SimilarityStep3Input");
		String output = path.get("SimilarityStep3Output");

		Configuration configuration = new Configuration();
		JobConf job = new JobConf(configuration, ItemSimilarityStep3.class);

		HDFSDao hdfsDao = new HDFSDao(configuration);
		hdfsDao.rmr(output);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step3MapClass.class);
		job.setReducerClass(Step3ReduceClass.class);

		job.set("mapred.textoutputformat.separator", ":");

		RunningJob runningJob = JobClient.runJob(job);
		while (!runningJob.isComplete()) {
			runningJob.waitForCompletion();
		}

	}
}
