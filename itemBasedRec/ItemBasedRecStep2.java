package itemBasedRec;

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
 * 计算同现矩阵：将Step1的输出作为Step2的输入
 * 比如，对于一行数据：1 101:5.0，102:3.0,103:2.5
 * map输出：101:101 1,101:102 1,101:103 1,102:101 1,102:102 1,
 *         102:103 1,103:101 1，103:102 1,103:103 1
 */
public class ItemBasedRecStep2 {

	public static final Pattern DELIMITER = Pattern.compile("[\t,]");

	public static class Step2_UserVectorToCooccurrenceMapper extends
			MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static Text k = new Text();
		private final static IntWritable v = new IntWritable(1);

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String[] tokens = DELIMITER.split(value.toString());
			for (int i = 1; i < tokens.length; i++) {
				String itemID = tokens[i].split(":")[0];
				for (int j = 1; j < tokens.length; j++) {
					String itemID2 = tokens[j].split(":")[0];
					k.set(itemID + ":" + itemID2);
					output.collect(k, v);
				}
			}
		}

	}

	public static class Step2_UserVectorToConoccurrenceReducer extends
			MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			result.set(sum);
			output.collect(key, result);
		}

	}

	public static void run(Map<String, String> path) throws IOException {

		Configuration configuration = new Configuration();
		JobConf job = new JobConf(configuration, ItemBasedRecStep2.class);
		String input = path.get("Step2Input");
		String output = path.get("Step2Output");

		HDFSDao hdfsDao = new HDFSDao(configuration);
		hdfsDao.rmr(output);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Step2_UserVectorToCooccurrenceMapper.class);
		job.setCombinerClass(Step2_UserVectorToConoccurrenceReducer.class);
		job.setReducerClass(Step2_UserVectorToConoccurrenceReducer.class);

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
