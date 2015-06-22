package itemBasedRec;

import hdfs.HDFSDao;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * 按用户分组，计算所有物品出现的组合列表，得到用户对物品的评分矩阵
 * 比如 value为 1,101,5；1,102，3 --> 1 101:5,102:3
 */
public class ItemBasedRecStep1 {

	public static final Pattern DELIMITER = Pattern.compile("[\t,]");

	public static class Step1_ToItemPreMapper extends MapReduceBase implements
			Mapper<Object, Text, IntWritable, Text> {

		private final static IntWritable k = new IntWritable();
		private final static Text v = new Text();

		public void map(Object key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			String[] tokens = DELIMITER.split(value.toString());
			int userID = Integer.parseInt(tokens[0]);
			String itemID = tokens[1];
			String pref = tokens[2];
			k.set(userID);
			v.set(itemID + ":" + pref);
			output.collect(k, v);
		}
	}

	public static class Step1_ToUserVectorReducer extends MapReduceBase
			implements Reducer<IntWritable, Text, IntWritable, Text> {

		private final static Text v = new Text();

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

		Configuration configuration = new Configuration();
		JobConf job = new JobConf(configuration, ItemBasedRecStep1.class);
		String input = path.get("Step1Input");
		String output = path.get("Step1Output");

		HDFSDao hdfsDao = new HDFSDao(configuration);
		hdfsDao.rmr(output);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step1_ToItemPreMapper.class);
		job.setCombinerClass(Step1_ToUserVectorReducer.class);
		job.setReducerClass(Step1_ToUserVectorReducer.class);

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
