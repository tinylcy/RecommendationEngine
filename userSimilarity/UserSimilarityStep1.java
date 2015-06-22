package userSimilarity;

import hdfs.HDFSDao;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

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
 * 清洗数据，把一个用户给一部电影打分超过3分
 * 的观看记录清洗出来作为计算用户相似度的依据
 */
public class UserSimilarityStep1 {

	public static class Step1MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {

		private IntWritable k = new IntWritable();
		private Text v = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			String[] tokens = value.toString().split(",");
			if (Double.parseDouble(tokens[2]) >= 3.0) {
				k.set(Integer.parseInt(tokens[0]));
				v.set(tokens[1]);
				output.collect(k, v);
			}
		}
	}

	public static class Step1ReduceClass extends MapReduceBase implements
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
		JobConf job = new JobConf(conf, UserSimilarityStep1.class);

		String input = path.get("SimilarityStep1Input");
		String output = path.get("SimilarityStep1Output");

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

		job.setMapperClass(Step1MapClass.class);
		job.setReducerClass(Step1ReduceClass.class);

		job.set("mapred.textoutputformat.separator", ":");

		RunningJob runningJob = JobClient.runJob(job);
		while (!runningJob.isComplete()) {
			runningJob.waitForCompletion();
		}
	}

}
