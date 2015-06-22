package itemSimilarity;

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

/*
 * 清洗数据
 */
public class ItemSimilarityStep1 {

	public static final Pattern DELIMITER = Pattern.compile("[\t,]");

	public static class Step1MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {

		private IntWritable k = new IntWritable();
		private Text v = new Text();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			String[] tokens = DELIMITER.split(value.toString());
			if (Integer.parseInt(tokens[2]) > 2) {
				k.set(Integer.parseInt(tokens[0]));
				v.set(tokens[1] + "," + tokens[2]);
				output.collect(k, v);
			}
		}
	}

	public static void run(Map<String, String> path) throws IOException {

		String input = path.get("SimilarityStep1Input");
		String output = path.get("SimilarityStep1Output");

		Configuration configuration = new Configuration();
		JobConf job = new JobConf(configuration, ItemSimilarityStep1.class);

		HDFSDao hdfsDao = new HDFSDao(configuration);
		hdfsDao.rmr(output);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(Step1MapClass.class);

		job.set("mapred.textoutputformat.separator", ",");

		RunningJob runningJob = JobClient.runJob(job);
		while (!runningJob.isComplete()) {
			runningJob.waitForCompletion();
		}

	}
}
