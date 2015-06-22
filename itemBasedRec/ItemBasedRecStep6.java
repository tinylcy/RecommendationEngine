package itemBasedRec;

import hdfs.HDFSDao;

import java.io.IOException;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import recommend.Recommend;

public class ItemBasedRecStep6 {

	public static final Pattern DELIMITER = Pattern.compile("[\t,]");

	public static class MapClasss extends MapReduceBase implements
			Mapper<Object, Text, IntWritable, Text> {

		private IntWritable key = new IntWritable();
		private Text value = new Text();

		public void map(Object k, Text v,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			String[] tokens = ItemBasedRecStep6.DELIMITER.split(v.toString());
			key.set(Integer.parseInt(tokens[0]));
			double score = Double.parseDouble(tokens[2]);
			value.set(tokens[1] + "," + score);
			output.collect(key, value);
		}

	}

	public static void run(Map<String, String> path) throws IOException {

		Configuration configuration = new Configuration();

		JobConf job = new JobConf(configuration, ItemBasedRecStep6.class);

		String input = path.get("Step6Input");
		String output = path.get("Step6Output");

		HDFSDao hdfsDao = new HDFSDao(Recommend.HDFS, new Configuration());
		hdfsDao.rmr(output);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(MapClasss.class);
		job.set("mapred.textoutputformat.separator", ",");

		RunningJob runningJob = JobClient.runJob(job);
		while (!runningJob.isComplete()) {
			runningJob.waitForCompletion();
		}

	}
}
