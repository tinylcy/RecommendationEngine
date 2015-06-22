package itemBasedRec;

import hdfs.HDFSDao;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
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

public class ItemBasedRecStep7 {

	public static class NormalizationMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {

		private IntWritable k = new IntWritable();
		private Text v = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			String[] tokens = value.toString().split(",");
			k.set(Integer.parseInt(tokens[0]));
			v.set(tokens[1] + "," + tokens[2]);
			output.collect(k, v);
		}

	}

	public static class NormalizationReducer extends MapReduceBase implements
			Reducer<IntWritable, Text, IntWritable, Text> {

		private Text v = new Text();

		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			ArrayList<String> list = new ArrayList<String>();

			double maxScore = Double.MIN_VALUE;
			while (values.hasNext()) {
				String value = values.next().toString();
				String[] tokens = value.split(",");
				double score = Double.parseDouble(tokens[1].toString());
				if (score > maxScore) {
					maxScore = score;
				}
				list.add(value);
			}

			DecimalFormat df = new DecimalFormat("#.###");
			Iterator<String> iterator = list.iterator();
			while (iterator.hasNext()) {
				String[] tokens = iterator.next().split(",");
				String normalizationScore = df.format(Double
						.parseDouble(tokens[1]) / maxScore * 5);
				v.set(tokens[0] + "," + normalizationScore);
				output.collect(key, v);
			}

		}
	}

	public static void run(Map<String, String> path) throws Exception {

		Configuration conf = new Configuration();
		JobConf job = new JobConf(conf, ItemBasedRecStep7.class);

		String input = path.get("Step7Input");
		String output = path.get("Step7Output");

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

		job.setMapperClass(NormalizationMapper.class);
		job.setReducerClass(NormalizationReducer.class);

		job.set("mapred.textoutputformat.separator", ",");

		RunningJob runningJob = JobClient.runJob(job);
		while (!runningJob.isComplete()) {
			runningJob.waitForCompletion();
		}

	}

}
