package org.tinylcy.driver;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.tinylcy.hdfs.HDFS;

import java.io.IOException;
import java.util.Iterator;

public class ProduceRecords {

	public static class ProduceRecordsMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] tokens = value.toString().split("::");
			int userID = Integer.parseInt(tokens[0]);
			if (userID <= 5000) {
				context.write(value, NullWritable.get());
			}
		}
	}

	public static class ProduceRecordsReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			Iterator<NullWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				context.write(key, iterator.next());
			}
		}
	}

	public static void run() throws IOException, ClassNotFoundException,
			InterruptedException {
		String inputPath = HDFS.HDFSPATH + "/ratings.dat";
		String outputPath = HDFS.HDFSPATH + "/data";

		Configuration conf = new Configuration();

		HDFS hdfs = new HDFS(conf);
		hdfs.rmr(outputPath);

		Job job = Job.getInstance(conf);

		job.setMapperClass(ProduceRecordsMapper.class);
		job.setReducerClass(ProduceRecordsReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		run();
	}
}
