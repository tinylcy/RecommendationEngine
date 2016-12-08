package org.tinylcy.multiplication.j;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.tinylcy.driver.ItemBasedCFDriver;
import org.tinylcy.hdfs.HDFS;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;


public class JMatrixMultiplicationStep3 {

	private static final Pattern DELIMITER = Pattern.compile("[,]");

	public static class Step3_Mapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		private Text k = new Text();
		private DoubleWritable v = new DoubleWritable();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = DELIMITER.split(value.toString());
			k.set(tokens[1] + "," + tokens[0]);
			v.set(Double.parseDouble(tokens[2]));
			context.write(k, v);
		}
	}

	public static class Step3_Reducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		private DoubleWritable v = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			Iterator<DoubleWritable> iterator = values.iterator();
			Double sum = 0.0;
			while (iterator.hasNext()) {
				sum += Double.parseDouble(iterator.next().toString());
			}
			v.set(sum);
			context.write(key, v);
		}
	}

	public static void run() throws IOException, ClassNotFoundException,
			InterruptedException {
		String inputPath = ItemBasedCFDriver.path.get("step9InputPath");
		String outputPath = ItemBasedCFDriver.path.get("step9OutputPath");

		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");

		Job job = Job.getInstance(conf);

		HDFS hdfs = new HDFS(conf);
		hdfs.rmr(outputPath);

		job.setMapperClass(Step3_Mapper.class);
		job.setReducerClass(Step3_Reducer.class);
		job.setCombinerClass(Step3_Reducer.class);
		job.setJarByClass(JMatrixMultiplicationStep3.class);
		job.setNumReduceTasks(ItemBasedCFDriver.ReducerNumber);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

}
