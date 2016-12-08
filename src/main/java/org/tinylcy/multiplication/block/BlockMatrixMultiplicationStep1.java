package org.tinylcy.multiplication.block;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.tinylcy.driver.ItemBasedCFDriver;
import org.tinylcy.hdfs.HDFS;

import java.io.IOException;
import java.util.regex.Pattern;


public class BlockMatrixMultiplicationStep1 {

	private static final Pattern DELIMITER = Pattern.compile("[:\\s]");

	public static class BlockMatrixStep1_Mapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String parentName = fileSplit.getPath().getParent().getName();

			String[] tokens = DELIMITER.split(value.toString());

			if (parentName.equals("step6")) {
				outputKey.set("A");
				outputValue.set(tokens[0] + "," + tokens[1] + "," + tokens[2]);
				context.write(outputKey, outputValue);
			} else if (parentName.equals("step1")) {
				outputKey.set("B");
				outputValue.set(tokens[0] + "," + tokens[1] + "," + tokens[2]);
				context.write(outputKey, outputValue);
			}
		}
	}

	public static void run() throws IOException, ClassNotFoundException,
			InterruptedException {
		String inputPath1 = ItemBasedCFDriver.path.get("step7InputPath1");
		String inputPath2 = ItemBasedCFDriver.path.get("step7InputPath2");
		String outputPath = ItemBasedCFDriver.path.get("step7OutputPath");

		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");

		Job job = Job.getInstance(conf);

		HDFS
				hdfs = new HDFS(conf);
		hdfs.rmr(outputPath);

		job.setMapperClass(BlockMatrixStep1_Mapper.class);

		job.setJarByClass(BlockMatrixMultiplicationStep1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath1), new Path(
				inputPath2));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

	}
}
