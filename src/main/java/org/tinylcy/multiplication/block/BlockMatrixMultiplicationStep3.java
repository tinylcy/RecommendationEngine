package org.tinylcy.multiplication.block;


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


public class BlockMatrixMultiplicationStep3 {

	public static class BlockMatrixStep3_Mapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		private Text outputKey = new Text();
		private DoubleWritable outputValue = new DoubleWritable();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			outputKey.set(tokens[1] + "," + tokens[0]);
			outputValue.set(Double.parseDouble(tokens[2]));
			context.write(outputKey, outputValue);
		}

	}

	public static class BlockMatrixStep3_Reducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		private DoubleWritable outputValue = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			Iterator<DoubleWritable> iterator = values.iterator();
			double sum = 0.0;
			while (iterator.hasNext()) {
				sum += Double.parseDouble(iterator.next().toString());
			}
			outputValue.set(sum);
			context.write(key, outputValue);
		}
	}

	public static void run() throws IOException, ClassNotFoundException,
			InterruptedException {

		String inputPath = ItemBasedCFDriver.path.get("step9InputPath");
		String outputPath = ItemBasedCFDriver.path.get("step9OutputPath");

		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");

		HDFS hdfs = new HDFS(conf);
		hdfs.rmr(outputPath);

		Job job = Job.getInstance(conf);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(BlockMatrixStep3_Mapper.class);
		job.setReducerClass(BlockMatrixStep3_Reducer.class);
		job.setCombinerClass(BlockMatrixStep3_Reducer.class);
		job.setNumReduceTasks(ItemBasedCFDriver.ReducerNumber);

		job.setJarByClass(BlockMatrixMultiplicationStep3.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		long beginTime = System.currentTimeMillis();
		run();
		long endTime = System.currentTimeMillis();
		System.out.println("耗时：" + (endTime - beginTime) + "ms");
	}

}
