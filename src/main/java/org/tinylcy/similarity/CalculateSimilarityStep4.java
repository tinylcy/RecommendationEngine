package org.tinylcy.similarity;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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


/*
 * 
 * 计算每个物品被几个用户喜欢；
 * 输入数据格式为：UserID ItemID 评分 时间戳；
 * 输出数据格式为：ItemID count；
 * 用于计算物品之间的相似度；
 *
 */
public class CalculateSimilarityStep4 {
	private static final Pattern DELIMITER = Pattern.compile("::|\t| +");

	public static class Step4_Mapper extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		private IntWritable k = new IntWritable();
		private IntWritable v = new IntWritable();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = DELIMITER.split(value.toString());
			k.set(Integer.parseInt(tokens[1]));
			v.set(Integer.parseInt(tokens[0]));
			context.write(k, v);
		}
	}

	public static class Step4_Reducer extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		private IntWritable v = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			Iterator<IntWritable> iterator = values.iterator();
			int count = 0;
			while (iterator.hasNext()) {
				count++;
				iterator.next();
			}
			v.set(count);
			context.write(key, v);
		}
	}

	public static void run() throws IOException, ClassNotFoundException,
			InterruptedException {
		String inputPath = ItemBasedCFDriver.path.get("step4InputPath");
		String outputPath = ItemBasedCFDriver.path.get("step4OutputPath");

		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ":");
		Job job = Job.getInstance(conf);

		HDFS hdfs = new HDFS(conf);
		hdfs.rmr(outputPath);

		job.setMapperClass(Step4_Mapper.class);
		job.setReducerClass(Step4_Reducer.class);

		job.setJarByClass(CalculateSimilarityStep4.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		run();
	}

}
