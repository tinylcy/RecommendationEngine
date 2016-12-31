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
 * 计算 用户-物品倒排表；
 * 输入数据格式为：UserID ItemID 评分 时间戳；
 * 输出数据格式为：UserID ItemID1 ItemID2 ...
 * 两个作用：
 * 1.用于计算每个用户喜欢的物品的个数（假设评分了就是喜欢）；
 * 2.用户计算物品与物品的同现矩阵（concurrence matrix）；
 */

public class CalculateSimilarityStep2 {
	private static final Pattern DELIMITER = Pattern.compile("::|\t| +");

	public static class Step2_Mapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		private IntWritable k = new IntWritable();
		private Text v = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = DELIMITER.split(value.toString());
			k.set(Integer.parseInt(tokens[0]));
			v.set(tokens[1]);
			context.write(k, v);
		}
	}

	public static class Step2_Reducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		private Text v = new Text();

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Iterator<Text> iterator = values.iterator();
			StringBuilder builder = new StringBuilder();
			while (iterator.hasNext()) {
				builder.append("," + iterator.next().toString());
			}
			v.set(builder.toString().replaceFirst(",", ""));
			context.write(key, v);
		}
	}

	public static void run() throws IOException, ClassNotFoundException,
			InterruptedException {

		String inputPath = ItemBasedCFDriver.path.get("step2InputPath");
		String outputPath = ItemBasedCFDriver.path.get("step2OutputPath");

		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ":");

		Job job = Job.getInstance(conf);

		HDFS hdfs = new HDFS(conf);
		hdfs.rmr(outputPath);

		job.setMapperClass(Step2_Mapper.class);
		job.setReducerClass(Step2_Reducer.class);
		job.setCombinerClass(Step2_Reducer.class);
		job.setNumReduceTasks(ItemBasedCFDriver.ReducerNumber);

		job.setJarByClass(CalculateSimilarityStep2.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

}
