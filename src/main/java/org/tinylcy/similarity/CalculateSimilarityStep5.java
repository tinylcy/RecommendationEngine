package org.tinylcy.similarity;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.tinylcy.driver.ItemBasedCFDriver;
import org.tinylcy.hdfs.HDFS;

import java.io.IOException;
import java.util.regex.Pattern;


/*
 * 
 * 计算每个用户喜欢几个物品；
 * 输入数据为Step2的输出数据(用户-物品倒排表)；
 * 只需要一个Mapper；
 * 输出数据格式：UserID count
 * 用于计算物品之间的相似度。
 *
 */
public class CalculateSimilarityStep5 {

	private static final Pattern DELIMITER = Pattern.compile("[,:]");

	public static class Step5_Mapper extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		private IntWritable k = new IntWritable();
		private IntWritable v = new IntWritable();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = DELIMITER.split(value.toString());
			k.set(Integer.parseInt(tokens[0]));
			v.set(tokens.length - 1);
			context.write(k, v);
		}
	}

	public static void run() throws IOException, ClassNotFoundException,
			InterruptedException {
		String inputPath = ItemBasedCFDriver.path.get("step5InputPath");
		String outputPath = ItemBasedCFDriver.path.get("step5OutputPath");

		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ":");

		Job job = Job.getInstance(conf);
		HDFS hdfs = new HDFS(conf);
		hdfs.rmr(outputPath);

		job.setMapperClass(Step5_Mapper.class);

		job.setJarByClass(CalculateSimilarityStep5.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

}
