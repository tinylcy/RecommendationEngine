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
 * 计算用户对物品的评分矩阵，只需要一个Mapper即可；
 * 输入数据格式为：UserID ItemID 评分 时间戳； 
 * 输出数据格式为：ItemID UserID 评分；
 * 设置ItemID在前的原因是根据用户评分矩阵的格式。
 */

public class CalculateSimilarityStep1 {

	private static final Pattern DELIMITER = Pattern.compile("::|\t| +");

	public static class Step1_Mapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		private IntWritable k = new IntWritable();
		private Text v = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = DELIMITER.split(value.toString());
			k.set(Integer.parseInt(tokens[1]));
			v.set(tokens[0] + " " + tokens[2]);
			context.write(k, v);
		}
	}

	public static void run() throws IOException, ClassNotFoundException,
			InterruptedException {
		String inputPath = ItemBasedCFDriver.path.get("step1InputPath");
		String outputPath = ItemBasedCFDriver.path.get("step1OutputPath");

		Configuration conf = new Configuration();

		conf.set("mapred.textoutputformat.separator", " ");

		Job job = Job.getInstance(conf);

		HDFS hdfs = new HDFS(conf);
		hdfs.rmr(outputPath);

		job.setMapperClass(Step1_Mapper.class);

		job.setJarByClass(CalculateSimilarityStep1.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		run();
	}

}
