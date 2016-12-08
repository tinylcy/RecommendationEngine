package org.tinylcy.multiplication.j;


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
import org.tinylcy.driver.ItemBasedCFDriver;
import org.tinylcy.hdfs.HDFS;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.regex.Pattern;


/**
 * 
 * 矩阵乘法 步骤一
 *
 */

public class JMatrixMultiplicationStep2 {

	private static final Pattern DELIMITER = Pattern.compile("[,]");

	public static class Step2_Mapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = DELIMITER.split(value.toString());
			if (tokens[0].equals("A")) {
				k.set(tokens[2]);
				v.set("A," + tokens[1] + "," + tokens[3]);
				context.write(k, v);
			} else if (tokens[0].equals("B")) {
				k.set(tokens[1]);
				v.set("B," + tokens[2] + "," + tokens[3]);
				context.write(k, v);
			}
		}
	}

	public static class Step2_Reducer extends
			Reducer<Text, Text, NullWritable, Text> {

		private Text v = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String[] tokens = null;
			ArrayList<Entry<Integer, Double>> listA = new ArrayList<Entry<Integer, Double>>();
			ArrayList<Entry<Integer, Integer>> listB = new ArrayList<Entry<Integer, Integer>>();
			Iterator<Text> iterator = values.iterator();
			while (iterator.hasNext()) {
				tokens = DELIMITER.split(iterator.next().toString());
				if (tokens[0].equals("A")) {
					listA.add(new SimpleEntry<Integer, Double>(Integer
							.parseInt(tokens[1]), Double.parseDouble(tokens[2])));
				} else if (tokens[0].equals("B")) {
					listB.add(new SimpleEntry<Integer, Integer>(Integer
							.parseInt(tokens[1]), Integer.parseInt(tokens[2])));
				}
			}

			String i = null;
			Double A_ij = null;
			String k = null;
			Integer B_jk = null;

			for (Entry<Integer, Double> a : listA) {
				i = a.getKey().toString();
				A_ij = a.getValue();
				for (Entry<Integer, Integer> b : listB) {
					k = b.getKey().toString();
					B_jk = b.getValue();
					v.set(i + "," + k + "," + String.valueOf(A_ij * B_jk));
					context.write(null, v);
				}
			}
		}
	}

	public static void run() throws IOException, ClassNotFoundException,
			InterruptedException {
		String inputPath = ItemBasedCFDriver.path.get("step8InputPath");
		String outputPath = ItemBasedCFDriver.path.get("step8OutputPath");

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		HDFS hdfs = new HDFS(conf);
		hdfs.rmr(outputPath);

		job.setMapperClass(Step2_Mapper.class);
		job.setReducerClass(Step2_Reducer.class);
		job.setJarByClass(JMatrixMultiplicationStep2.class);
		job.setNumReduceTasks(ItemBasedCFDriver.ReducerNumber);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

}
