package org.tinylcy.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.tinylcy.hdfs.HDFS;

import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Pattern;

/*
 * 计算训练数据中实际被打过分的物品的个数
 */
public class CalculateItemNumber {
	public static class ItemNumberMapper extends
			Mapper<LongWritable, Text, NullWritable, NullWritable> {

		private static final Pattern DELIMITER = Pattern.compile("::|\t");
		private HashSet<Integer> itemIDs = new HashSet<Integer>();

		public void map(LongWritable key, Text value, Context context) {

			String[] tokens = DELIMITER.split(value.toString());
			int itemID = Integer.parseInt(tokens[1]);
			if (!itemIDs.contains(itemID)) {
				itemIDs.add(itemID);
			}
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			System.out.println("-------itemIDs.size()=" + itemIDs.size()
					+ "-------");
		}
	}

	public static void run() throws IOException, ClassNotFoundException,
			InterruptedException {
		String inputPath = HDFS.HDFSPATH + "/ratings.dat";

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setMapperClass(ItemNumberMapper.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));

		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		run();
	}

}
