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

/*
 *该类用于获取输入记录中最大的ItemID，
 *通过一个mapper遍历即可。 
 */

public class CalculateMaxItemID {

	// private static final Pattern DELIMITER = Pattern.compile("::|\t| +");

	public static class ItemNumberMapper extends
			Mapper<LongWritable, Text, NullWritable, NullWritable> {
		// 只需要一个ItemID，没有必要写HDFS，输出key和value类型均设为NullWritable

		private int maxItemID = Integer.MIN_VALUE;

		public void map(LongWritable key, Text value, Context context) {
			String line = value.toString();
			// String[] tokens = DELIMITER.split(line);
			String[] tokens = line.split(",");
			int itemID = Integer.parseInt(tokens[1]);
			if (itemID > maxItemID) {
				maxItemID = itemID;
			}
		}

		/*
		 * cleanup()会在每个mapper执行完之后再调用
		 */
		public void cleanup(
				Context context)
				throws IOException, InterruptedException {

			System.out.println("---------------MAX_ITEM_ID=" + maxItemID
					+ "---------------");

		}
	}

	public static void run() throws IOException, ClassNotFoundException,
			InterruptedException {
		// String inputPath = HDFS.HDFSPATH + ItemBasedCFDriver.FileName;
		String inputPath = HDFS.HDFSPATH + "/ratings.csv";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setMapperClass(ItemNumberMapper.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		// Hadoop只有在输出类型为NullOutputFormat才可以不写HDFS，否则会报没有设置输出路径的错误
		job.setOutputFormatClass(NullOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));

		job.waitForCompletion(true);

	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		run();
	}
}
