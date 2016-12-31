package org.tinylcy.similarity;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;


/*
 * 
 * 计算物品的相似度矩阵；
 * 首先需要将Step4和Step5的输出数据缓存到每个结点；
 * 输入数据为Step3的输出数据（物品的同现矩阵）；
 * 输出数据格式为：ItemID_i ItemID_j similarity;
 *
 */
public class CalculateSimilarityStep6 {

	private static final Pattern DELIMITER = Pattern.compile("[:, ]");

	public static class Step6_Mapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		// 在每个map结点缓存每个物品被几个用户喜欢
		private Map<Integer, Integer> map_A = new HashMap<Integer, Integer>();
		// 在每个map结点缓存每个用户喜欢几个物品
		private Map<Integer, Integer> map_B = new HashMap<Integer, Integer>();

		private Text k = new Text();
		private DoubleWritable v = new DoubleWritable();

		public void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();
			Path[] paths = DistributedCache.getLocalCacheFiles(conf);

			Path file1Path = paths[0];
			Path file2Path = paths[1];

			String line = null;
			String[] tokens = null;

			BufferedReader reader = new BufferedReader(new FileReader(
					file1Path.toString()));

			try {
				while ((line = reader.readLine()) != null) {
					tokens = line.split(":");
					map_A.put(Integer.parseInt(tokens[0]),
							Integer.parseInt(tokens[1]));
				}
			} finally {
				reader.close();
			}

			reader = new BufferedReader(new FileReader(file2Path.toString()));
			try {
				while ((line = reader.readLine()) != null) {
					tokens = line.split(":");
					map_B.put(Integer.parseInt(tokens[0]),
							Integer.parseInt(tokens[1]));
				}
			} finally {
				reader.close();
			}

		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = DELIMITER.split(value.toString());
			int itemID_a = Integer.parseInt(tokens[0]);
			int itemID_b = Integer.parseInt(tokens[1]);
			int userID = -1;
			double sum = 0.0;
			double similarity = 0.0;

			if (tokens.length > 2) {
				for (int i = 2; i < tokens.length; i++) {
					userID = Integer.parseInt(tokens[i]);
					sum += 1 / (Math.log(1 + map_B.get(userID)));
				}
				similarity = sum
						/ Math.sqrt(map_A.get(itemID_a) * map_A.get(itemID_b));
				k.set(itemID_a + " " + itemID_b);
				v.set(similarity);
				context.write(k, v);
			}
		}
	}

	public static void run() throws IOException, ClassNotFoundException,
			InterruptedException, URISyntaxException {

		Configuration conf = new Configuration();
		HDFS hdfs = new HDFS(conf);

		// hdfs.download(HDFS.HDFSPATH + "/step4/part-r-00000",
		// ItemBasedCFDriver.LOCALPATH + "/step4/part-r-00000");
		// hdfs.download(HDFS.HDFSPATH + "/step5/part-r-00000",
		// ItemBasedCFDriver.LOCALPATH + "/step5/part-r-00000");

		// String inputPath1 = "/var/ItemBased/step4/part-r-00000";//
		// 每个物品被几个用户喜欢
		// String inputPath2 = "/var/ItemBased/step5/part-r-00000";// 每个用户喜欢几个物品

		String inputPath1 = HDFS.HDFSPATH + "/step4/part-r-00000";
		String inputPath2 = HDFS.HDFSPATH + "/step5/part-r-00000";

		String inputPath3 = ItemBasedCFDriver.path.get("step6InputPath");
		String outputPath = ItemBasedCFDriver.path.get("step6OutputPath");

		conf.set("mapreduce.output.textoutputformat.separator", ":");

		DistributedCache.addCacheFile(new Path(inputPath1).toUri(), conf);
		DistributedCache.addCacheFile(new Path(inputPath2).toUri(), conf);

		Job job = Job.getInstance(conf);

		hdfs.rmr(outputPath);

		job.setMapperClass(Step6_Mapper.class);

		job.setJarByClass(CalculateSimilarityStep6.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath3));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

}
