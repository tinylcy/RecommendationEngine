package itemSimilarity;

import hdfs.HDFSDao;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/*
 * 输入数据为Step2、Step3、Step4的输出，
 * Step1输出用来计算每部电影被多少用户观看过，用A标记；
 * Step2输出用来计算每个用户观看过的电影的数量，用B标记；
 * Step3输出用来计算两部电影之间被哪些用户观看过，用C标记；
 */
public class ItemSimilarityStep5 {

	public static final Pattern DELIMITER = Pattern.compile("[\t:,]");

	public static class Step5MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text k = new Text();
		private Text v = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
			String fileName = fileSplit.getPath().getParent().getName();

			if (fileName.equals("ItemSimilarityStep2")) {
				k.set("A");
				v.set(value.toString());
				output.collect(k, v);
			} else {
				if (fileName.equals("ItemSimilarityStep3")) {
					k.set("B");
					v.set(value.toString());
					output.collect(k, v);
				} else {
					if (fileName.equals("ItemSimilarityStep4")) {
						k.set("C");
						v.set(value.toString());
						output.collect(k, v);
					}
				}
			}

		}

	}

	public static class Step5ReduceClass extends MapReduceBase implements
			Reducer<Text, Text, Text, DoubleWritable> {

		private Text k = new Text();
		private DoubleWritable v = new DoubleWritable();

		private Map<Integer, Integer> map_A = new HashMap<Integer, Integer>();// 用来存储每部电影被几个用户观看过
		private Map<Integer, Integer> map_B = new HashMap<Integer, Integer>();// 用来存储每个用户观看过的电影数量
		private Map<String, ArrayList<Integer>> map_C = new HashMap<String, ArrayList<Integer>>();// 用来存储两部电影被哪些用户同时观看过

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {

			while (values.hasNext()) {
				String line = values.next().toString();
				String[] tokens = DELIMITER.split(line);
				if (key.toString().equals("A")) {
					map_A.put(Integer.parseInt(tokens[0]), tokens.length - 1);
				} else {
					if (key.toString().equals("B")) {
						map_B.put(Integer.parseInt(tokens[0]),
								tokens.length - 1);
					} else {
						if (key.toString().equals("C")) {
							String itemID1_itemID2 = tokens[0] + ":"
									+ tokens[1];
							for (int i = 2; i < tokens.length; i++) {
								if (!map_C.containsKey(itemID1_itemID2)) {
									map_C.put(itemID1_itemID2,
											new ArrayList<Integer>());
									map_C.get(itemID1_itemID2).add(
											Integer.parseInt(tokens[i]));
								} else {
									map_C.get(itemID1_itemID2).add(
											Integer.parseInt(tokens[i]));
								}
							}
						}
					}
				}
			}

			for (String value : map_C.keySet()) {
				String[] tokens = DELIMITER.split(value);
				int itemID1 = Integer.parseInt(tokens[0]);
				int itemID2 = Integer.parseInt(tokens[1]);
				int itemID1Num = map_A.get(itemID1);
				int itemID2Num = map_A.get(itemID2);

				ArrayList<Integer> commonUserID = new ArrayList<Integer>();
				commonUserID = map_C.get(itemID1 + ":" + itemID2);
				double sum = 0.0;
				for (Integer userID : commonUserID) {
					sum += Math.log(1 + map_B.get(userID));
				}
				double degree = (1 / sum) / Math.sqrt(itemID1Num * itemID2Num);
				DecimalFormat df = new DecimalFormat("#.#####");
				degree = Double.parseDouble(df.format(degree));
				k.set(itemID1 + ":" + itemID2);
				v.set(degree);
				output.collect(k, v);

			}

		}
	}

	public static void run(Map<String, String> path) throws IOException {

		String input1 = path.get("SimilarityStep5Input_1");
		String input2 = path.get("SimilarityStep5Input_2");
		String input3 = path.get("SimilarityStep5Input_3");
		String output = path.get("SimilarityStep5Output");

		Configuration configuration = new Configuration();
		JobConf job = new JobConf(configuration, ItemSimilarityStep5.class);

		HDFSDao hdfsDao = new HDFSDao(configuration);
		hdfsDao.rmr(output);

		FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2),
				new Path(input3));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Step5MapClass.class);
		job.setReducerClass(Step5ReduceClass.class);

		job.set("mapred.textoutputformat.separator", ":");

		RunningJob runningJob = JobClient.runJob(job);
		while (!runningJob.isComplete()) {
			runningJob.waitForCompletion();
		}

	}
}
