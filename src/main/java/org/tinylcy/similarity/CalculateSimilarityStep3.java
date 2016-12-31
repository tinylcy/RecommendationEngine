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


/*
 * 
 * 构造物品的同现矩阵，即计算 物品i 和 物品j 被哪些用户同时喜欢；
 * 输入数据为Step2的输出数据(用户-物品的倒排表)；
 * 输出数据格式为：ItemID_i:ItemID_j UserID1,UserID2 ...
 * 在计算物品之间的相似度时，只有当两个物品至少被一个用户同时喜欢过，
 * 这两个物品才有资格计算相似度。
 */

public class CalculateSimilarityStep3 {

    public static class Step3_Mapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {

        private Text k = new Text();
        private IntWritable v = new IntWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] tokens = value.toString().split("[,:]");
            int userID = Integer.parseInt(tokens[0]);
            v.set(userID);
            for (int i = 1; i < tokens.length; i++) {
                for (int j = 1; j < tokens.length; j++) {
                    String itemID_a = tokens[i];
                    String itemID_b = tokens[j];
                    k.set(itemID_a + ":" + itemID_b);
                    context.write(k, v);
                }
            }
        }
    }

    public static class Step3_Reducer extends
            Reducer<Text, IntWritable, Text, Text> {

        private Text v = new Text();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> iterator = values.iterator();
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
        String inputPath = ItemBasedCFDriver.path.get("step3InputPath");
        String outputPath = ItemBasedCFDriver.path.get("step3OutputPath");

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", " ");

        Job job = Job.getInstance(conf);

        HDFS hdfs = new HDFS(conf);
        hdfs.rmr(outputPath);

        job.setMapperClass(Step3_Mapper.class);
        job.setReducerClass(Step3_Reducer.class);
        job.setNumReduceTasks(ItemBasedCFDriver.ReducerNumber);

        job.setJarByClass(CalculateSimilarityStep3.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }


}
