package org.tinylcy.multiplication.block;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.regex.Pattern;

public class BlockMatrixMultiplicationStep2 {

    private static final Pattern DELIMITER = Pattern.compile("[,]");

    public static class BlockMatrixStep2_Mapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int n = Integer.parseInt(conf.get("n"));
            int m = Integer.parseInt(conf.get("m"));
            int p = Integer.parseInt(conf.get("p"));
            int q = Integer.parseInt(conf.get("q"));
            int r = Integer.parseInt(conf.get("r"));

            int mPerR = m % r == 0 ? m / r : m / r + 1;
            int nPerP = n % p == 0 ? n / p : n / p + 1;

            String line = value.toString();
            String[] tokens = DELIMITER.split(line);

            String keyStr = null;
            String valueStr = null;

            if (tokens[0].equals("A")) {
                int i = Integer.parseInt(tokens[1]);
                int j = Integer.parseInt(tokens[2]);
                double a_ij = Double.parseDouble(tokens[3]);
                for (int k = 0; k < mPerR; k++) {
                    keyStr = i / p + "," + j / q + "," + k;
                    valueStr = "A," + i % p + "," + j % q + "," + a_ij;
                    outputKey.set(keyStr);
                    outputValue.set(valueStr);
                    context.write(outputKey, outputValue);
                }
            } else if (tokens[0].equals("B")) {
                int j = Integer.parseInt(tokens[1]);
                int k = Integer.parseInt(tokens[2]);
                double b_jk = Double.parseDouble(tokens[3]);
                for (int i = 0; i < nPerP; i++) {
                    keyStr = i + "," + j / q + "," + k / r;
                    valueStr = "B," + j % q + "," + k % r + "," + b_jk;
                    outputKey.set(keyStr);
                    outputValue.set(valueStr);
                    context.write(outputKey, outputValue);
                }
            }

        }
    }

    public static class BlockMatrixStep2_Reducer extends
            Reducer<Text, Text, NullWritable, Text> {

        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<Entry<String, Double>> listA = new ArrayList<Entry<String, Double>>();
            ArrayList<Entry<String, Double>> listB = new ArrayList<Entry<String, Double>>();
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                String line = iterator.next().toString();
                String[] tokens = DELIMITER.split(line);
                if (tokens[0].equals("A")) {
                    listA.add(new SimpleEntry<String, Double>(tokens[1] + ","
                            + tokens[2], Double.parseDouble(tokens[3])));
                } else if (tokens[0].equals("B")) {
                    listB.add(new SimpleEntry<String, Double>(tokens[1] + ","
                            + tokens[2], Double.parseDouble(tokens[3])));
                }
            }

            HashMap<String, Double> map = new HashMap<String, Double>();

            String[] iAndJ = null;
            String[] jAndK = null;
            String iAndK = null;
            double a_ij = 0.0;
            double b_jk = 0.0;

            for (Entry<String, Double> a : listA) {
                iAndJ = a.getKey().split(",");
                a_ij = a.getValue();
                for (Entry<String, Double> b : listB) {
                    jAndK = b.getKey().split(",");
                    b_jk = b.getValue();
                    if (iAndJ[1].equals(jAndK[0])) {
                        iAndK = iAndJ[0] + "," + jAndK[1];
                        if (map.containsKey(iAndK)) {
                            map.put(iAndK, map.get(iAndK) + a_ij * b_jk);
                        } else {
                            map.put(iAndK, a_ij * b_jk);
                        }
                    }
                }
            }

            Configuration conf = context.getConfiguration();
            int p = Integer.parseInt(conf.get("p"));
            int r = Integer.parseInt(conf.get("r"));

            String[] keyIndices = DELIMITER.split(key.toString());
            String[] localIndices = null;

            int key_i = 0;
            int key_k = 0;
            int local_i = 0;
            int local_k = 0;
            int global_i = 0;
            int global_k = 0;

            for (Entry<String, Double> entry : map.entrySet()) {
                key_i = Integer.parseInt(keyIndices[0]);
                key_k = Integer.parseInt(keyIndices[2]);
                localIndices = entry.getKey().split(",");
                local_i = Integer.parseInt(localIndices[0]);
                local_k = Integer.parseInt(localIndices[1]);
                global_i = key_i * p + local_i;
                global_k = key_k * r + local_k;

                outputValue.set(global_i + "," + global_k + ","
                        + entry.getValue());
                context.write(null, outputValue);
            }
        }

    }

    public static void run() throws IOException, ClassNotFoundException,
            InterruptedException {

        String inputPath = ItemBasedCFDriver.path.get("step8InputPath");
        // String inputPath = HDFS.HDFSPATH + "/matrix.txt";
        String outputPath = ItemBasedCFDriver.path.get("step8OutputPath");

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", "");

        conf.set("n", String.valueOf(ItemBasedCFDriver.M));
        conf.set("m", String.valueOf(ItemBasedCFDriver.N));
        conf.set("p", String.valueOf(ItemBasedCFDriver.P));
        conf.set("q", String.valueOf(ItemBasedCFDriver.Q));
        conf.set("r", String.valueOf(ItemBasedCFDriver.R));

        // conf.set("n", "4");
        // conf.set("m", "3");
        // conf.set("p", "3");
        // conf.set("q", "2");
        // conf.set("r", "2");

        HDFS hdfs = new HDFS(conf);
        hdfs.rmr(outputPath);

        Job job = Job.getInstance(conf);

        job.setMapperClass(BlockMatrixStep2_Mapper.class);
        job.setReducerClass(BlockMatrixStep2_Reducer.class);
        job.setNumReduceTasks(ItemBasedCFDriver.ReducerNumber);

        job.setJarByClass(BlockMatrixMultiplicationStep2.class);

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
