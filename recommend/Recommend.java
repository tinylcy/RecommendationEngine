package recommend;

import itemBasedRec.ItemBasedRecStep1;
import itemBasedRec.ItemBasedRecStep2;
import itemBasedRec.ItemBasedRecStep3;
import itemBasedRec.ItemBasedRecStep4;
import itemBasedRec.ItemBasedRecStep5;
import itemBasedRec.ItemBasedRecStep6;
import itemBasedRec.ItemBasedRecStep7;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

public class Recommend {

	public static final String HDFS = "hdfs://localhost:9000";
	public static final Pattern DELIMITER = Pattern.compile("[\t,]");

	public static JobConf config() {

		Configuration configuration = new Configuration();
		JobConf job = new JobConf(configuration, Recommend.class);
		job.setJobName("Recommend");
		job.addResource("/usr/local/hadoop/conf/core-site.xml");
		job.addResource("/usr/local/hadoop/conf/hdfs-site.xml");
		job.addResource("/usr/local/hadoop/conf/mapred-site.xml");
		return job;
	}

	public static void main(String[] args) throws Exception {

		Map<String, String> path = new HashMap<String, String>();

		String stepPath = HDFS + "/user/hadoop/Recommend";

		path.put("Step1Input", HDFS + "/user/hadoop/Recommend/InputData");
		path.put("Step1Output", stepPath + "/step1");
		path.put("Step2Input", path.get("Step1Output"));
		path.put("Step2Output", stepPath + "/step2");
		path.put("Step3Input1", path.get("Step1Output"));
		path.put("Step3Output1", stepPath + "/step3_1");
		path.put("Step3Input2", path.get("Step2Output"));
		path.put("Step3Output2", stepPath + "/step3_2");
		path.put("Step4Input1", path.get("Step3Output1"));
		path.put("Step4Input2", path.get("Step3Output2"));
		path.put("Step4Output", stepPath + "/step4");
		path.put("Step5Input", path.get("Step4Output"));
		path.put("Step5Output", stepPath + "/step5");
		path.put("Step6Input", path.get("Step5Output"));
		path.put("Step6Output", stepPath + "/step6");
		path.put("Step7Input", path.get("Step6Output"));
		path.put("Step7Output", stepPath + "/step7");

		ItemBasedRecStep1.run(path);
		ItemBasedRecStep2.run(path);
		ItemBasedRecStep3.run1(path);
		ItemBasedRecStep3.run2(path);
		ItemBasedRecStep4.run(path);
		ItemBasedRecStep5.run(path);
		ItemBasedRecStep6.run(path);
		ItemBasedRecStep7.run(path);

		CalculateUserSimilarity.runUserSimilarityCalculation();
		CalculateItemSimilarity.runItemSimilarityCalculation();

		System.exit(0);
	}
}
