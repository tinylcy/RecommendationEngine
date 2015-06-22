package recommend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import userSimilarity.UserSimilarityStep1;
import userSimilarity.UserSimilarityStep2;
import userSimilarity.UserSimilarityStep3;
import userSimilarity.UserSimilarityStep4;
import userSimilarity.UserSimilarityStep5;

public class CalculateUserSimilarity {

	public static final String HDFS = "hdfs://localhost:9000";
	public static final String SIMILARITYPATH = HDFS
			+ "/user/hadoop/Recommend/UserSimilarity";

	public static void runUserSimilarityCalculation() throws IOException,
			ClassNotFoundException, InterruptedException {

		Map<String, String> path = new HashMap<String, String>();

		path.put("SimilarityStep1Input", HDFS
				+ "/user/hadoop/Recommend/InputData/");
		path.put("SimilarityStep1Output", SIMILARITYPATH
				+ "/UserSimilarityStep1");

		path.put("SimilarityStep2Input", path.get("SimilarityStep1Output"));
		path.put("SimilarityStep2Output", SIMILARITYPATH
				+ "/UserSimilarityStep2");

		path.put("SimilarityStep3Input", path.get("SimilarityStep1Output"));
		path.put("SimilarityStep3Output", SIMILARITYPATH
				+ "/UserSimilarityStep3");

		path.put("SimilarityStep4Input", path.get("SimilarityStep2Output"));
		path.put("SimilarityStep4Output", SIMILARITYPATH
				+ "/UserSimilarityStep4");

		path.put("SimilarityStep5Input_1", path.get("SimilarityStep1Output"));
		path.put("SimilarityStep5Input_2", path.get("SimilarityStep3Output"));
		path.put("SimilarityStep5Input_3", path.get("SimilarityStep4Output"));
		path.put("SimilarityStep5Output", SIMILARITYPATH
				+ "/UserSimilarityStep5");

		UserSimilarityStep1.run(path);
		UserSimilarityStep2.run(path);
		UserSimilarityStep3.run(path);
		UserSimilarityStep4.run(path);
		UserSimilarityStep5.run(path);
	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		runUserSimilarityCalculation();
	}

}
