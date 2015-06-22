package recommend;

import itemSimilarity.ItemSimilarityStep1;
import itemSimilarity.ItemSimilarityStep2;
import itemSimilarity.ItemSimilarityStep3;
import itemSimilarity.ItemSimilarityStep4;
import itemSimilarity.ItemSimilarityStep5;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CalculateItemSimilarity {

	public static final String HDFS = "hdfs://localhost:9000";
	public static final String SIMILARITYPATH = HDFS
			+ "/user/hadoop/Recommend/ItemSimilarity";

	public static void runItemSimilarityCalculation() throws IOException {

		Map<String, String> path = new HashMap<String, String>();

		path.put("SimilarityStep1Input", HDFS
				+ "/user/hadoop/Recommend/InputData/");
		path.put("SimilarityStep1Output", SIMILARITYPATH
				+ "/ItemSimilarityStep1");

		path.put("SimilarityStep2Input", path.get("SimilarityStep1Output"));
		path.put("SimilarityStep2Output", SIMILARITYPATH
				+ "/ItemSimilarityStep2");

		path.put("SimilarityStep3Input", path.get("SimilarityStep2Output"));
		path.put("SimilarityStep3Output", SIMILARITYPATH
				+ "/ItemSimilarityStep3");

		path.put("SimilarityStep4Input", path.get("SimilarityStep3Output"));
		path.put("SimilarityStep4Output", SIMILARITYPATH
				+ "/ItemSimilarityStep4");

		path.put("SimilarityStep5Input_1", path.get("SimilarityStep2Output"));
		path.put("SimilarityStep5Input_2", path.get("SimilarityStep3Output"));
		path.put("SimilarityStep5Input_3", path.get("SimilarityStep4Output"));
		path.put("SimilarityStep5Output", SIMILARITYPATH
				+ "/ItemSimilarityStep5");

		ItemSimilarityStep1.run(path);
		ItemSimilarityStep2.run(path);
		ItemSimilarityStep3.run(path);
		ItemSimilarityStep4.run(path);
		ItemSimilarityStep5.run(path);

	}

	public static void main(String[] args) throws IOException {
		runItemSimilarityCalculation();
	}

}
