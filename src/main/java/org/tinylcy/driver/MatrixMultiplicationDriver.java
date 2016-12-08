package org.tinylcy.driver;

import org.tinylcy.multiplication.block.BlockMatrixMultiplicationStep1;
import org.tinylcy.multiplication.block.BlockMatrixMultiplicationStep2;
import org.tinylcy.multiplication.block.BlockMatrixMultiplicationStep3;

import java.io.IOException;

public class MatrixMultiplicationDriver {
	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {

		long beginTime = System.currentTimeMillis();

		// BasicMatrixMultiplicationStep2.run();
		// BasicMatrixMultiplicationStep3.run();

		// OneStepMatrixMultiplication.run();

		BlockMatrixMultiplicationStep1.run();
		BlockMatrixMultiplicationStep2.run();
		BlockMatrixMultiplicationStep3.run();

		long endTime = System.currentTimeMillis();
		double costTime = endTime - beginTime;
		System.out.println("耗时：" + costTime / 1000 + " 秒");
	}
}
