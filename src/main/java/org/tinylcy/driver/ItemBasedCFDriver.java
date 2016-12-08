package org.tinylcy.driver;

import org.tinylcy.hdfs.HDFS;
import org.tinylcy.multiplication.block.BlockMatrixMultiplicationStep1;
import org.tinylcy.multiplication.block.BlockMatrixMultiplicationStep2;
import org.tinylcy.multiplication.block.BlockMatrixMultiplicationStep3;
import org.tinylcy.multiplication.ik.IAndKMatrixMultiplicationStep1;
import org.tinylcy.multiplication.ik.IAndKMatrixMultiplicationStep2;
import org.tinylcy.multiplication.j.JMatrixMultiplicationStep1;
import org.tinylcy.multiplication.j.JMatrixMultiplicationStep2;
import org.tinylcy.multiplication.j.JMatrixMultiplicationStep3;
import org.tinylcy.similarity.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class ItemBasedCFDriver {

    public static Map<String, String> path = new HashMap<String, String>();
    public static String FileName = null;
    public static String HOST = null;
    public static String HDFSPATH = null;
    public static MultiplicationMode MODE = MultiplicationMode.BLOCK;

    public static int N; // Item的个数，即物品相似矩阵的行数和列数，以及评分矩阵的行数
    public static int M; // User的个数，即评分矩阵的列数
    public static int P; // 物品相似矩阵分块的行数
    public static int Q; // 物品相似矩阵分块的列数，评分矩阵分块的行数
    public static int R; // 评分矩阵分块的列数

    public static int ReducerNumber = 1;

    public static void main(String[] args) throws ClassNotFoundException,
            IOException, InterruptedException, URISyntaxException {

        parseArguments(args);
        initialInputAndOutputPath();
        run();
    }

    private static void parseArguments(String[] args) {
        Map<String, String> arguments = new HashMap<String, String>();
        for (int i = 0; i < args.length - 1; i = i + 2) {
            arguments.put(args[i], args[i + 1]);
        }

        if (arguments.get("-filename") != null) {
            FileName = "/".concat(arguments.get("-filename"));
        } else {
            argNullErrorMsg("-filename");
            return;
        }

        if (arguments.get("-reducer") != null) {
            ReducerNumber = Integer.parseInt(arguments.get("-reducer"));
        } else {
            argNullErrorMsg("-reducer");
            return;
        }

        if (arguments.get("-n") != null) {
            N = Integer.parseInt(arguments.get("-n"));
        } else {
            argNullErrorMsg("-n(the number of item)");
            return;
        }

        if (arguments.get("-m") != null) {
            M = Integer.parseInt(arguments.get("-m"));
        } else {
            argNullErrorMsg("-m(the number of user)");
            return;
        }

        if (arguments.get("-p") != null) {
            P = Integer.parseInt(arguments.get("-p"));
        } else {
            argNullErrorMsg("-p");
            return;
        }

        if (arguments.get("-q") != null) {
            Q = Integer.parseInt(arguments.get("-q"));
        } else {
            argNullErrorMsg("-q");
            return;
        }

        if (arguments.get("-r") != null) {
            R = Integer.parseInt(arguments.get("-r"));
        } else {
            argNullErrorMsg("-r");
            return;
        }

        if (arguments.get("-host") != null) {
            HOST = arguments.get("-host");
        } else {
            argNullErrorMsg("-host");
            return;
        }

        if (arguments.get("-path") != null) {
            HDFSPATH = arguments.get("-path");
        } else {
            argNullErrorMsg("-path");
            return;
        }

        if (arguments.get("-mode") != null) {
            String mode = arguments.get("-mode");
            if (mode.equals("BLOCK") || mode.equals("IK") || mode.equals("J")) {
                if (mode.equals("BLOCK")) {
                    MODE = MultiplicationMode.BLOCK;
                } else if (mode.equals("IK")) {
                    MODE = MultiplicationMode.IK;
                } else {
                    MODE = MultiplicationMode.J;
                }
            } else {
                argNullErrorMsg("-mode");
                return;
            }

        }
    }

    private static void initialInputAndOutputPath() {
        path.put("step1InputPath", HDFS.HDFSPATH + FileName);
        path.put("step1OutputPath", HDFS.HDFSPATH + "/step1");

        path.put("step2InputPath", HDFS.HDFSPATH + FileName);
        path.put("step2OutputPath", HDFS.HDFSPATH + "/step2");

        path.put("step3InputPath", HDFS.HDFSPATH + "/step2");
        path.put("step3OutputPath", HDFS.HDFSPATH + "/step3");

        path.put("step4InputPath", HDFS.HDFSPATH + FileName);
        path.put("step4OutputPath", HDFS.HDFSPATH + "/step4");

        path.put("step5InputPath", HDFS.HDFSPATH + "/step2");
        path.put("step5OutputPath", HDFS.HDFSPATH + "/step5");

        path.put("step6InputPath", HDFS.HDFSPATH + "/step3");
        path.put("step6OutputPath", HDFS.HDFSPATH + "/step6");

        path.put("step7InputPath1", HDFS.HDFSPATH + "/step6");
        path.put("step7InputPath2", HDFS.HDFSPATH + "/step1");
        path.put("step7OutputPath", HDFS.HDFSPATH + "/step7");

        path.put("step8InputPath", HDFS.HDFSPATH + "/step7");
        path.put("step8OutputPath", HDFS.HDFSPATH + "/step8");

        path.put("step9InputPath", HDFS.HDFSPATH + "/step8");
        path.put("step9OutputPath", HDFS.HDFSPATH + "/step9");
    }

    private static void run() throws ClassNotFoundException, IOException,
            InterruptedException, URISyntaxException {

        double begin = System.currentTimeMillis();

        CalculateSimilarityStep1.run();
        CalculateSimilarityStep2.run();
        CalculateSimilarityStep3.run();
        CalculateSimilarityStep4.run();
        CalculateSimilarityStep5.run();
        CalculateSimilarityStep6.run();

        switch (MODE) {
            case BLOCK:
                BlockMatrixMultiplicationStep1.run();
                BlockMatrixMultiplicationStep2.run();
                BlockMatrixMultiplicationStep3.run();
                break;
            case IK:
                IAndKMatrixMultiplicationStep1.run();
                IAndKMatrixMultiplicationStep2.run();
                break;
            case J:
                JMatrixMultiplicationStep1.run();
                JMatrixMultiplicationStep2.run();
                JMatrixMultiplicationStep3.run();
                break;
            default:
                break;
        }


        double end = System.currentTimeMillis();
        System.out.println("耗时: " + (end - begin) / 1000 + " s");
    }


    private static void argNullErrorMsg(String target) {
        System.err.println(target + " please specify the " + target + ".");
    }
}
