package mainpage_playlist;

/**
 * Created by qianwei on 2017/8/14.
 */
public class Rank {
    /**
     * 输出是lr的分数
     * @param weight: lr权重
     * @param feature: 特征向量
     * @return sigmoid(score)
     */
    public static double LRRankPointWithoutBias(double[] weight, double[] feature){
        if (weight.length != feature.length)
            return -1.0;
        double score = 0.0;
        for (int i = 0; i < weight.length; ++i) {
            score +=  weight[i] * feature[i];
        }
        return sigmoid(score);
    }

    public static double LRRankPoint(double[] weight, double[] feature, double bias){
        if (weight.length != feature.length)
            return -1.0;
        double score = 0.0;
        for (int i = 0; i < weight.length; ++i) {
            score +=  weight[i] * feature[i];
        }
        return sigmoid(score + bias);
    }


    public static double sigmoid(double x)
    {
        return 1 / (1 + Math.pow(Math.E, -1 * x));
    }


}
