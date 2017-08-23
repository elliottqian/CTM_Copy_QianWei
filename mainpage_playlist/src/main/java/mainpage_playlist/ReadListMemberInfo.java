package mainpage_playlist;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by qianwei on 2017/8/15.
 * 用于读取歌单付费信息的类
 * 错误的样例: 430818113	7:Infinity,16:Infinity	null:Infinity,yueyuMusic:Infinity
 */
public class ReadListMemberInfo implements Serializable {
    /**
     * 在HDFS中读取 首页歌单中, 点击率对会员人数, 会员钱数的影响信息
     */
    public static HashMap<String, String[]> readMemberPayList(Reducer.Context context) {

        HashMap<String, String[]> playlistInfo = new HashMap<>();

        try{
            // 如果没有就读取默认路径
            String listMemberInfoPath = context.getConfiguration().get("playlist_member_info_path");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fin = fs.open(new Path(listMemberInfoPath));
            BufferedReader in = null;

            String line;

            try {
                in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
                while ((line = in.readLine()) != null) {
                    String[] lines = line
                            .replace("(", "")
                            .replace(")", "")
                            .split(",");
                    String[] infoArray = {lines[1], lines[2]};
                    playlistInfo.put(lines[0], infoArray);
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }catch (Exception e) {
            System.out.print(e.getMessage());
        }
        return playlistInfo;
    }

    /**
     * 首页歌单中, 点击率对会员人数, 会员钱数的影响信息  产生的权重
     * @return
     */
    protected static double createMemberPlaylistWeight(String userId,
                                                       String playlistId,
                                                       HashMap<String, String[]> memberPlaylistInfo,
                                                       int moneyWeight,
                                                       int clickWeight){

        double moneyContribute;
        double clickContribute;

//        double moneyWeight = 70.0;
//        double clickWeight = 800.0;

        double payPlaylistWeight = 0.0;

        try {
            // 金钱和点击率贡献
            String[] moneyAndClick = memberPlaylistInfo.get(playlistId);
            moneyContribute = Double.parseDouble(moneyAndClick[0]);
            clickContribute = Double.parseDouble(moneyAndClick[1]);

            //  会员转换率 * 权重 + 金钱转换率 * 权重
            payPlaylistWeight = moneyContribute * moneyWeight + clickContribute * clickWeight;

        }catch (Exception e){
            e.printStackTrace();
        }
        return payPlaylistWeight;
    }
}
