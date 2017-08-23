package mainpage_playlist;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.*;

/**
 * 执行脚本位于: /data/music_useraction/mainpage_playlist/rec_tagbased_withSort.sh
 */
public class UserPlaylistRecByTag_withSort extends Configured implements Tool
{


	/*
	 * input1:userId \t playlist:score,playlist:score,playlist:score.. \t score
	 * \t playlistID(reason) input2:userid,playlist,score (user visited
	 * playlist) output1:userId \t
	 * playlist:score,playlist:score,playlist:score.. \t score \t
	 * playlistID(reason) output2:userid \t playlist \t score (user visited
	 * playlist)
	 */
	public static class RecommendMapper extends Mapper<WritableComparable<?>, Text, LongPair, Text>
	{
		private HashMap<String, Integer> userPortrayIndex = new HashMap<String, Integer>();
		private String userPortrayPath;
		private String userActiveTime;
		String userTag = "";
		String userFeature;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			userFeature = context.getConfiguration().get("userFeature");
			userTag = context.getConfiguration().get("userTag");
			userActiveTime = context.getConfiguration().get("userActiveTime");
			userPortrayPath = context.getConfiguration().get("userPortrayPath");
			String portrayMetadata = context.getConfiguration().get("portrayMetadata");
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FileStatus[] st = fs.listStatus(new Path(portrayMetadata));
			String line;
			for (FileStatus f : st)
			{
				if (!f.getPath().getName().contains("_logs"))
				{
					int index = 0;
					FSDataInputStream hdfsInStream = fs.open(f.getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
					while ((line = br.readLine()) != null)
					{
						String[] array = line.split(",");
						if (line.isEmpty() || array.length < 4)
							continue;
						userPortrayIndex.put(array[0], index++);
					}
				}
			}
		}

		@Override
		protected void map(WritableComparable<?> key, Text value, Context context) throws IOException, InterruptedException
		{
			try
			{

				Path path = ((FileSplit) context.getInputSplit()).getPath();
				String fileName = path.toString();
				String str = value.toString();
				LongPair outKey = new LongPair();
				if(fileName.contains(userActiveTime))
				{
					String[]array = str.split("\t",-1);
					outKey.set(Long.valueOf(array[0]), -10);
					context.write(outKey,new Text( array[1]));
				}
				else if (fileName.contains(userFeature)){
					int pos = str.indexOf("\t");
					String userID = str.substring(0, pos);
					String feature = str.substring(pos + 1);
					outKey.set(Long.valueOf(userID), -8);
					
					context.write(outKey, new Text(feature));
				}
				else if (fileName.contains(userPortrayPath))
				{
					String[]array = str.split("\t",-1);
					Long userID = Long.valueOf(array[0]);
					if(userID < 0)
						userID *= -1;
					outKey.set(userID, -5);
					
					Integer newSongTypeIndex = userPortrayIndex.get("newSongType");
					Integer newSongWeightIndex =  userPortrayIndex.get("newSongWeight");
					if(newSongTypeIndex <= array.length-1)
					{
						String newSongType = array[newSongTypeIndex];
						Float newSongWeight = Float.valueOf(array[newSongWeightIndex]);
						context.write(outKey, new Text( String.valueOf(newSongWeight)));
					}
					
				} 
				else if (fileName.contains(userTag))
				{
					String[]array = str.split("\t",-1);
					
					Long userID = Long.valueOf(array[0]);
					if(userID < 0)
						userID *= -1;
					outKey.set(userID, 1);
					context.write(outKey, new Text(array[1]));
					
				}
				else if (fileName.contains("mainpage_impress") )
				{
					if( !str.contains("user-playlist"))
						return;
					
					String[]array = str.split("\t",-1);
					
					Long userID = Long.valueOf(array[2]);
					if(userID < 0)
						userID *= -1;
					outKey.set(userID, -2);
					context.write(outKey, new Text(array[3]));
					
				}
				else if(fileName.contains("user_action"))
				{//visit
					String[] array = str.split(",");
					Long userID = Long.valueOf(array[0]);
					if(userID < 0)
						userID *= -1;
					outKey.set(userID, -1);
					//outKey.set(Long.valueOf(array[0]), -1);
					context.write(outKey, new Text(array[1] + "\t" + array[2]));
					
				}
				else if(fileName.contains("rec_history"))
				{//rec_history
					String[]array = str.split("\t",-1);
					
					Long userID = Long.valueOf(array[0]);
					if(userID < 0)
						userID *= -1;
					outKey.set(userID, -3);
					int count = 0;
					for(String term : array[1].split(","))
					{
						context.write(outKey, new Text(term));
						if( ++ count >= 2 )
							break;
					
					}
				}else if(fileName.contains("user_tag_new")) {
                    String[] lines = str.split("\t");
                    String userId = lines[0];
                    if (lines[1].startsWith("lan")) {
                        String languagePromote = lines[2];
                        outKey.set(Long.valueOf(userId), -101);
                        context.write(outKey, new Text(languagePromote));
                    }else if (lines.length == 2) {
                        String stylePromote = lines[1];
                        outKey.set(Long.valueOf(userId), -100);
                        context.write(outKey, new Text(stylePromote));
                    }
                }
			
			}catch(Exception e)
			{
				
			}
		}
	}
	
	public static List<GenericPair<String, Float>> getList(String result)
	{
		List<GenericPair<String, Float>> list = new LinkedList<GenericPair<String, Float>>();
		try
		{
			if (result != null && !result.isEmpty())
			{
				for (String str : result.split(","))
				{
					int pos = str.lastIndexOf(":");
					if (pos < 0 || pos == str.length() - 1)
						continue;
					String id = str.substring(0, pos);
					Float similarity = Float.parseFloat(str.substring(pos + 1));
					list.add(new GenericPair(id, similarity));
					if (list.size() >= 2000)
						break;
				}
			}
			return list;
		} catch (Exception e)
		{
			return new LinkedList<GenericPair<String, Float>>();
		}

	}
	
	static class ListInfo
	{
		public int pubTime;
		public int createDays;
		public float score;
		public int recClick;
		public float rec_subRate;
		public  ListInfo()
		{
			pubTime = 100;
			score = 0;
			createDays = 1000;
			//recClick = 0;
			//rec_subRate = 0.07f;
		}
	}
	
	static long getNowDayMills()
	{
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTimeInMillis();
	}
	
	/*
	 * input1:key is userId value playlist:score,playlist:score,playlist:score..
	 * \t score \t playlistID(reason) input2:key is userid value playlist \t
	 * score (user visited playlist)
	 */
	public static class RecommendReducer extends Reducer<LongPair, Text, Text, Text>
	{
		int maxRecommendation;
		HashSet<String> badPlaylistSet = new HashSet<String>();
		int maxImpress = 0;
		HashMap<Long, ListInfo> listInfoMap = new HashMap<Long, ListInfo>();
		HashMap<String, LinkedList<GenericPair<String, Float>>> tag2playlistMap = new HashMap<String, LinkedList<GenericPair<String, Float>>>();
		float recSubRatio_default = 0.07f;
		//int createDays = 60;
		private long lastestSec = 0;
		
		
		HashMap<String, BitSet> playlistFeatureMap = new HashMap<String, BitSet>();
		private int originalPlaylistfeatureSize = 0;
		double[] weights = null;
		double[] weights_ab = null;
		HashMap<String, HashMap<String,Float>> playlistSubRatio_Promoted_Map = new HashMap<String, HashMap<String,Float>>();

        HashMap<String, String> listLanguage = new HashMap<>();
        HashMap<String, String> listStyle = new HashMap<>();

//        /**
//         * 得到用户喜爱的语言和语种风格
//         * @param context
//         * @throws IOException
//         * @throws InterruptedException
//         */
//        public void getUserLanAndStyle(Context context, FileSystem fs) throws IOException, InterruptedException{
//            String userTagPath = context.getConfiguration().get("user_tag_new");
//            if (userTagPath == null)
//                return;
//            FileStatus[] st = fs.listStatus(new Path(userTagPath));
//
//            String line;
//            for (FileStatus f : st) {
//                FSDataInputStream hdfsInStream = fs.open(f.getPath());
//                BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
//                while ((line = br.readLine()) != null)
//                {
//                    String[] lines = line.split("\t");
//                    String userId = lines[0];
//                    if (lines[1].startsWith("lan")) {
//                        String languagePromote = lines[2];
//                        HashMap<String, String> tempHm = string2Map(languagePromote);
//                        userLanguage.put(userId, tempHm);
//                    }else if (lines.length == 2) {
//                        String stylePromote = lines[1];
//                        HashMap<String, String> tempHm = string2Map(stylePromote);
//                        userStyle.put(userId, tempHm);
//                    }
//                }//while
//            }
//        }

        /**
         * 获得歌单的语种和风格
         */
        private void getListLanAndStyle(Context context,
                                        FileSystem fs,
                                        HashMap<String, LinkedList<GenericPair<String, Float>>> tag2playlistMap) throws IOException, InterruptedException{

            HashSet<String> listIdSet = new HashSet<>();
            for (String key: tag2playlistMap.keySet()) {
                LinkedList<GenericPair<String, Float>> ll =  tag2playlistMap.get(key);
                for (GenericPair<String, Float> g: ll) {
                    String listId = g.first;
                    if (!listIdSet.contains(listId)) {
                        listIdSet.add(listId);
                    }
                }
            }

            String playlistAreaTagPath = context.getConfiguration().get("playlistAreaTag");
            if (playlistAreaTagPath == null)
                return;
            FileStatus[] st = fs.listStatus(new Path(playlistAreaTagPath));

            String line;
            for (FileStatus f : st) {
                FSDataInputStream hdfsInStream = fs.open(f.getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
                while ((line = br.readLine()) != null) {
                    String[] lines = line.split("\t");
                    String listId = lines[0];

                    if (listIdSet.contains(listId)) {
                        String lan = lines[1];
                        String style = lines[2];
                        listLanguage.put(listId, lan);
                        listStyle.put(listId, style);
                    }
                }
            }
        }

        /**
         * @param userId 用户id
         * @param listId 歌单id
         */
        private double getLanOrStyleScore(String userId,
                                  String listId,
                                  HashMap<String, HashMap<String, String>> userMap,
                                  HashMap<String, String> listMap){

            HashMap<String, String> userScoreMap = userMap.get(userId);
            if (userScoreMap == null) {
                System.out.println("user is empty:" + userId);
            }
            HashMap<String, String> listScoreMap = string2Map(listMap.get(listId));
            double score = 0.0;

            try{
                for (String lanOrStyleKey: userScoreMap.keySet()) {

                    double userScore = Double.valueOf(userScoreMap.get(lanOrStyleKey));

                    if (userScore > 0.03 && listScoreMap.keySet().contains(lanOrStyleKey)) {
                        double listScore = Double.valueOf(listScoreMap.get(lanOrStyleKey));
                    /* 用户对这个语种或风格的喜爱 乘以 歌单在这个风格和语种的比例 */
                        score += userScore * listScore;
                    }
                }
            }catch (Exception e) {
                System.out.println(userId);
                System.out.println(listId);
                System.out.println("can not find userId or listId");
            }


            return score;
        }


        private HashMap<String, String> string2Map(String string) {
            HashMap<String, String> hm = new HashMap<>();
            String[] strings = string.split(",");
            for (String element: strings) {
                String key = element.split(":")[0];
                String value = element.split(":")[1];
                hm.put(key, value);
            }
            return hm;
        }

//        /**
//         *
//         * 输入的向量
//         * @return
//         */
//        public double[] getFeatureVec(String userId,
//                                      String listId,
//                                      int[] userFeature,
//                                      HashMap<String, BitSet> playlistFeatureMap,
//                                      int listFeatureSize,
//                                      HashMap<String,HashMap<String,Float>> playlistSubRatioPromotedMap,
//                                      HashMap<String, String> recommendSrcMap){
//            // 后面的1代表playlistSubRatioPromoted, 2代表语种和风格
//            int featureSize = userFeature.length + listFeatureSize + 1 + 2;
//
//            /* 处理用户特征 */
//            double[] featureVec = new double[featureSize];
//            for (int i = 0; i < userFeature.length; ++i) {
//                featureVec[i] = userFeature[i];
//            }
//
//            /* 处理歌单特征 */
//            BitSet listFeature = playlistFeatureMap.get(listId);
//            if (listFeature != null)
//                for (int i = userFeature.length; i < userFeature.length + listFeatureSize; ++i) {
//                    int listFeatureIndex = i - userFeature.length;
//                    featureVec[i] = listFeature.get(listFeatureIndex) ? 1 : 0;
//                }
//
//            /* 处理ratio_Promoted特征 */
//            float ratio_Promoted = 0.05f;
//            HashMap<String, Float> ratio_Promoted_Map = playlistSubRatioPromotedMap.get(listId);
//            if(ratio_Promoted_Map != null) {
//                String src = recommendSrcMap.get(listId);
//                Float value = ratio_Promoted_Map.get(src);
//                if(value != null)
//                    ratio_Promoted = value;
//            }
//            featureVec[userFeature.length + listFeatureSize] = ratio_Promoted;
//
//            /* 处理语言和风格特征 */
//            int languageIndex = userFeature.length + listFeatureSize + 1;
//            int styleIndex = languageIndex + 1;
//            featureVec[languageIndex] = getLanOrStyleScore(userId, listId, userLanguage, listLanguage);
//            featureVec[styleIndex] = getLanOrStyleScore(userId, listId, userStyle, listStyle);
//
//            // 打印来检查 这个要和属性转换里面的一致
//            System.out.print(userId + "\t");
//            System.out.print(listId + "\t");
//            for (double d: featureVec)
//                System.out.print(String.valueOf(d) + ",");
//            System.out.println("\n");
//
//            return featureVec;
//        }


        /**
         *
         * 输入的向量
         * @return
         */
        public double[] getFeatureVec(String userId,
                                      String listId,
                                      int[] userFeature,
                                      HashMap<String, BitSet> playlistFeatureMap,
                                      int listFeatureSize,
                                      HashMap<String,HashMap<String,Float>> playlistSubRatioPromotedMap,
                                      HashMap<String, String> recommendSrcMap,
                                      HashMap<String,HashMap<String,String>> userLanguage,
                                      HashMap<String,HashMap<String,String>> userStyle,
                                      HashMap<String,String> listLanguage,
                                      HashMap<String,String> listStyle) {

            // 后面的1代表playlistSubRatioPromoted, 2代表语种和风格
            int userFeatureLength = 56;
            int listFeatureLength = 221;
            int featureSize = userFeatureLength + listFeatureLength + 1 + 2;
            double[] featureVec = new double[featureSize];

            /* 处理用户特征 */
            if (userFeature != null) {
                for (int i = 0; i < userFeature.length; ++i) {
                    featureVec[i] = userFeature[i];
                }
            }


            /* 处理歌单特征 */
            BitSet listFeature = playlistFeatureMap.get(listId);
            if (listFeature != null)
                for (int i = userFeatureLength; i < userFeatureLength + listFeatureSize; ++i) {
                    int listFeatureIndex = i - userFeatureLength;
                    featureVec[i] = listFeature.get(listFeatureIndex) ? 1 : 0;
                }

            /* 处理ratio_Promoted特征 */
            float ratio_Promoted = 0.05f;
            HashMap<String, Float> ratio_Promoted_Map = playlistSubRatioPromotedMap.get(listId);
            if(ratio_Promoted_Map != null) {
                String src = recommendSrcMap.get(listId);
                Float value = ratio_Promoted_Map.get(src);
                if(value != null)
                    ratio_Promoted = value;
            }
            featureVec[userFeatureLength + listFeatureSize] = ratio_Promoted;

            /* 处理语言和风格特征 */
            int languageIndex = userFeatureLength + listFeatureSize + 1;
            int styleIndex = languageIndex + 1;
            featureVec[languageIndex] = getLanOrStyleScore(userId, listId, userLanguage, listLanguage);
            featureVec[styleIndex] = getLanOrStyleScore(userId, listId, userStyle, listStyle);

            // 打印来检查 这个要和属性转换里面的一致
            System.out.print(userId + "\t");
            System.out.print(listId + "\t");
            for (double d: featureVec)
                System.out.print(String.valueOf(d) + ",");
            System.out.println("\n");

            return featureVec;
        }

        @Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			recSubRatio_default = context.getConfiguration().getFloat("recSubRatio_default", 0.07f);
			//createDays = context.getConfiguration().getInt("createDays", 60);
			maxRecommendation = context.getConfiguration().getInt("maxRecommendation", 48);


			maxImpress = 0 - context.getConfiguration().getInt("maxImpress", 2);
			int day = context.getConfiguration().getInt("day", 60);
			if(day > 0)
			{
				lastestSec = getNowDayMills()/1000 - day*3600*24;
			}

            /**
             * badPlaylist -------------------------------------------------------
             */
			String badPlaylist = context.getConfiguration().get("badPlaylist");
			if (badPlaylist != null)
			{
				FileSystem fs = FileSystem.get(context.getConfiguration());
				FileStatus[] st = fs.listStatus(new Path(badPlaylist));
				String line;
				for (FileStatus f : st)
				{
					if (!f.getPath().getName().contains("_logs"))
					{
						FSDataInputStream hdfsInStream = fs.open(f.getPath());
						BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
						while ((line = br.readLine()) != null)
						{
							badPlaylistSet.add(line);
						}
					}
				}
			}

            /**
             * badPlaylist2 -------------------------------------------------------
             */
			String badPlaylist2 = context.getConfiguration().get("badPlaylist2");
			if (badPlaylist2 != null)
			{
				FileSystem fs = FileSystem.get(context.getConfiguration());
				FileStatus[] st = fs.listStatus(new Path(badPlaylist2));
				String line = null;
				int count = 0;
				for (FileStatus f : st)
				{
					if (!f.getPath().getName().contains("_logs"))
					{
						FSDataInputStream hdfsInStream = fs.open(f.getPath());
						BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
						while ((line = br.readLine()) != null)
						{
							if(count++ < 50)
							{	
								String[]array=line.split("\t");
								badPlaylistSet.add(array[0]);
							}
							
						}
					}
				}
			}
				
			
			String tag2playlist = context.getConfiguration().get("tag2playlist");
			String tag2playlist2 = context.getConfiguration().get("tag2playlist2");
			String listProfile = context.getConfiguration().get("listProfile");
			String listQuality = context.getConfiguration().get("listQuality");

			//HashSet<Long> listSet = new HashSet<Long>();
			String line;
			FileSystem fs = FileSystem.get(context.getConfiguration());
			JSONParser parser = new JSONParser();
			FileStatus[] st = fs.listStatus(new Path(listQuality));
			for (FileStatus f : st)
			{
				if (!f.getPath().getName().contains("_logs"))
				{
					FSDataInputStream hdfsInStream = fs.open(f.getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
					while ((line = br.readLine()) != null)
					{
						String[] array = line.split("\t", -1);
						long listId = Long.valueOf(array[0]);
						ListInfo listInfo = listInfoMap.get(listId);
						if(listInfo == null)
						{
							listInfo = new ListInfo();
						}
						listInfo.score = Float.parseFloat(array[1]);
						listInfo.createDays = Integer.parseInt(array[3]);
						listInfoMap.put(listId, listInfo);
					}
				}
			}
			
			
			st = fs.listStatus(new Path(listProfile));
			int current = (int) (System.currentTimeMillis() / 1000);
			for (FileStatus f : st)
			{
				if (!f.getPath().getName().contains("_logs"))
				{
					FSDataInputStream hdfsInStream = fs.open(f.getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
					while ((line = br.readLine()) != null)
					{
						String[] array = line.split("\t", -1);
						long listId = Long.valueOf(array[0]);
						if (array.length == 2 && listInfoMap.keySet().contains(listId))
						{
							try
							{
								JSONObject obj = (JSONObject) parser.parse(array[1]);
								int pubTime = ((Long) obj.get("avgPubTime")).intValue();
								ListInfo listInfo = listInfoMap.get(listId);
								listInfo.pubTime = (current - pubTime) / (24 * 60 * 60);
								//listAvgPubTime.put(listId, (int)(pubTime/1000000)); //只取前四位
								listInfoMap.put(listId,listInfo);
							}
							catch(Exception e)
							{
								e.printStackTrace();
							}
						}
					}
				}
			}

            /**
             * tag2playlist-------------------------------------------
             */
			st = fs.listStatus(new Path(tag2playlist));
			String[] info = null;
			int pos = -1;
			for (FileStatus f : st)
			{
				if (!f.getPath().getName().contains("_logs"))
				{
					FSDataInputStream hdfsInStream = fs.open(f.getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
					while ((line = br.readLine()) != null)
					{
						info = line.split("\t");
						LinkedList<GenericPair<String, Float>> list = new LinkedList<GenericPair<String, Float>>();
						for(String idscore : info[1].split(","))
						{
							pos = idscore.indexOf(":");
							list.add(new GenericPair<String, Float>(idscore.substring(0, pos),
									Float.parseFloat(idscore.substring(pos + 1))));
						}
						tag2playlistMap.put(info[0], list);
					}
				}
			}


            /**
             * playlistFeature
             */
			st = fs.listStatus(new Path(context.getConfiguration().get("playlistFeature")));
			String pid = "";
			for (FileStatus f : st) {
				if (!f.getPath().getName().contains("_logs")) {
					FSDataInputStream hdfsInStream = fs.open(f.getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
					while ((line = br.readLine()) != null) {
						String[] array = line.split("\t");
						pid = array[0];
						
						String[] featurelist = array[1].split(",");
						BitSet bitset = new BitSet(featurelist.length);
						
						for(int i = 0; i < featurelist.length; i ++)
						{
							if(featurelist[i].equals("1"))
								bitset.set(i, true);
							else
								bitset.set(i, false);
							
						}
						
						playlistFeatureMap.put(pid, bitset);
						
						originalPlaylistfeatureSize = featurelist.length;
					}
				}
			}

            /**
             * LRweights--------------------------------------------------------------------------------
             */
			st = fs.listStatus(new Path(context.getConfiguration().get("LRweights")));
			for (FileStatus f : st) {
				if (!f.getPath().getName().contains("_logs")) {
					FSDataInputStream hdfsInStream = fs.open(f.getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
					while ((line = br.readLine()) != null) {
						String weightsStr = line.trim();
						weightsStr = weightsStr.substring(1);
						weightsStr = weightsStr.substring(0, weightsStr.length() - 1);
						String[] array = weightsStr.split(",");
						
						weights = new double[array.length];
						
						for(int i = 0; i < array.length; i ++)
						{
							weights[i] = Double.parseDouble(array[i]);
						}
					}
				}
			}

            /**
             * LRweights_ab-------------------------------------------------------------------------------
             */
			st = fs.listStatus(new Path(context.getConfiguration().get("LRweights_ab")));
			for (FileStatus f : st) {
				if (!f.getPath().getName().contains("_logs")) {
					FSDataInputStream hdfsInStream = fs.open(f.getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
					while ((line = br.readLine()) != null) {
						String weightsStr = line.trim();
						weightsStr = weightsStr.substring(1);
						weightsStr = weightsStr.substring(0, weightsStr.length() - 1);
						String[] array = weightsStr.split(",");
						
						weights_ab = new double[array.length];
						
						for(int i = 0; i < array.length; i ++)
						{
							weights_ab[i] = Double.parseDouble(array[i]);
						}
						
					
					}
				}
			}

            /**
             * playlistSubRatio_Promoted------------------------------------------------------------------
             */
			st = fs.listStatus(new Path(context.getConfiguration().get("playlistSubRatio_Promoted")));
			
			for (FileStatus f : st) {
				if (!f.getPath().getName().contains("_logs")) {
					FSDataInputStream hdfsInStream = fs.open(f.getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
					while ((line = br.readLine()) != null) {
						String[] array = line.trim().split("\t", -1);
						HashMap<String, Float> map = new HashMap<String, Float>();
						for(String str : array[5].split(","))
						{
							String[] infos = str.split(":");
							if(infos.length < 3)
								continue;
							map.put(infos[0], Float.parseFloat(infos[2]));
						}
						
						playlistSubRatio_Promoted_Map.put(array[0], map);
					}
				}
			}

            /**
             * 得到歌单 语种和风格的 map ---------------------------------------------------------------
             */
            getListLanAndStyle(context, fs, tag2playlistMap);

		}


		public void getListLanAndStyleWithListIds(Context context,
                                                  FileSystem fs,
                                                  HashSet<String> listIds,
                                                  HashMap<String, HashMap<String, String>> listLanguage,
                                                  HashMap<String, HashMap<String, String>> listStyle)  throws IOException, InterruptedException{
            String playlistAreaTagPath = context.getConfiguration().get("playlistAreaTag");
            if (playlistAreaTagPath == null)
                return;
            FileStatus[] st = fs.listStatus(new Path(playlistAreaTagPath));

            String line;
            for (FileStatus f : st) {
                FSDataInputStream hdfsInStream = fs.open(f.getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
                while ((line = br.readLine()) != null) {

                    String[] lines = line.split("\t");

                    String readListId = lines[0];
                    if (listIds.contains(readListId)){
                        String lan = lines[1];
                        String style = lines[2];

                        HashMap<String, String> tempHmLan = string2Map(lan);
                        HashMap<String, String> tempHmStyle = string2Map(style);

                        listLanguage.put(readListId, tempHmLan);
                        listStyle.put(readListId, tempHmStyle);
                    }
                }
            }
        }

		public void getUserLanAndStyleWithUserId(Context context,
                                                 FileSystem fs,
                                                 String userId,
                                                 HashMap<String, HashMap<String, String>> userLanguageMap,
                                                 HashMap<String, HashMap<String, String>> userStyleMap) throws IOException, InterruptedException{

            String userTagPath = context.getConfiguration().get("user_tag_new");
            if (userTagPath == null)
                return;

            FileStatus[] st = fs.listStatus(new Path(userTagPath));

            String line;
            for (FileStatus f : st) {
                FSDataInputStream hdfsInStream = fs.open(f.getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
                while ((line = br.readLine()) != null)
                {
                    String[] lines = line.split("\t");
                    String readUserId = lines[0];
                    if (userId.equals(readUserId)) {
                        if (lines[1].startsWith("lan")) {
                            String languagePromote = lines[2];
                            HashMap<String, String> tempHm = string2Map(languagePromote);
                            userLanguageMap.put(readUserId, tempHm);
                        }else if (lines.length == 2) {
                            String stylePromote = lines[1];
                            HashMap<String, String> tempHm = string2Map(stylePromote);
                            userStyleMap.put(readUserId, tempHm);
                        }
                    }
                }//while
            }
        }


		/**
		 * 这个里面主要用
		 * @param key  第一个是userid
		 * @param values
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		protected void reduce(LongPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			// HashSet<String> visitedList = new HashSet<String>();
			HashSet<String> clickedList = new HashSet<String>();
			HashMap<String, Integer> impressCount = new HashMap<String, Integer>();
			//List<GenericPair<String, Float>> resultList = new LinkedList<GenericPair<String, Float>>();
			HashSet<String> impressSet = new HashSet<String>();
			HashSet<String> impressTagSet = new HashSet<String>();
			HashMap<String, Float > recMap = new HashMap<String, Float >();
			HashMap<String, String > reasonMap = new HashMap<String, String>();

            String userId = String.valueOf(key.getFirst());
            HashMap<String, HashMap<String, String>> nowUserLan = new HashMap<>();
            HashMap<String, HashMap<String, String>> nowUserStyle = new HashMap<>();

			int[] userfeature = null ;
			
			long uid = key.getFirst();
			Float newSongWeight = 0f;//是否喜欢新歌
			boolean isActive = false;
			int srcCount = 0;
			long filetype = -1111;
			for (Text text : values)
			{
				String str = text.toString();
				filetype = key.getSecond();
				
				if(filetype == -10)//activeTime
				{
					
					long sec = Long.valueOf(str);
					isActive = (sec >= lastestSec);
					
				}
				else if (filetype == -8) {
					if(!isActive)
						return;
					
					String[] featureStr = str.split(",");
					userfeature = new int[featureStr.length];
					for(int i = 0; i < featureStr.length; i++ )
						userfeature[i] = Integer.parseInt(featureStr[i]);
				}
				else if(filetype == -5)//用户profile
				{
					if(!isActive)
					{
						return;
					}
				
					newSongWeight = Float.valueOf(str);//
				}
			
				else if (filetype == 1)// 歌单推荐数据
				{//tags
					if(!isActive)
					{
						return;
					}
					srcCount ++;
					
					String[] tagScores = str.split(",");
					int num = 0;
					for (String tagScore : tagScores)
					{
						
						int pos = tagScore.indexOf(":");
						String tag = tagScore.substring(0, pos);
						float prefscore = Float.parseFloat(tagScore.substring(pos + 1));
						if (tag.isEmpty() )
							continue;
						
						if(impressTagSet.contains(tag))
							prefscore = prefscore / 2000;
						
						LinkedList<GenericPair<String, Float>> playlists = tag2playlistMap.get(tag);
					
						
						if(playlists == null)
							continue;
						
						num = 0;
						for (GenericPair<String, Float> playlistScore : playlists)
						{//id:score
							
							String playlist = playlistScore.first;
							//Float score = Float.valueOf(playlistScore.substring(pos + 1));
							Float quality = playlistScore.second;
							//filter
							ListInfo info  = listInfoMap.get(Long.parseLong(playlist) );
							if(info == null || info.score == 0)
								continue;
							
							if( (uid == 2011l  || uid == 1005l ) &&  info.createDays > 360)
								continue;
							
							if (impressSet.contains(playlist) || clickedList.contains(playlist) || badPlaylistSet.contains(playlist))
								continue;
							
							
							//test
							float newScore = prefscore * quality ;
							Float oldScore = recMap.get(playlist);
							if(oldScore == null)
								oldScore = 0f;
							
							recMap.put(playlist, newScore + oldScore);
							
							//recList.add(new GenericPair<String, Float>(playlist, newScore));
							
							String reason = reasonMap.get(playlist);
							if (reason == null)
							{
								reason = tag;
								reasonMap.put(playlist, reason);
							} 
							
							//if(++num >= 20)
								//break;
							
							if(++num >= 30)
							break;
						}
					
						
						
					}

				} 
				else if(filetype == -1)
				// 用户看过的歌单
				{
					if(!isActive)
					{
						return;
					}
					String[] array = str.split("\t");
					// visitedList.add(array[0]);
					int score = Integer.valueOf(array[1]);
					if (score >= 1 || score <= maxImpress) // 点击或曝光超过阈值的，直接过滤
						clickedList.add(array[0]);
					impressCount.put(array[0], score);
				}
				else if(filetype == -2)
				{
					impressSet.add(str);
				}
				else if(filetype == -3)
				{
					String[] array = str.split(":");
					impressSet.add(array[0]);
					impressTagSet.add(array[2]);
				}
                /**
                 * 换成case?
                 * -101 语言  HashMap<String, String> nowUserLan
                 * -100 风格 HashMap<String, String> nowUserStyle
                 */
                else if (filetype == -101) {
                    HashMap<String, String> hm = string2Map(str);
                    nowUserLan.put(userId, hm);
                }else if (filetype == -100) {
                    HashMap<String, String> hm = string2Map(str);
                    nowUserStyle.put(userId, hm);
                }
			}//for (Text text : values)
			
			if(srcCount <= 0 || reasonMap.isEmpty())
				return;

			Comparator<GenericPair<String, Float>> comp = new Comparator<GenericPair<String, Float>>()
			{
				@Override
				public int compare(GenericPair<String, Float> o1, GenericPair<String, Float> o2)
				{
					if (o1.second > o2.second)
						return -1;
					else if (o1.second < o2.second)
						return 1;
					else 
						return 0;
				}
			};

			
			//候选得分
			List<GenericPair<String, Float>> recList = new LinkedList<GenericPair<String, Float>>();
			for(Map.Entry<String, Float> e : recMap.entrySet())
			{
				ListInfo info  = listInfoMap.get(Long.parseLong(e.getKey()));
				if(info == null)
				{
					continue;
				}
				//总分数 + recScore
				//float newScore = e.getValue() * (info.score + info.rec_subRate * 0.2f);
				float newScore = e.getValue();
				if(newSongWeight > 1)//用户喜欢新歌
				{
					if(info.pubTime < 60 )
						newScore = newScore * newSongWeight;
				}
				
				if (impressCount.containsKey(e.getKey()))
				{
					int count = 1 - impressCount.get(e.getKey());
					float decay = (float) (2.5 * Math.pow(count, 1 / 3));
					newScore /= decay;
				}
				recList.add(new GenericPair<String, Float>(e.getKey(), newScore));
			}
			
			Collections.sort(recList, comp);
			
			//前n个
			List<GenericPair<String, Float>> recommendPlaylists = new LinkedList<GenericPair<String, Float>>();
			HashMap<String, Integer> srcCountMap = new HashMap<String, Integer>();
		
			
			int num_each = 30;
			
			int recNum = 0;
			HashSet<String> reasonTagSet = new HashSet<String>();


			for(GenericPair<String, Float> p : recList)
			{
				float score = p.second;
				
				String reason = reasonMap.get(p.first);
				Integer count = srcCountMap.get(reason);
				reasonTagSet.add(reason);
				
				if(count == null)
					count = 0;
				
				if(count >= num_each)
					continue;
				//else if(count >= 1)
					//score = -count + score / (count * 10);
				
				//BigDecimal bd = new BigDecimal(score );   
				//bd = bd.setScale(4,BigDecimal.ROUND_HALF_UP);  
				
				recommendPlaylists.add(new GenericPair<String, Float>(p.first, score));
				
				srcCountMap.put(reason, count + 1);
				
				if ( ++ recNum >= 360)
					break;
			}
			//topn重新排序
			//Collections.sort(recommendPlaylists, comp);

			List<GenericPair<String, Float>> recommendPlaylists_lr = new LinkedList<GenericPair<String, Float>>();

            /**
             * ab test 部分
             */
            int divisor = Integer.valueOf(context.getConfiguration().get("divisor"));
            int remainder = Integer.valueOf(context.getConfiguration().get("remainder"));
			for (GenericPair<String, Float> p : recommendPlaylists)
			{
				float newscore = p.second;

				if (key.getFirst() % divisor == remainder) {
                    double[] feature = getFeatureVec(
                                                userId,
                                                p.first,
                                                userfeature,
                                                playlistFeatureMap,
                                                originalPlaylistfeatureSize,
                                                playlistSubRatio_Promoted_Map,
                                                reasonMap,
                                                nowUserLan,
                                                nowUserStyle,
                                                listLanguage,
                                                listStyle);

                    newscore = p.second / 1000 + (float)Rank.LRRankPointWithoutBias(weights, feature);

                }else{
                    newscore = p.second / 1000 + rankByLR2(
                            String.valueOf(key.getFirst()),
                            p.first,
                            weights_ab,
                            userfeature,
                            playlistFeatureMap,
                            originalPlaylistfeatureSize,
                            playlistSubRatio_Promoted_Map,
                            reasonMap);
                }

				recommendPlaylists_lr.add(new GenericPair<String, Float>(p.first, newscore));
			}
		
					
			Collections.sort(recommendPlaylists_lr, comp);
			
			//前n个
			recommendPlaylists = new LinkedList<GenericPair<String, Float>>();
			srcCountMap = new HashMap<String, Integer>();
			
			srcCount = reasonTagSet.size();
			num_each = maxRecommendation / srcCount;
			if(num_each < 2)
				num_each = 2;
			else if(num_each > 5)
				num_each = 5;
				
			recNum = 0;
			for(GenericPair<String, Float> p : recommendPlaylists_lr)
			{
				float score = p.second;
				//String id = p.first;
				
				String reason = reasonMap.get(p.first);
				Integer count = srcCountMap .get(reason);
				if(count == null)
					count = 0;
				
				if(count >= num_each)
					continue;
				else if(count >= 1)
					score = -count + score / (count * 10);
				
				BigDecimal bd = new BigDecimal(score );
				bd = bd.setScale(4, BigDecimal.ROUND_HALF_UP);
				
				recommendPlaylists.add(new GenericPair<String, Float>
				(p.first + ":" + bd.floatValue() + ":" + reason , score));
				
				srcCountMap.put(reason, count + 1);
				
				if ( ++ recNum >= maxRecommendation)
					break;
			}
			
			
			//topn重新排序
			Collections.sort(recommendPlaylists, comp);
			//输出
			if (!recommendPlaylists.isEmpty())
			{
				StringBuffer buffer = new StringBuffer();
				for (GenericPair<String, Float> p : recommendPlaylists)
				{
					buffer.append(p.first).append(",");
				}
				String outValue = buffer.toString();
				outValue = outValue.substring(0, outValue.length() - 1);
				context.write(new Text(String.valueOf(key.getFirst())), new Text(outValue));
			}
		}
	}


	public static float rankByLR(String userID, String pid, double[] weights,
                                 int[]userfeature, HashMap<String, BitSet> playlistFeatureMap, int playlistfeature_size) {
		
		double lrScore_user = 0;
		int userfeature_size = 56;
		//int songfeature_size = 250;
		//float []totalFeature = new float[playfeature_size + userfeature_size ];
		if(userfeature != null)//可算，可不算
		{
			for(int i = 0;i < userfeature.length; i++)
			{
				lrScore_user += weights[i] * userfeature[i];
				//totalFeature[i] = userfeature[i];
			}
			
			//userfeature_size = userfeature.length;
		}
		
		BitSet playlistfeature = playlistFeatureMap.get(pid);
		double lrScore_song = 0;
		if(playlistfeature != null)
		{
			for(int i = 0;i < playlistfeature_size; i++)
			{
				lrScore_song += weights[i + userfeature_size] *(playlistfeature.get(i) ? 1 : 0);
				//totalFeature[i + userfeature_size] = (playlistfeature.get(i) ? 1 : 0);
			}
			
			return (float) sigmoid(lrScore_song + lrScore_user );
		}
		else
		{
			return 0.05f;
		}
		
	}
	private static double sigmoid(double x)
	{
		return 1 / (1 + Math.pow(Math.E, -1 * x));
	}
	
	public static float rankByLR2(String userID, String pid, double[] weights,
                                  int[]userfeature, HashMap<String, BitSet> playlistFeatureMap, int playlistfeature_size,
                                  HashMap<String,HashMap<String,Float>> playlistSubRatio_Promoted_Map,
                                  HashMap<String, String> recommendSrcMap) {
		
		double lrScore_user = 0;
		int userfeature_size = 56;
		//int songfeature_size = 250;
		//float []totalFeature = new float[playfeature_size + userfeature_size ];
		if(userfeature != null)//可算，可不算
		{
			for(int i = 0;i < userfeature.length; i++)
			{
				lrScore_user += weights[i] * userfeature[i];
				//totalFeature[i] = userfeature[i];
			}
			
			//userfeature_size = userfeature.length;
		}
		
		BitSet playlistfeature = playlistFeatureMap.get(pid);
		double lrScore_song = 0;
		if(playlistfeature != null)
		{
			for(int i = 0;i < playlistfeature_size; i++)
			{
				lrScore_song += weights[i + userfeature_size] *(playlistfeature.get(i) ? 1 : 0);
				//totalFeature[i + userfeature_size] = (playlistfeature.get(i) ? 1 : 0);
			}
			
			float ratio_Promoted = 0.05f;
			HashMap<String, Float> ratio_Promoted_Map = playlistSubRatio_Promoted_Map.get(pid);
			if(ratio_Promoted_Map != null)
			{
				String src = recommendSrcMap.get(pid);
				Float value = ratio_Promoted_Map.get(src);
				if(value != null)
					ratio_Promoted = value;
			}
			
			double otherFeatureScore = weights[playlistfeature_size + userfeature_size] * ratio_Promoted;
			
			return (float) sigmoid(otherFeatureScore + lrScore_song + lrScore_user );
		}
		else
		{
			return 0.05f;
		}
		
	}
	@Override
	public int run(String[] args) throws Exception
	{
		Options options = new Options();
		//options.addOption("input", true, "input directory");
		options.addOption("output", true, "output directory");
		options.addOption("visited", true, "visited song list");
		options.addOption("listProfile", true, "listProfile");
		options.addOption("listQuality", true, "listQuality");
		options.addOption("maxRecommendation", true, "maxRecommendation number of playlist per user");
		options.addOption("badPlaylist", true, "badPlaylist should filter");
		options.addOption("maxImpress", true, "max playlist Impress");

		options.addOption("portrayMetadata", true, "portrayMetadata");// 用户特征元信息
		options.addOption("userPortrayPath", true, "userPortrayPath");// 用户特征数据的路径

		options.addOption("day", true, "day");//最近多少天活跃的用户输出推荐结果
		options.addOption("userActiveTime", true, "userActiveTime");//最近多少天活跃的用户输出推荐结果
		options.addOption("badPlaylist2", true, "badPlaylist2");//
		//options.addOption("createDays", true, "createDays");
		options.addOption("recSubRatio_default", true, "0.07");
		options.addOption("userTag", true, "userTag");
		options.addOption("tag2playlist", true, "tag2playlist");
		
		options.addOption("mainpage_impress", true, "mainpage_impress");
		options.addOption("rec_history", true, "rec_history");
		
		options.addOption("userFeature", true, "input");
		options.addOption("playlistFeature", true, "input");
		options.addOption("LRweights", true, "LRweights");
		options.addOption("LRweights_ab", true, "LRweights_ab");
		options.addOption("playlistSubRatio_Promoted", true, "playlistSubRatio_Promoted");

        /**
         * 增加4个
         */
        options.addOption("user_tag_new", true, "user_tag_new");
        options.addOption("playlistAreaTag", true, "playlistAreaTag");
        options.addOption("remainder", true, "remainder");
        options.addOption("divisor", true, "divisor");

		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		
		String userTag = cmd.getOptionValue("userTag");
		String tag2playlist = cmd.getOptionValue("tag2playlist");
	
		
		String userActiveTime = cmd.getOptionValue("userActiveTime");
		String output = cmd.getOptionValue("output");
		String visited = cmd.getOptionValue("visited");
		String portrayMetadata = cmd.getOptionValue("portrayMetadata");
		String userPortrayPath = cmd.getOptionValue("userPortrayPath");
		String[]mainpage_impress = cmd.getOptionValues("mainpage_impress");
		String rec_history = cmd.getOptionValue("rec_history");


		String userFeature = cmd.getOptionValue("userFeature");
		String playlistFeature = cmd.getOptionValue("playlistFeature");
		
		//int createDays = Integer.parseInt(cmd.getOptionValue("createDays"));
		float recSubRatio_default = Float.parseFloat(cmd.getOptionValue("recSubRatio_default"));
		
		Configuration conf = getConf();
		conf.setFloat("recSubRatio_default", recSubRatio_default);
		conf.set("userFeature", userFeature);
		conf.set("playlistFeature", playlistFeature);
		conf.set("LRweights", cmd.getOptionValue("LRweights"));
		conf.set("LRweights_ab", cmd.getOptionValue("LRweights_ab"));
		conf.set("playlistSubRatio_Promoted", cmd.getOptionValue("playlistSubRatio_Promoted"));
		//conf.setInt("createDays", createDays);
		if (cmd.hasOption("maxRecommendation"))
		{
			int maxRecommendation = Integer.valueOf(cmd.getOptionValue("maxRecommendation"));
			conf.setInt("maxRecommendation", maxRecommendation);
		}
		if (cmd.hasOption("badPlaylist"))
		{
			String badPlaylist = cmd.getOptionValue("badPlaylist");
			conf.set("badPlaylist", badPlaylist);
		}

		if (cmd.hasOption("maxImpress"))
		{
			int maxImpress = Integer.valueOf(cmd.getOptionValue("maxImpress"));
			conf.setInt("maxImpress", maxImpress);
		}

	
		conf.set("userTag", userTag);
		conf.set("tag2playlist", tag2playlist);
		
		conf.set("listProfile", cmd.getOptionValue("listProfile"));
		conf.set("listQuality", cmd.getOptionValue("listQuality"));
		conf.set("portrayMetadata", portrayMetadata);
		conf.set("userPortrayPath", userPortrayPath);
		conf.set("day",cmd.getOptionValue("day"));
		conf.set("userActiveTime", userActiveTime);
		conf.set("badPlaylist2", cmd.getOptionValue("badPlaylist2"));

        /**
         * 增加参数, 应该有4个  用户标签, 歌单标签, abtest的除数, 余数
         * by 钱伟 2017-08-15
         */
        String user_tag_new = cmd.getOptionValue("user_tag_new");
        String playlistAreaTag = cmd.getOptionValue("playlistAreaTag");
        String remainder = cmd.getOptionValue("remainder");
        String divisor = cmd.getOptionValue("divisor");
        conf.set("user_tag_new", user_tag_new);
        conf.set("playlistAreaTag", playlistAreaTag);
        conf.set("remainder", remainder);
        conf.set("divisor", divisor);
		

		Job sumJob = new Job(conf, "playlist recommend tag job ");

		sumJob.setJarByClass(UserPlaylistRecByTag_withSort.class);

		sumJob.setMapperClass(RecommendMapper.class);
		sumJob.setMapOutputKeyClass(LongPair.class);
		sumJob.setMapOutputValueClass(Text.class);
		sumJob.setReducerClass(RecommendReducer.class);
		sumJob.setOutputKeyClass(Text.class);
		sumJob.setOutputValueClass(Text.class);

		sumJob.setPartitionerClass(LongPair.FirstPartitioner.class);
		sumJob.setGroupingComparatorClass(LongPair.FirstGroupingComparator.class);
		sumJob.setSortComparatorClass(LongPair.KeyComparator.class);

		for(String path : mainpage_impress)
		{
			FileInputFormat.addInputPath(sumJob, new Path(path));
		}
		if( !rec_history.equals("null"))
			FileInputFormat.addInputPath(sumJob, new Path(rec_history));
		
		FileInputFormat.addInputPath(sumJob, new Path(userTag));
		FileInputFormat.addInputPath(sumJob, new Path(visited));
		FileInputFormat.addInputPath(sumJob, new Path(userPortrayPath));//用户profile
		FileInputFormat.addInputPath(sumJob, new Path(userActiveTime));
		FileInputFormat.addInputPath(sumJob, new Path(userFeature));
		
		FileOutputFormat.setOutputPath(sumJob, new Path(output ));
		if (!sumJob.waitForCompletion(true))
		{
			throw new InterruptedException("playlist recommend tag job  " + args[0]);
		}
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new Configuration(), new UserPlaylistRecByTag_withSort(), args);
	}
}
