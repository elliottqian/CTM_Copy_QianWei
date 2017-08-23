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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


public class RecommendPlaylistByAddedSong_withSort2 extends Configured implements Tool
{
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

	/*
	 * input1: songId \t playlist:score,playlist:socre,.... 
	 * input2: user song:score:time,song:score:time
	 */
	public static class SongMapper extends Mapper<WritableComparable<?>, Text, LongPair, Text>
	{
		@Override
		protected void map(WritableComparable<?> key, Text value, Context context) throws IOException, InterruptedException
		{
			LongPair outKey = new LongPair();
			String str = value.toString();
			String[] array = str.split("\t");
			InputSplit inputSplit = context.getInputSplit();
			String fileName = ((FileSplit) inputSplit).getPath().toString();
			if(fileName.contains("user_sub_profile"))
			{
				String user = array[0];
				String[] playsongs = array[1].split(",");
				for(String songScore:playsongs)
				{
					String song  = songScore.split(":")[0];
					String score  = songScore.split(":")[1]; //相关分数
					outKey.set(Long.valueOf(song), 1);
					context.write(outKey, new Text(user+"\t"+score));
				}
			}
			else if(fileName.contains("resort_songPlaylist"))
			{
				outKey.set(Long.valueOf(array[0]), -1);
				context.write(outKey, new Text(array[1]));
			}
			
		}
	}

	/*
	 * input1: LongPair(songId,1)  user
	 * input2: LongPair(songId,0) \t playlist:score,playlist:socre,.... 
	 * output:userid \t recomend \t song 
	 */
	public static class SongReducer extends Reducer<LongPair, Text, Text, Text>
	{
		@Override
		protected void reduce(LongPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String recommend = "";
			String song = String.valueOf(key.getFirst());
			long filetype = -1111;
			for (Text text : values)
			{
				String str = text.toString();
				filetype = key.getSecond();
				if (filetype == -1)
				{
					recommend = str;
				}
				else 
				{
					String user = text.toString().split("\t")[0];
					String related_score = text.toString().split("\t")[1];
					if (recommend.isEmpty())
						return;
					
					context.write(new Text(user), new Text(recommend + "\t" + song+"\t"+related_score));
				}
			}
		}
	}

	/*
	 * input1:userId \t playlist:score,playlist:score,playlist:score.. \t song related_score
	 * input2:userid,playlist,score (user visited playlist) 
	 * input3:useractive
	 * input4:user \t songid:count:time,songid:count:time
	 * output1:userId \t  playlist:score,playlist:score,playlist:score.. \t song
	 * output2:userid \t playlist \t score (user visited playlist)
	 */
	public static class RecommendMapper extends Mapper<WritableComparable<?>, Text, LongPair, Text>
	{
		private String userActiveTime;
		String userFeature;
		private String fileName;
		private String user_tag_new;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			userActiveTime = context.getConfiguration().get("userActiveTime");
			userFeature = context.getConfiguration().get("userFeature");
			user_tag_new = context.getConfiguration().get("user_tag_new");
			InputSplit inputSplit = context.getInputSplit();
			fileName = ((FileSplit) inputSplit).getPath().toString();
		}
		
		@Override
		protected void map(WritableComparable<?> key, Text value, Context context) throws IOException, InterruptedException
		{
			String str = value.toString();
			int pos = str.indexOf("\t");
			LongPair outKey = new LongPair();
			if(fileName.contains(userActiveTime))
			{
				String userID = str.substring(0, pos);
				String time = str.substring(pos + 1);
				outKey.set(Long.valueOf(userID), -9);
				context.write(outKey, new Text(outKey.getSecond() + "\t"+time));
			}
			else if(fileName.contains(user_tag_new))
			{
				//user_tag_new
				
				outKey.set(Long.valueOf(str.substring(0, pos)), -6);
				context.write(outKey, new Text(outKey.getSecond() + "\t" + value.toString().substring(pos + 1)));
			}
			else if(fileName.contains("getUser_XinJiang"))
			{
				long userID = Long.parseLong(value.toString());
				if(userID  < 0)
					userID = userID * -1;
				outKey.set(Long.valueOf(userID), -7);
				context.write(outKey, new Text(outKey.getSecond() + "\t"+ userID));
			}
			else if (fileName.contains(userFeature)){
				String userID = str.substring(0, pos);
				String feature = str.substring(pos + 1);
				outKey.set(Long.valueOf(userID), -8);
				
				context.write(outKey, new Text(outKey.getSecond() + "\t"
						+ feature));
			}
			else if (fileName.contains("user_playprofile"))
			{
				String userID = str.substring(0, pos);
				String playsongs = str.substring(pos + 1);
				outKey.set(Long.valueOf(userID), 0);
				context.write(outKey, new Text(outKey.getSecond() + "\t"+ playsongs));
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
				context.write(outKey, new Text(outKey.getSecond() + "\t"+ array[3]));
				
			}
			else if (pos > 0)//rec
			{
				String userID = str.substring(0, pos);
				String recommend = str.substring(pos + 1);
				outKey.set(Long.valueOf(userID), 1);
				context.write(outKey, new Text(outKey.getSecond() + "\t"+recommend));
			} else //page
			{
				String[] array = str.split(",");
				String userID =array[0];
				String list = array[1];
				String score = array[2];
				outKey.set(Long.valueOf(userID), -1);
				context.write(outKey, new Text(outKey.getSecond() + "\t"+list + "\t" + score));
			}
		}
	}

	/*
	 * input1:userId  playlist:score,playlist:score,playlist:score.. song
	 * input2:userid  playlist \t  score (user visited playlist) 
	 * output:user \t playlist:score:song,playlist:score:song
	 */
	public static class RecommendReducer extends Reducer<LongPair, Text, Text, Text>
	{
		int maxImpress;
		private HashMap<String, Float> listQualityMap = new HashMap<String, Float>();
		private HashMap<String, Float> listQualityMap_test = new HashMap<String, Float>();
		private HashSet<String> nocoverSet = new HashSet<String>();
		
		private int listSimilarCount = 0;
		private long lastestSec = 0;
		HashMap<String, BitSet> playlistFeatureMap = new HashMap<String, BitSet>();
		private int originalPlaylistfeatureSize = 0;
		double[] weights = null;
		double[] weights_ab = null;
		HashMap<String, HashMap<String,Float>> playlistSubRatio_Promoted_Map = new HashMap<String, HashMap<String,Float>>();
		HashSet<String> arabicPlaylist = new HashSet<String>();
		HashMap<String, HashMap<String, Float>> playlist_tagRatioMap = new HashMap<String, HashMap<String, Float>>();
		HashMap<String, HashMap<String, Float>> playlist_lanRatioMap = new HashMap<String, HashMap<String, Float>>();

		HashMap<String, String[]> memberPlaylistInfo = null;  //added by qw

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			maxImpress = 0 - context.getConfiguration().getInt("maxImpress", 2);
			int day = context.getConfiguration().getInt("day", 60);
			if(day > 0)
			{
				lastestSec = getNowDayMills()/1000 - day*3600*24;
			}
			String similarity = context.getConfiguration().get("similarity");
			String listQuality = context.getConfiguration().get("listQuality");
			String coverStatus = context.getConfiguration().get("coverStatus");
			
			HashSet<Long> listSet = new HashSet<Long>();
			String line;
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FileStatus[] st = fs.listStatus(new Path(similarity));
			for (FileStatus f : st)
			{
				if (!f.getPath().getName().contains("_logs"))
				{
					FSDataInputStream hdfsInStream = fs.open(f.getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
					while ((line = br.readLine()) != null)
					{
						String[] array = line.split("\t", -1);
						if (array.length >= 2)// 锟斤拷锟狡歌单
						{
							List<GenericPair<String, Float>> lists = getList(array[1]);
							for (GenericPair<String, Float> p : lists)
							{
								listSet.add(Long.valueOf(p.first));
							}
						}
					}
				}
			}
			listSimilarCount = listSet.size();
			
//			st = fs.listStatus(new Path(listQuality));
//			String [] info = null;
//			line="";
//			JSONParser parser = new JSONParser();
//			for(FileStatus file: st){
//				FSDataInputStream input=fs.open(file.getPath());
//				BufferedReader reader=new BufferedReader(new InputStreamReader(input));
//				
//				while( (line = reader.readLine()) != null){
//					info = line.split("\t");
//					listQualityMap.put(info[0], Float.parseFloat(info[2]));
//					try
//					{
//						JSONObject object = (JSONObject) parser.parse(info[4]);
//						if(object.containsKey("scoreRec_play_playend"))
//							listQualityMap_test.put(info[0], ((Double)object.get("scoreRec_play_playend")).floatValue());
//						else
//							listQualityMap_test.put(info[0], 0.2f);
//					
//					}catch (Exception e)
//					{
//						
//					}
//				}
//			}
			
			/*
			st = fs.listStatus(new Path(coverStatus ));
			for (FileStatus f : st)
			{
				if (!f.getPath().getName().contains("_logs"))
				{
					FSDataInputStream hdfsInStream = fs.open(f.getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
					while ((line = br.readLine()) != null)
					{//// id \t userid \t bookedcount \t tags \t trackcount \t coverStatus
						String[] array = line.split("\t", -1);
						long listId = Long.valueOf(array[0]);
						if (array.length >= 6 && listSet.contains(listId))
						{
							/**interface CoverStatus {
							/**
							 * 无封面
							 /
							static final int noCover = 0;

							static final int single_cover = 1;

							static final int composed_cover = 2;

							static final int user_upload = 3;
						}/
							
							if( !array[5].equals("3"))
								nocoverSet.add(array[0]);
						}
					}
				}
			}*/
			
			//
			String playlistAreaTag = context.getConfiguration().get("playlistAreaTag");;
			st = fs.listStatus(new Path(playlistAreaTag));
			for (FileStatus f : st)
			{
				if (!f.getPath().getName().contains("_logs"))
				{
					FSDataInputStream hdfsInStream = fs.open(f.getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
					while ((line = br.readLine()) != null)
					{

						String[] array = line.split("\t");
						String pid = array[0];
						
						if(!listSet.contains(Long.valueOf(pid)) )
							continue;
						
					 String lanRatio = array[1];
	               	 String tagRatio = array[2];
	               	 
	               	 HashMap<String, Float> lanMap = new HashMap<String, Float>();
	               	 for (String s : lanRatio.split(",")) {

								int innerPos = s.indexOf(":");
								if (innerPos > 0) {
									String lan = s.substring(0, innerPos);
									float score = Float.valueOf(s
											.substring(innerPos + 1));
									lanMap.put(lan, score);
								
										
								}
							}
	               	playlist_lanRatioMap.put(pid, lanMap);
	               	 
	               	HashMap<String, Float> tagMap = new HashMap<String, Float>();
	               	 for (String s : tagRatio.split(",")) {

								int innerPos = s.indexOf(":");
								if (innerPos > 0) {
									String lan = s.substring(0, innerPos);
									float score = Float.valueOf(s
											.substring(innerPos + 1));
									tagMap.put(lan, score);
								}
							}
	               	 
	               	playlist_tagRatioMap.put(pid, tagMap);
	                
					}
				}
			}
			
			listSet.clear();
			
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
			
			//
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
			
			//
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
			
			//
			st = fs.listStatus(new Path(context.getConfiguration().get("playlistSubRatio_Promoted")));
			
			for (FileStatus f : st) {
				if (!f.getPath().getName().contains("_logs")) {
					FSDataInputStream hdfsInStream = fs.open(f.getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
					while ((line = br.readLine()) != null) {
						String[] array = line.trim().split("\t", -1);
						HashMap<String, Float> map = new HashMap<String, Float>();
						for(String str : array[1].split(","))
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
			
			//
			st = fs.listStatus(new Path(context.getConfiguration().get("arabicPlaylist")));
			
			for (FileStatus f : st) {
				if (!f.getPath().getName().contains("_logs")) {
					FSDataInputStream hdfsInStream = fs.open(f.getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
					while ((line = br.readLine()) != null) {
						arabicPlaylist.add(line.trim());
					}
				}
			}

			/**
			 * 读取歌单付费信息
			 */
			memberPlaylistInfo = ReadListMemberInfo.readMemberPayList(context);  // added by qw
		}

		@Override
		protected void reduce(LongPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			HashSet<String> clickedList = new HashSet<String>();
			HashMap<String, Integer> impressCount = new HashMap<String, Integer>();
			List<GenericPair<String, Float>> recStrList = new LinkedList<GenericPair<String, Float>>();
			HashMap<String, Float> recommendScoreMap = new HashMap<String, Float>();
			HashMap<String, String> recommendSrcMap = new HashMap<String, String>();
			HashMap<String, Integer> songPlayCount = new HashMap<String, Integer>();
			List<GenericPair<String, Float>> resultList = new LinkedList<GenericPair<String, Float>>();
			boolean isActive = false;
			boolean user_XinJiang = false;
			int[] userfeature = null ;
			List<GenericPair<String, Float>> user_lanRatio = new LinkedList<GenericPair<String, Float>>();
			List<GenericPair<String, Float>> user_tagRatio = new LinkedList<GenericPair<String, Float>>();
//			long fileType = -11111;
			for (Text text : values)
			{
				String str = text.toString();
//				fileType = key.getSecond();
				int pos = str.indexOf("\t");
				String tag= str.substring(0,pos);
				String content = str.substring(pos+1);
				
				if(tag.equals("-9")) //active
				{
					Long sec = Long.valueOf(content);
					isActive = (sec >= lastestSec);
					if(!isActive)
						return;
				}
				else if (tag.equals("-8")) {
					if(!isActive)
						return;
					
					String[] featureStr = content.split(",");
					userfeature = new int[featureStr.length];
					for(int i = 0; i < featureStr.length; i++ )
						userfeature[i] = Integer.parseInt(featureStr[i]);
				}
				else if (tag.equals("-7")) {
					if(!isActive)
						return;
					
					user_XinJiang = true;
				}
				else if (tag.equals("-6")) 
				{
					if(!isActive)
						return;
					
					//user_tag_new
					String[]infos = content.split("\t");
					if (infos.length == 1) {
						String tagRatio = infos[0];
						for(String tagscore : tagRatio.split(","))
						{
							int pos_index = tagscore.indexOf(":");
							user_tagRatio.add(
									new GenericPair<String, Float>(tagscore.substring(0, pos_index),
											Float.parseFloat(tagscore.substring(pos_index + 1)) )
											);
						}
					}
					/*else if (infos[0].equals("fati")) {
						fatiRatio = infos[1];
					}*/
					else if (infos[0].equals("lan")) {
						String lanRatio = infos[1];
						for(String lanscore : lanRatio.split(","))
						{
							int pos_index = lanscore.indexOf(":");
							user_lanRatio.add(
									new GenericPair<String, Float>(lanscore.substring(0, pos_index),
											Float.parseFloat(lanscore.substring(pos_index + 1)) )
											);
						}
					}
					
					
				}
				else if (tag.equals("1")) //recommend
				{
					if(!isActive)
						return;
					
					String[] array = content.split("\t");
					String recStr = array[0];
					String song = array[1];
					String songScore = array[2];
					recStrList.add(new GenericPair<String, Float>(recStr, Float.valueOf(songScore)));
					for (String playlistScore : recStr.split(","))
					{
						int newpos = playlistScore.indexOf(":");
						String playlist = playlistScore.substring(0, newpos);
						recommendSrcMap.put(playlist, song);
					}
				}
				else if (tag.equals("-1"))  //user visit
				{
					if(!isActive)
						return;
					String[] array = content.split("\t");
					int score = Integer.valueOf(array[1]);
					if (score >= 1 || score <= maxImpress)	
						clickedList.add(array[0]);
					impressCount.put(array[0], score);
				}
				else if (tag.equals("-2"))  //mainpage impress
				{
					if(!isActive)
						return;
					
					clickedList.add(content);
					
				}
				else if (tag.equals("0"))  //play
				{
					if(!isActive)
						return;
					String playAction = content;
					for (String songAction : playAction.split(","))
					{
						String[] array = songAction.split(":");
						String song = array[0];
						Integer count = Integer.valueOf(array[1]);
						songPlayCount.put(song, count);
					}
				}
			}

			Comparator<GenericPair<String, Float>> comp = new Comparator<GenericPair<String, Float>>()
			{
				@Override
				public int compare(GenericPair<String, Float> o1, GenericPair<String, Float> o2)
				{
					
					return - o1.second.compareTo(o2.second);
				}
			};
			Collections.sort(recStrList, comp);

			int playlistCount = 0;
			float score = 0;
			
			int size = recStrList.size();
			int eachNum = 30;
			if(size >= 10)
				eachNum = 18;
			for (int i = 0; i < recStrList.size() ; i++)
			{
				score = recStrList.get(i).second;
				String[] playlistScoreArray = recStrList.get(i).first.split(",");
				playlistCount = 0;
				for (String playlistScore : playlistScoreArray)
				{
					String[] array = playlistScore.split(":");
					String playlist = array[0];
					if (clickedList.contains(playlist))
						continue;
					
					if ( !user_XinJiang && arabicPlaylist.contains(playlist))
						continue;
					
					//
					//Float score = (float) ( Math.sqrt(Float.valueOf(array[1]) ) / (i + 1));
					score = 1;
					//score = score / (i + 1);
					if (impressCount.containsKey(playlist))
					{
						
						int count = 1 - impressCount.get(playlist);
						float decay = (float) (2.5 * Math.pow(count, 1 / 3));
						score /= decay;
						
						//continue;
					}

//					Float quality = listQualityMap.get(playlist);
//					if(key.getFirst() % 2 == 0)
//					{
//						//quality = listQualityMap_test.get(playlist);
//						quality = 1f;
//						score = score * Float.valueOf(array[1]) ;
//					}
					
					Float quality = 1f;
					score = score * Float.valueOf(array[1]) ;
					
					/*
					if(quality == null)
					{
						//continue;
						quality = -1f;
					}
					
					if(nocoverSet.contains(playlist))
					{
						score = score/ 10000;
						continue;
					}*/
					
					score = score * quality;
					
					Float oldScore = recommendScoreMap.get(playlist);
					if (oldScore == null)
					{
						recommendScoreMap.put(playlist, score);
					} else if (oldScore < score)
					{
						recommendScoreMap.put(playlist, score);
					}
					
					if( playlistCount ++ >= eachNum)
						break;
				}
				
				if (recommendScoreMap.size() >= 640)
					break;
			}

			/*sort playlists by song score*/
			List<Map.Entry<String, Float>> recommendPlaylists = new LinkedList<Map.Entry<String, Float>>(recommendScoreMap.entrySet());

			/*Collections.sort(recommendPlaylists, new Comparator<Map.Entry<String, Float>>()
			{
				public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2)
				{
					return -o1.getValue().compareTo(o2.getValue());
				}
			});*/

			List<GenericPair<String, Float>> recommendPlaylists_lr= new LinkedList<GenericPair<String, Float>>();

			/**
			 * abTest 部分
			 */
			int divisor = Integer.valueOf(context.getConfiguration().get("divisor"));
			int remainder = Integer.valueOf(context.getConfiguration().get("remainder"));
            int moneyWeight = 80;
            int clickWeight = 0;
            double factor = 0.12;

			// 这边就是排序主体
			// 对每一个歌单用LR  产生一个权重   这里只要把对应的用户  和对应的歌单  加上权重就可以了
			// 代码主体就是读取(歌单id, 金额率, 会员率)  根据   会员id, 歌单id  来用  金额转换率率, 会员转换率  加到LR权重里面
			// 把付费歌单读成一个HashMap,  这个hashMap的大小一般最大能有多大
			// e 代表一个歌单
			for (Map.Entry<String, Float> e : recommendPlaylists)
			{
				float newscore = 0;

				double payPlaylistWeight = ReadListMemberInfo.createMemberPlaylistWeight(
						String.valueOf(key.getFirst()),
						e.getKey(),
						memberPlaylistInfo,
                        moneyWeight,
                        clickWeight);

				// 最大只会增加0.20的权重
				float addedSortWeight =  (float)(sigmoid(payPlaylistWeight) * factor * 2 - factor);

				if( key.getFirst() % divisor == remainder)
					newscore = e.getValue() / 1000000 + rankByLR3(String.valueOf(key.getFirst()), e.getKey(),
								weights, userfeature, playlistFeatureMap, originalPlaylistfeatureSize, playlistSubRatio_Promoted_Map, recommendSrcMap,
								user_lanRatio, user_tagRatio, this.playlist_lanRatioMap, this.playlist_tagRatioMap) + addedSortWeight;
				else
					newscore = e.getValue() / 1000000 + rankByLR3(String.valueOf(key.getFirst()), e.getKey(),
							weights, userfeature, playlistFeatureMap, originalPlaylistfeatureSize, playlistSubRatio_Promoted_Map, recommendSrcMap,
							user_lanRatio, user_tagRatio, this.playlist_lanRatioMap, this.playlist_tagRatioMap);
			
				
				recommendPlaylists_lr.add(new GenericPair<String, Float>(e.getKey(), newscore));
			}
			Collections.sort(recommendPlaylists_lr, new Comparator<GenericPair<String, Float>>()
					{

						@Override
						public int compare(GenericPair<String, Float> arg0,
                                           GenericPair<String, Float> arg1) {
							return - arg0.second.compareTo(arg1.second);
						}
						
					});
			
			List<GenericPair<String, Float>> recommendPlaylists_result = new LinkedList<GenericPair<String, Float>>();
			HashMap<String, Integer> srcCount = new HashMap<String, Integer>();
			String src = "";
			float newscore = 0.0f;
			for (GenericPair<String, Float> p : recommendPlaylists_lr)
			{
				src = recommendSrcMap.get(p.first);
				Integer count = srcCount.get(src);
				if(count == null)
					count = 1;
				else
					count += 1;

				newscore = p.second / count;
				
				recommendPlaylists_result.add(new GenericPair<String, Float>(p.first, newscore));
				
				srcCount.put(src, count);
			}
			
			Collections.sort(recommendPlaylists_result, new Comparator<GenericPair<String, Float>>()
			{

				@Override
				public int compare(GenericPair<String, Float> arg0,
                                   GenericPair<String, Float> arg1) {
					return - arg0.second.compareTo(arg1.second);
				}
				
			});
			
			if (!recommendPlaylists_result.isEmpty())
			{
				int count = 0;
				StringBuffer builder = new StringBuffer();

                /**
                 *  二次重排序部分  对这个48首歌进行重排序, 根据付费歌单来重排序
                 *  另一个abtest
                 *  改变了  recommendPlaylists_result  中的分数
                 */
                reSortByListByPay(recommendPlaylists_result,   // 推荐列表
                                context,
                                key.getFirst(),                //用户id
                                memberPlaylistInfo);


                for (GenericPair<String, Float> p : recommendPlaylists_result)
                {
                    String list = p.first;
                    Float songScore = p.second;

                    if(count == 0)
                        builder.append(list + ":" + songScore+ ":" + recommendSrcMap.get(p.first));
                    else
                        builder.append(","+list+ ":" + songScore + ":" + recommendSrcMap.get(p.first));
                    if (++count > 48)
                        break;
                }



				if(builder.length() > 1)
				{
					String outValue = builder.toString();
					outValue = outValue.substring(0, outValue.length() - 1);
					context.write(new Text(String.valueOf(key.getFirst())), new Text(outValue));
				}
				
			}
		}

        /**
         * 根据歌单的付费情况对推荐列表的歌单重新进行排序
         * @param recommendPlaylists_result    歌单的推荐列表
         * @param context                      reduce的context
         * @param userId                       用户id
         * @param memberPlaylistInfo           记录歌单信息的类
         */
        public void reSortByListByPay(List<GenericPair<String, Float>> recommendPlaylists_result,
                                             Context context,
                                             long userId,
                                             HashMap<String, String[]> memberPlaylistInfo){
            if (recommendPlaylists_result == null)
                return;

            /** 另一个abtest的余数 */
            int remainder_2 = Integer.valueOf(context.getConfiguration().get("remainder_2"));
            int divisor = Integer.valueOf(context.getConfiguration().get("divisor"));


            if (userId % divisor == remainder_2) {

                if (recommendPlaylists_result.size() > 48) {
                    recommendPlaylists_result = recommendPlaylists_result.subList(0, 48);
                }

                /**
                 * 主要调整这3个参数
                 * 分别是:
                 *      缩放因子: 最大提升的额外权重
                 *      金钱权重
                 *      点击率权重
                 */
                double factor = 0.12;
                int moneyWeight = 83;
                int clickWeight = 0;

                for (GenericPair<String, Float> p : recommendPlaylists_result) {

                    double payPlaylistWeight = ReadListMemberInfo.createMemberPlaylistWeight(
                            String.valueOf(userId),
                            p.first,
                            memberPlaylistInfo,
                            moneyWeight,
                            clickWeight);

                    // 最大只会增加0.1的权重

                    float addedSortWeight =  (float)(sigmoid(payPlaylistWeight) * factor * 2 - factor);
                    p.second = (addedSortWeight + p.second) / (float)(factor + 1);
                }//for

                /**
                 * 调权后进行重排序
                 */
                Collections.sort(recommendPlaylists_result, new Comparator<GenericPair<String, Float>>()
                {

                    @Override
                    public int compare(GenericPair<String, Float> arg0,
                                       GenericPair<String, Float> arg1) {
                        return - arg0.second.compareTo(arg1.second);
                    }

                });
            }//if
        }//reSortByListByPay


	}//Reducer 结束



	static long getNowDayMills()
	{
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTimeInMillis();
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
                                  HashMap<String,HashMap<String,Float>> playlistSubRatio_Promoted_Map, HashMap<String, String> recommendSrcMap) {
		
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
			
			float ratio_Promoted = 0.04f;
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
	
	public static float rankByLR3(String userID, String pid, double[] weights,
                                  int[]userfeature, HashMap<String, BitSet> playlistFeatureMap, int playlistfeature_size,
                                  HashMap<String,HashMap<String,Float>> playlistSubRatio_Promoted_Map, HashMap<String, String> recommendSrcMap,
                                  List<GenericPair<String, Float>> userLanRatio, List<GenericPair<String, Float>> usertagRatio,
                                  HashMap<String, HashMap<String, Float>> playlist_lanRatioMap, HashMap<String, HashMap<String, Float>> playlist_tagRatioMap) {
		
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
			
			float ratio_Promoted = 0.04f;
			HashMap<String, Float> ratio_Promoted_Map = playlistSubRatio_Promoted_Map.get(pid);
			if(ratio_Promoted_Map != null)
			{
				String src = recommendSrcMap.get(pid);
				Float value = ratio_Promoted_Map.get(src);
				if(value != null)
					ratio_Promoted = value;
			}
			
			double otherFeatureScore = weights[playlistfeature_size + userfeature_size] * ratio_Promoted;
			
			float lanScore = 0, tagScore = 0;
			
           	for (GenericPair<String, Float> p : userLanRatio) {


				String lan = p.first;
				float score = p.second;
				if( score > 0.03 && playlist_lanRatioMap.containsKey(pid))
				{
					HashMap<String, Float> ratioMap = playlist_lanRatioMap.get(pid);
					if(ratioMap.containsKey(lan))
						lanScore += ratioMap.get(lan) * score;
				}
			
					
			
			}
   	 
   	 
           	for (GenericPair<String, Float> p : usertagRatio) {

				String tag = p.first;
				float score = p.second;
				if(!tag.equals("unknow") && !tag.equals("null") && score > 0.03 && playlist_tagRatioMap.containsKey(pid))
				{
					HashMap<String, Float> ratioMap = playlist_tagRatioMap.get(pid);
					if(ratioMap.containsKey(tag))
						tagScore += ratioMap.get(tag) * score;
				};
			
			}
           	otherFeatureScore += weights[playlistfeature_size + userfeature_size + 1] *  lanScore
           			+ weights[playlistfeature_size + userfeature_size + 2] *  tagScore;
			
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
		options.addOption("user_sub_profile", true, "input directory");
		options.addOption("output", true, "output directory");
		options.addOption("visited", true, "visited song list");
		options.addOption("songinfo", true, "song info");
		//options.addOption("song_play", true, "user song recent play");
		options.addOption("maxImpress", true, "max playlist Impress");
		
		options.addOption("similarity", true, "song playlist similarity");
		options.addOption("listQuality", true, "listQuality");
		
		options.addOption("day", true, "day");
		options.addOption("userActiveTime", true, "userActiveTime");
		
		options.addOption("coverStatus", true, "playlist CoverStatus info");
		options.addOption("mainpage_impress", true, "mainpage_impress");
		options.addOption("userFeature", true, "input");
		options.addOption("playlistFeature", true, "input");
		options.addOption("LRweights", true, "LRweights");
		options.addOption("LRweights_ab", true, "LRweights_ab");
		options.addOption("playlistSubRatio_Promoted", true, "playlistSubRatio_Promoted");
		options.addOption("user_XinJiang", true, "user_XinJiang");
		options.addOption("arabicPlaylist", true, "arabicPlaylist");
		options.addOption("playlistAreaTag", true, "input");//music_recommend/mainpage_playlist/playlistAreaTag2/final
		options.addOption("user_tag_new", true, "input");
		options.addOption("playlist_member_info_path", true, "song playlist_member_info_path");// 新加的 by qianwei
		options.addOption("divisor", true, "song divisor");// 新加的 by qianwei
		options.addOption("remainder", true, "song remainder");// 新加的 by qianwei
        options.addOption("remainder_2", true, "song remainder_2");// 新加的 by qianwei
		
		//options.addOption("badPlaylist", true, "badPlaylist");//
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

	
		String output = cmd.getOptionValue("output");
		String visited = cmd.getOptionValue("visited");
		String songInfo = cmd.getOptionValue("songinfo");
		//String songPlay = cmd.getOptionValue("song_play");
		String userActiveTime = cmd.getOptionValue("userActiveTime");
		String coverStatus = cmd.getOptionValue("coverStatus");
		String[]mainpage_impress = cmd.getOptionValues("mainpage_impress");
		String userFeature = cmd.getOptionValue("userFeature");
		String playlistFeature = cmd.getOptionValue("playlistFeature");
		String user_XinJiang = cmd.getOptionValue("user_XinJiang");
		String arabicPlaylist = cmd.getOptionValue("arabicPlaylist");
		String playlistAreaTag = cmd.getOptionValue("playlistAreaTag");
		String user_tag_new = cmd.getOptionValue("user_tag_new");
		String user_sub_profile = cmd.getOptionValue("user_sub_profile");
		String similarity = cmd.getOptionValue("similarity");
		String playlist_member_info_path = cmd.getOptionValue("playlist_member_info_path");// 新加的 by qianwei
		String remainder = cmd.getOptionValue("remainder");// 新加的 by qianwei
		String divisor = cmd.getOptionValue("divisor");// 新加的 by qianwei
        String remainder_2 = cmd.getOptionValue("remainder_2");// 新加的 by qianwei
		
		Configuration conf = getConf();

		conf.set("userFeature", userFeature);
		conf.set("playlistFeature", playlistFeature);
		conf.set("LRweights", cmd.getOptionValue("LRweights"));
		conf.set("LRweights_ab", cmd.getOptionValue("LRweights_ab"));
		conf.set("playlistSubRatio_Promoted", cmd.getOptionValue("playlistSubRatio_Promoted"));
		conf.set("arabicPlaylist", arabicPlaylist);
		
		if (cmd.hasOption("maxImpress"))
		{
			int maxImpress = Integer.valueOf(cmd.getOptionValue("maxImpress"));
			conf.setInt("maxImpress", maxImpress);
		}
		conf.set("listQuality", cmd.getOptionValue("listQuality"));
		conf.set("similarity", cmd.getOptionValue("similarity"));
		conf.set("userActiveTime", userActiveTime);
		conf.set("day", cmd.getOptionValue("day"));
		conf.set("coverStatus", cmd.getOptionValue("coverStatus"));
		conf.set("playlistAreaTag", playlistAreaTag);
		conf.set("user_tag_new", user_tag_new);
		conf.set("playlist_member_info_path", playlist_member_info_path);// 新加的 by qianwei
		conf.set("divisor", divisor);// 新加的 by qianwei
		conf.set("remainder", remainder);// 新加的 by qianwei
        conf.set("remainder_2", remainder_2);// 新加的 by qianwei
		//conf.set("badPlaylist", cmd.getOptionValue("badPlaylist"));
		
		Job job = new Job(conf, "user songlist recommend by added song stage1");
		job.setJarByClass(RecommendPlaylistByAddedSong_withSort2.class);
		job.setMapperClass(SongMapper.class);
		job.setMapOutputKeyClass(LongPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(SongReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(LongPair.FirstPartitioner.class);
		job.setGroupingComparatorClass(LongPair.FirstGroupingComparator.class);
		job.setSortComparatorClass(LongPair.KeyComparator.class);
		
		FileInputFormat.addInputPath(job, new Path(similarity));
		FileInputFormat.addInputPath(job, new Path(user_sub_profile));
		
		FileOutputFormat.setOutputPath(job, new Path(output + "/stage1"));
		if (!job.waitForCompletion(true))
		{
			throw new InterruptedException("user songlist recommend by added song stage1 failed" + args[0]);
		}

		Job sumJob = new Job(conf, "user songlist recommend by added song stage2");
		sumJob.setJarByClass(RecommendPlaylistByAddedSong_withSort2.class);
		sumJob.setMapperClass(RecommendMapper.class);
		sumJob.setMapOutputKeyClass(LongPair.class);
		sumJob.setMapOutputValueClass(Text.class);
		sumJob.setReducerClass(RecommendReducer.class);
		sumJob.setOutputKeyClass(Text.class);
		sumJob.setOutputValueClass(Text.class);
		sumJob.setPartitionerClass(LongPair.FirstPartitioner.class);
		sumJob.setGroupingComparatorClass(LongPair.FirstGroupingComparator.class);
		sumJob.setSortComparatorClass(LongPair.KeyComparator.class);
		FileInputFormat.addInputPath(sumJob, new Path(output + "/stage1"));
		FileInputFormat.addInputPath(sumJob, new Path(visited));
		//FileInputFormat.addInputPath(sumJob, new Path(songPlay));
		FileInputFormat.addInputPath(sumJob, new Path(userActiveTime));
		FileInputFormat.addInputPath(sumJob, new Path(user_tag_new));
		FileInputFormat.addInputPath(sumJob, new Path(userFeature));
		FileInputFormat.addInputPath(sumJob, new Path(user_XinJiang));
		for(String path : mainpage_impress)
		{
			FileInputFormat.addInputPath(sumJob, new Path(path));
		}
		
		FileOutputFormat.setOutputPath(sumJob, new Path(output + "/final"));
		if (!sumJob.waitForCompletion(true))
		{
			throw new InterruptedException("user songlist recommend by added song stage2 failed " + args[0]);
		}
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new Configuration(), new RecommendPlaylistByAddedSong_withSort2(), args);
	}

}
