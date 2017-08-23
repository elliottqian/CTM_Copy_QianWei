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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.*;


/*
 * 根据用户喜欢的歌单，推荐歌单（喜欢的含义是播放次数多，并且收藏过）
 *
 */
public class UserPlaylistRecByLikedPlaylist_withSort2_test2 extends Configured implements Tool
{

	/*
	 * input1: playlist \t playlist:score,playlist:score,playlist:score
	 * input2:Id \t userID \t PlaylistId \t Time \t Type \t Privacy \t
	 * externalType \t SubscribeEventId input3: userid \t
	 * paylist:score,playlist:socre,.... output1:playlist \t
	 * playlist:score,playlist:score,playlist:score(相似歌单) output2:playlist \t
	 * userID \t time (收藏) output3:playlist \t userid \t play \t score (播放分数)
	 */
	public static class PlaylistMapper extends Mapper<WritableComparable<?>, Text, LongPair, Text>
	{
		int filetype = -111111;
		@Override
		public void setup(Context context) throws IOException
		{
			String file = ((FileSplit)context.getInputSplit()).getPath().toString();
			
			String similarity2 = context.getConfiguration().get("similarity2");
			
			if(file.contains(similarity2))
				filetype = -2;
			
		}
		
		@Override
		protected void map(WritableComparable<?> key, Text value, Context context) throws IOException, InterruptedException
		{
			LongPair outKey = new LongPair();
			String str = value.toString();
			String[] array = str.split("\t");
			InputSplit inputSplit = context.getInputSplit();
			String fileName = ((FileSplit) inputSplit).getPath().toString();
			
			if (fileName.contains("mainpage_log") )
			{//action + "\t" + userID + "\t" + id + "\t" + type + "\t" + source + "\t" + sourceId + "\t" + logTime + "\t" + os + "\t" + time
				String action = array[0];
				String type = array[3];
				String source = array[4];
					if( !action.equals("play") || !type.equals("song") ||!source.equals("list") )
						return;

					Long userID = Long.valueOf(array[1]);
					if(userID < 0)
						userID *= -1;
					if(userID == 0)
						return;
					
					String playlist = array[5];
					if (playlist.isEmpty() || !playlist.matches("[0-9]+"))
						return;
					
					outKey.set(Long.valueOf(playlist), 3);
					context.write(outKey, new Text(userID + "\t" + "source" + "\t0.05"));
					
			}
			else if (filetype == -2)// 相似歌单数据
			{
				outKey.set(Long.valueOf(array[0]), -2);
				
				context.write(outKey, new Text(array[1]));
			}
			else if (array.length > 2)// 用户歌单收藏数据
			{
				String time = array[3];
				Integer type = Integer.valueOf(array[4]);
				Long userID = Long.valueOf(array[1]);
				if (userID < 0 || type == 5)
					return;
				String playlistID = array[2];
				outKey.set(Long.valueOf(playlistID), 1);
				context.write(outKey, new Text(userID + "\t" + time));
			} else if (fileName.contains("user_list_profile"))// 歌单收听数据
			{
				Long userID = Long.valueOf(array[0]);
				String[] playlistScores = array[1].split(",");
				for (String playlistScore : playlistScores)
				{
					int pos = playlistScore.indexOf(":");
					String playlist = playlistScore.substring(0, pos);
					String score = playlistScore.substring(pos + 1);
					if (playlist.isEmpty() || score.isEmpty())
						continue;
					outKey.set(Long.valueOf(playlist), 2);
					context.write(outKey, new Text(userID + "\t" + "play" + "\t" + score));
				}
			} 
			else
			// 相似歌单数据
			{
				outKey.set(Long.valueOf(array[0]), 0);
				
				context.write(outKey, new Text(array[1]));
				
			}
		}
	}

	/*
	 * input1:key is playlistID value is
	 * playlist:score,playlist:score,playlist:score input2:key is playlistID
	 * value is userID \t time input3:key is playlistId value is userid \t play
	 * \t score output:userId \t playlist:score,playlist:score,playlist:score..
	 * \t score \t playlistID(reason)
	 */
	public static class PlaylistReducer extends Reducer<LongPair, Text, Text, Text>
	{
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{

		}

		@Override
		protected void reduce(LongPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			HashSet<String> subscrbieUser = new HashSet<String>();
			HashSet<String> playUser = new HashSet<String>();
			String similarPlaylists = "", similarPlaylists_test = "";
			long filetype = -1111;
			for (Text text : values)
			{
				String str = text.toString();
				filetype = key.getSecond();
				if(filetype == -2)
				{
					similarPlaylists_test = str;
				}
				else if (str.contains(":"))// 相似歌单数据
				{
					similarPlaylists = str;
				} else if (str.contains("play"))// 用户播放数据
				{
					String[] array = str.split("\t");
					String user = array[0];
					String score = array[2];
					if (similarPlaylists.isEmpty() )
					{
						return;
					}
					
					if(!playUser.add(user))
						continue;
					
					if (subscrbieUser.contains(user))// 用户既播放，又收藏了该歌单
					{
						//context.write(new Text(user), new Text(similarPlaylists + "\t" + score + "\t" + key.getFirst()));
						if(Long.parseLong(user) == 44001 && key.getFirst() == 3778678)
						{
							continue;
						}
						
						if(Long.parseLong(user) % 2 == 1 && Long.parseLong(user) != 44001)
							context.write(new Text(user), new Text(similarPlaylists + "\t" + score + "\t" + key.getFirst()));
						else
						{
							if (similarPlaylists_test.isEmpty() )
							{
								continue;
							}
							context.write(new Text(user), new Text(similarPlaylists_test + "\t" + score + "\t" + key.getFirst()));
						}
					}
				} else if (str.contains("source"))// 用户播放数据
				{
					String[] array = str.split("\t");
					String user = array[0];
					String score = array[2];
					if (similarPlaylists.isEmpty() )
					{
						return;
					}
					if(!playUser.add(user))
						continue;
					
					if(Long.parseLong(user) == 44001 && key.getFirst() == 3778678)
					{
						continue;
					}
					
					if (subscrbieUser.contains(user))// 用户既播放，又收藏了该歌单
					{
						if(Long.parseLong(user) % 2 == 1 && Long.parseLong(user) != 44001)
							context.write(new Text(user), new Text(similarPlaylists + "\t" + score + "\t" + key.getFirst()));
						else
						{
							if (similarPlaylists_test.isEmpty() )
							{
								continue;
							}
							context.write(new Text(user), new Text(similarPlaylists_test + "\t" + score + "\t" + key.getFirst()));
						}
							
					}
				}
				else
				// 用户歌单收藏
				{
					int pos = str.indexOf("\t");
					if(pos > 0)
					{	
						String userID = str.substring(0, pos);
						subscrbieUser.add(userID);
					}
				}
			}
		}
	}

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
		private String userFeature;
		private String user_tag_new;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			userActiveTime = context.getConfiguration().get("userActiveTime");
			userPortrayPath = context.getConfiguration().get("userPortrayPath");
			userFeature = context.getConfiguration().get("userFeature");
			user_tag_new = context.getConfiguration().get("user_tag_new");
			
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
			Path path = ((FileSplit) context.getInputSplit()).getPath();
			String fileName = path.toString();
			String str = value.toString();
			LongPair outKey = new LongPair();
			if(fileName.contains(userActiveTime))
			{
				String[]array = str.split("\t",-1);
				outKey.set(Long.valueOf(array[0]), -10);
				context.write(outKey,new Text(array[1]));
			}
			else if(fileName.contains(user_tag_new))
			{
				//user_tag_new
				int pos = str.indexOf("\t");
				outKey.set(Long.valueOf(str.substring(0, pos)), -7);
				context.write(outKey, new Text(value.toString().substring(pos + 1)));
			}
			else if(fileName.contains("getUser_XinJiang"))
			{
				long userID = Long.parseLong(value.toString());
				if(userID  < 0)
					userID = userID * -1;
				outKey.set(Long.valueOf(userID), -9);
				context.write(outKey, new Text(outKey.getSecond() + "\t"+ userID));
			}
			else if (fileName.contains(userPortrayPath))
			{
				String[]array = str.split("\t",-1);
				outKey.set(Long.valueOf(array[0]), -5);
				Integer newSongTypeIndex = userPortrayIndex.get("newSongType");
				Integer newSongWeightIndex =  userPortrayIndex.get("newSongWeight");
				if(newSongTypeIndex <= array.length-1)
				{
					String newSongType = array[newSongTypeIndex];
					Float newSongWeight = Float.valueOf(array[newSongWeightIndex]);
					context.write(outKey, new Text("userprofile"+"\05"+ newSongWeight));
				}
				
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
				context.write(outKey, new Text( array[3]));
				
			}
			else if(fileName.contains("stage1") )
			{
				int pos = str.indexOf("\t");
				if (pos > 0)// 推荐数据
				{
					String userID = str.substring(0, pos);
					outKey.set(Long.valueOf(userID), 0);
					context.write(outKey, new Text(str.substring(pos + 1)));
				}
			}
			else if(fileName.contains("user_action/list") )
			{
					String[] array = str.split(",");
					outKey.set(Long.valueOf(array[0]), -1);
					context.write(outKey, new Text(array[1] + "\t" + array[2]));
				
			}
			else if (fileName.contains(userFeature)){
				int pos = str.indexOf("\t");
				String userID = str.substring(0, pos);
				String feature = str.substring(pos + 1);
				outKey.set(Long.valueOf(userID), -8);
				
				context.write(outKey, new Text(feature));
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
		public float score;
		public  ListInfo()
		{
			pubTime = 0;
			score = 0;
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
		int listQualityCount = 0;
		int listSimilarCount = 0;
		private long lastestSec = 0;
		HashMap<String, BitSet> playlistFeatureMap = new HashMap<String, BitSet>();
		private int originalPlaylistfeatureSize = 0;
		double[] weights = null;
		double[] weights_ab = null;
		HashMap<String, HashMap<String,Float>> playlistSubRatio_Promoted_Map = new HashMap<String, HashMap<String,Float>>();
		HashMap<String, HashMap<String, Float>> playlist_tagRatioMap = new HashMap<String, HashMap<String, Float>>();
		HashMap<String, HashMap<String, Float>> playlist_lanRatioMap = new HashMap<String, HashMap<String, Float>>();
		HashSet<String> arabicPlaylist = new HashSet<String>();

		HashMap<String, String[]> memberPlaylistInfo = null; //add by qianwei
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			maxRecommendation = context.getConfiguration().getInt("maxRecommendation", 48);
			maxImpress = 0 - context.getConfiguration().getInt("maxImpress", 2);
			int day = context.getConfiguration().getInt("day", 60);
			if(day > 0)
			{
				lastestSec = getNowDayMills()/1000 - day*3600*24;
			}
			
			String badPlaylist = context.getConfiguration().get("badPlaylist");
			if (badPlaylist != null)
			{
				FileSystem fs = FileSystem.get(context.getConfiguration());
				FileStatus[] st = fs.listStatus(new Path(badPlaylist));
				String line = null;
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
				
			
			String similarity = context.getConfiguration().get("similarity");
			String listProfile = context.getConfiguration().get("listProfile");
			String listQuality = context.getConfiguration().get("listQuality");

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
						if (array.length > 0 && array[0].matches("[0-9]+"))
						{
							listSet.add(Long.valueOf(array[0]));
						}
						if(array.length >=2 )//增加相似歌单，一般是对称的，即第一列里包含了所有歌单
						{
							List<GenericPair<String, Float>> lists = getList(array[1]);
							for(GenericPair<String, Float> p:lists)
							{
								listSet.add(Long.valueOf(p.first));
							}
						}
					}
				}
			}
			
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
						
						if(!listSet.contains(Long.valueOf(pid)))
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
			
			JSONParser parser = new JSONParser();
			st = fs.listStatus(new Path(listProfile));
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
						if (array.length == 2 && listSet.contains(listId))
						{
							try
							{
								JSONObject obj = (JSONObject) parser.parse(array[1]);
								long pubTime = (Long) obj.get("avgPubTime");
								ListInfo listInfo = new ListInfo();
								listInfo.pubTime = (int)(pubTime/1000000);
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
			
			
			st = fs.listStatus(new Path(listQuality));
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
						if (array.length == 2 && listSet.contains(listId))
						{
							try
							{
								JSONObject obj = (JSONObject) parser.parse(array[1]);
								double score = (Double) obj.get("score");
								ListInfo listInfo = listInfoMap.get(listId);
								if(listInfo == null)
								{
									listInfo = new ListInfo();
									listInfoMap.put(listId, listInfo);
								}
								listInfo.score = (float) score;
								listQualityCount++;
							}
							catch(Exception e)
							{
								e.printStackTrace();
								throw new IOException(e.getMessage());
							}
						}
					}
				}
			}
			listSimilarCount = listSet.size();
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
						for(String str : array[3].split(","))
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

			memberPlaylistInfo = ReadListMemberInfo.readMemberPayList(context); //add by qianwei
		}



		@Override
		protected void reduce(LongPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			// HashSet<String> visitedList = new HashSet<String>();
			HashSet<String> clickedList = new HashSet<String>();
			HashMap<String, Integer> impressCount = new HashMap<String, Integer>();
			List<GenericPair<String, Float>> resultList = new LinkedList<GenericPair<String, Float>>();
			HashMap<String, GenericPair<String, Float>> reasonMap = new HashMap<String, GenericPair<String, Float>>();
			HashMap<String, Float> recommendMap = new HashMap<String, Float>();
			Float newSongWeight = 0f;//是否喜欢新歌
			boolean isActive = false;
			int[] userfeature = null ;
			boolean user_XinJiang = false;
			long fileType = -11111;
			//String tagRatio = "null";
			List<GenericPair<String, Float>> user_lanRatio = new LinkedList<GenericPair<String, Float>>();
			//String fatiRatio = "null";
			//String lanRatio = "null";
			List<GenericPair<String, Float>> user_tagRatio = new LinkedList<GenericPair<String, Float>>();
			for (Text text : values)
			{
				String str = text.toString();
				fileType = key.getSecond();
				
				if(fileType == -10)
				{//activeTime
					long sec = Long.valueOf(str);
					isActive = (sec >= lastestSec);
					
					if(!isActive)
						return;
				
				}
				else if (fileType == -9) {
					if(!isActive)
						return;
					
					user_XinJiang = true;
				}
				else if (fileType == -8) 
				{
					if(!isActive)
						return;
					
					String[] featureStr = str.split(",");
					userfeature = new int[featureStr.length];
					for(int i = 0; i < featureStr.length; i++ )
						userfeature[i] = Integer.parseInt(featureStr[i]);
				}
				else if (fileType == -7) 
				{
					if(!isActive)
						return;
					
					//user_tag_new
					String[]infos = str.split("\t");
					if (infos.length == 1) {
						String tagRatio = infos[0];
						for(String tagscore : tagRatio.split(","))
						{
							int pos = tagscore.indexOf(":");
							user_tagRatio.add(
									new GenericPair<String, Float>(tagscore.substring(0, pos),
											Float.parseFloat(tagscore.substring(pos + 1)) )
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
							int pos = lanscore.indexOf(":");
							user_lanRatio.add(
									new GenericPair<String, Float>(lanscore.substring(0, pos),
											Float.parseFloat(lanscore.substring(pos + 1)) )
											);
						}
					}
					
					
				}
				else if(fileType == -5)//用户profile 或是activeTime
				{
					if(!isActive)
					{
						return;
					}
					
					String[] array = str.split("\05");
					
					newSongWeight = Float.valueOf(array[1]);
				}
				//else if (str.contains(":"))// 歌单推荐数据
				else if(fileType == 0)
				{
					if(!isActive)
					{
						return;
					}
					String[] array = str.split("\t");
					resultList.add(new GenericPair<String, Float>(array[0], Float.valueOf(array[1])));
					String[] playlists = array[0].split(",");
					for (String playlistScore : playlists)
					{
						int pos = playlistScore.indexOf(":");
						String playlist = playlistScore.substring(0, pos);
						Float score = Float.valueOf(playlistScore.substring(pos + 1));
						GenericPair<String, Float> reason = reasonMap.get(playlist);
						if (reason == null)
						{
							reason = new GenericPair(array[2], score);
							reasonMap.put(playlist, reason);
						} else if (reason.second < score)
						{
							reason = new GenericPair(array[2], score);
							reasonMap.put(playlist, reason);
						}

					}

				}
				else if (fileType == -2)  //mainpage impress
				{
					if(!isActive)
						return;
					
					clickedList.add(str);
					
				}
				else if (fileType == -1) 
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
			}

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

			Collections.sort(resultList, comp);// 按照歌单收听分数，降序排列
			/*
			List<Float> scoreList = new ArrayList<Float>();
			for (int i = 0; i < resultList.size(); i++)
			{
				String[] playlists = resultList.get(i).first.split(",");
				for (String playlistScore : playlists)
				{
					int pos = playlistScore.indexOf(":");
					String playlist = playlistScore.substring(0, pos);
					if (clickedList.contains(playlist) || badPlaylistSet.contains(playlist))
						continue;
					long playlistId = Long.valueOf(playlist);
					ListInfo info  = listInfoMap.get(playlistId);
					if(info == null || info.score == 0)
					{
						continue;
					}
					Float score = Float.valueOf(playlistScore.substring(pos + 1));
					scoreList.add(score);
				}
			}
			boolean overFiltered = false;// 被过度的过滤推荐结果
		
			/*
			Collections.sort(scoreList);
			Collections.reverse(scoreList);

			float threshold = 0.0f;
			if (scoreList.size() > 0)
				threshold = scoreList.size() > 64 ? scoreList.get(63) : scoreList.get(scoreList.size() - 1);
				*/
			
			int firstNum = 640;
			int size = resultList.size();
			int eachNum = 30;
			
			if(size >= 10)
				eachNum = 20;
			
			int playlistCount = 0;
			
			for (int i = 0; i < resultList.size(); i++)
			{
				String[] playlists = resultList.get(i).first.split(",");
				playlistCount = 0;
				for (String playlistScore : playlists)
				{
					int pos = playlistScore.indexOf(":");
					String playlist = playlistScore.substring(0, pos);
					boolean needPunish = false;
					
					long playlistId = Long.valueOf(playlist);
					ListInfo info  = listInfoMap.get(playlistId);
					
					if((info == null || info.score == 0) && listQualityCount>listSimilarCount/2)//listQualityCount表示歌单质量正常生成
					{
						continue;
					}
					
					if (clickedList.contains(playlist) || badPlaylistSet.contains(playlist))
						continue;
					
					if ( !user_XinJiang && arabicPlaylist.contains(playlist))
						continue;
					
					if (impressCount.containsKey(playlist))
					{
						needPunish = true;
					}
					float orginScore = Float.valueOf(playlistScore.substring(pos + 1));
					
					//if (orginScore < threshold)
					//	continue;
					
					Float score = (float) (orginScore / Math.sqrt(i + 1));
					if (needPunish)
					{
						int count = 1 - impressCount.get(playlist);
						float decay = (float) (2.5 * Math.pow(count, 1 / 3));
						score /= decay;

					}
					
					Float oldScore = recommendMap.get(playlist);
					if (oldScore == null)
					{
						recommendMap.put(playlist, score);
					} else
					{
						recommendMap.put(playlist, score + oldScore);
					}
					
					if( playlistCount ++ >= eachNum)
						break;
				}
				if (recommendMap.size() >= firstNum)
					break;
			}

			List<Map.Entry<String, Float>> recommendPlaylists = new LinkedList<Map.Entry<String, Float>>(recommendMap.entrySet());
			if(newSongWeight > 1)//用户喜欢新歌
			{
				for(Map.Entry<String, Float> e : recommendPlaylists)
				{
					if(e.getKey().matches("[0-9]+"))
					{
						ListInfo listInfo =listInfoMap.get(Long.valueOf(e.getKey()));
						//Integer pubTime = listAvgPubTime.get(Integer.valueOf(e.getKey()));
						if(listInfo == null)
						{
							continue;
						}
						float ratio = listInfo.pubTime/1254f;//1254是30%线
						if(ratio>1)
						{
							float oldValue = e.getValue();
							e.setValue(oldValue*ratio*newSongWeight*1.5f);//提升新歌较多歌单的分数
						}
					}
				}
			}

            /**
             * ab test 部分
             */
            int divisor = Integer.valueOf(context.getConfiguration().get("divisor"));
            int remainder = Integer.valueOf(context.getConfiguration().get("remainder"));
			for (Map.Entry<String, Float> e : recommendPlaylists)//加上歌单质量评分
			{
				ListInfo listInfo = listInfoMap.get(Long.valueOf(e.getKey()));
				
				float score = e.getValue();
				if(listInfo != null && listInfo.score > 0)
				{
					score += listInfo.score;
				}
				
				float newscore = score  ;


				int moneyWeight = 70;
				int clickWeight = 800;
                // 付费歌单的权重
                double payPlaylistWeight = ReadListMemberInfo.createMemberPlaylistWeight(
                        String.valueOf(key.getFirst()),
                        e.getKey(),
                        memberPlaylistInfo,
						moneyWeight,
						clickWeight);//add by qianwei

				if (key.getFirst() % divisor == remainder)
					newscore = e.getValue() / 1000000 + rankByLR3(String.valueOf(key.getFirst()), e.getKey(),
							weights, userfeature, playlistFeatureMap, originalPlaylistfeatureSize, playlistSubRatio_Promoted_Map, reasonMap,
							user_lanRatio, user_tagRatio, this.playlist_lanRatioMap, this.playlist_tagRatioMap)
							+ (float)(sigmoid(payPlaylistWeight) * 0.3 - 0.15);
				else
					newscore = e.getValue() / 1000000 + rankByLR3(String.valueOf(key.getFirst()), e.getKey(),
							weights, userfeature, playlistFeatureMap, originalPlaylistfeatureSize, playlistSubRatio_Promoted_Map, reasonMap,
							user_lanRatio, user_tagRatio, this.playlist_lanRatioMap, this.playlist_tagRatioMap);

				/*else
					newscore = e.getValue() / 1000000 + rankByLR2(String.valueOf(key.getFirst()), e.getKey(),
						weights, userfeature, playlistFeatureMap, originalPlaylistfeatureSize, playlistSubRatio_Promoted_Map, reasonMap);
				*/
				e.setValue(newscore);
			}
			
			Collections.sort(recommendPlaylists, new Comparator<Map.Entry<String, Float>>()
					{
						public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2)
						{
							return -o1.getValue().compareTo(o2.getValue());
						}
					});
					
			
			HashMap<String, Integer> reasonCount = new HashMap<String, Integer>();
			for (Map.Entry<String, Float> e : recommendPlaylists)
			{
				String pid = e.getKey();
				//float score = e.getValue();
				GenericPair<String, Float> reason = reasonMap.get(pid);
				Integer count = reasonCount.get(reason.first);
				if(count == null)
					count = 0;
				
				if(count >= 2 )
				{
					e.setValue(e.getValue() / (count * 1.5f) );
				}
				
				reasonCount.put(reason.first, count + 1);
			}
			
			Collections.sort(recommendPlaylists, new Comparator<Map.Entry<String, Float>>()
					{
						public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2)
						{
							return -o1.getValue().compareTo(o2.getValue());
						}
					});
			
			if (!recommendPlaylists.isEmpty())
			{
				int count = 0;
				StringBuilder builder = new StringBuilder();
				for (Map.Entry<String, Float> e : recommendPlaylists)
				{
					GenericPair<String, Float> reason = reasonMap.get(e.getKey());
					
					BigDecimal bd = new BigDecimal(e.getValue());
					bd = bd.setScale(4, BigDecimal.ROUND_HALF_UP);
					
					builder.append(e.getKey() + ":" + bd.floatValue() + ":" + reason.first + ",");
					if (++count > maxRecommendation)
						break;
				}
				String outValue = builder.toString();
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
                                  HashMap<String,HashMap<String,Float>> playlistSubRatio_Promoted_Map, HashMap<String, GenericPair<String, Float>> recommendSrcMap) {
		
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
			
			float ratio_Promoted = 0.09f;
			HashMap<String, Float> ratio_Promoted_Map = playlistSubRatio_Promoted_Map.get(pid);
			if(ratio_Promoted_Map != null)
			{
				GenericPair<String, Float> p = recommendSrcMap.get(pid);
				String src = p.first;
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
                                  HashMap<String,HashMap<String,Float>> playlistSubRatio_Promoted_Map, HashMap<String, GenericPair<String, Float>> recommendSrcMap,
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
			
			float ratio_Promoted = 0.09f;
			HashMap<String, Float> ratio_Promoted_Map = playlistSubRatio_Promoted_Map.get(pid);
			if(ratio_Promoted_Map != null)
			{
				GenericPair<String, Float> p = recommendSrcMap.get(pid);
				String src = p.first;
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
		options.addOption("input", true, "input directory");
		options.addOption("output", true, "output directory");
		options.addOption("visited", true, "visited song list");
		options.addOption("similarity", true, "playlist similarity");
		options.addOption("similarity2", true, "playlist similarity");
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
		
		options.addOption("mainpage_impress", true, "mainpage_impress");
		options.addOption("mainpage_log", true, "mainpage_log");
		options.addOption("userFeature", true, "input");
		options.addOption("playlistFeature", true, "input");
		options.addOption("LRweights", true, "LRweights");
		options.addOption("LRweights_ab", true, "LRweights_ab");
		options.addOption("playlistSubRatio_Promoted", true, "playlistSubRatio_Promoted");
		options.addOption("playlistAreaTag", true, "input");//music_recommend/mainpage_playlist/playlistAreaTag2/final
		options.addOption("user_tag_new", true, "input");
		options.addOption("user_XinJiang", true, "user_XinJiang");
		options.addOption("arabicPlaylist", true, "arabicPlaylist");
		options.addOption("playlist_member_info_path", true, "song playlist_member_info_path");// 新加的 by qianwei
        options.addOption("divisor", true, "song divisor");// 新加的 by qianwei
        options.addOption("remainder", true, "song remainder");// 新加的 by qianwei

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		String[] inputs = cmd.getOptionValues("input");
		String output = cmd.getOptionValue("output");
		String visited = cmd.getOptionValue("visited");
		String portrayMetadata = cmd.getOptionValue("portrayMetadata");
		String userPortrayPath = cmd.getOptionValue("userPortrayPath");
		String userActiveTime = cmd.getOptionValue("userActiveTime");
		
		String[]mainpage_impress = cmd.getOptionValues("mainpage_impress");
		String[]mainpage_log = cmd.getOptionValues("mainpage_log");
		String userFeature = cmd.getOptionValue("userFeature");
		String playlistFeature = cmd.getOptionValue("playlistFeature");
		String similarity2 = cmd.getOptionValue("similarity2");
		String playlistAreaTag = cmd.getOptionValue("playlistAreaTag");
		String user_tag_new = cmd.getOptionValue("user_tag_new");
		String user_XinJiang = cmd.getOptionValue("user_XinJiang");
		String arabicPlaylist = cmd.getOptionValue("arabicPlaylist");
		String playlist_member_info_path = cmd.getOptionValue("playlist_member_info_path");// 新加的 by qianwei
        String remainder = cmd.getOptionValue("remainder");// 新加的 by qianwei
        String divisor = cmd.getOptionValue("divisor");// 新加的 by qianwei
		
		Configuration conf = getConf();
		conf.set("userFeature", userFeature);
		conf.set("playlistFeature", playlistFeature);
		conf.set("LRweights", cmd.getOptionValue("LRweights"));
		conf.set("LRweights_ab", cmd.getOptionValue("LRweights_ab"));
		conf.set("playlistSubRatio_Promoted", cmd.getOptionValue("playlistSubRatio_Promoted"));
		conf.set("similarity2", similarity2);
		conf.set("playlistAreaTag", playlistAreaTag);
		conf.set("user_tag_new", user_tag_new);
		conf.set("arabicPlaylist", arabicPlaylist);
		conf.set("playlist_member_info_path", playlist_member_info_path);// 新加的 by qianwei
        conf.set("divisor", divisor);// 新加的 by qianwei
        conf.set("remainder", remainder);// 新加的 by qianwei
		
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

		conf.set("similarity", cmd.getOptionValue("similarity"));
		conf.set("listProfile", cmd.getOptionValue("listProfile"));
		conf.set("listQuality", cmd.getOptionValue("listQuality"));
		conf.set("portrayMetadata", portrayMetadata);
		conf.set("userPortrayPath", userPortrayPath);
		conf.set("day",cmd.getOptionValue("day"));
		conf.set("userActiveTime", userActiveTime);
		conf.set("badPlaylist2", cmd.getOptionValue("badPlaylist2"));
		
		Job job = new Job(conf, "user songlist recommend contentbased job stage1");
		job.setJarByClass(UserPlaylistRecByLikedPlaylist_withSort2_test2.class);

		job.setMapperClass(PlaylistMapper.class);
		job.setMapOutputKeyClass(LongPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(PlaylistReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(LongPair.FirstPartitioner.class);
		job.setGroupingComparatorClass(LongPair.FirstGroupingComparator.class);
		job.setSortComparatorClass(LongPair.KeyComparator.class);

		for(String path : mainpage_log)
		{
			FileInputFormat.addInputPath(job, new Path(path));
		}
		
		for (String input : inputs)
			FileInputFormat.addInputPath(job, new Path(input));
		
		FileInputFormat.addInputPath(job, new Path(similarity2));
		
		FileOutputFormat.setOutputPath(job, new Path(output + "/stage1"));
		
		if (!job.waitForCompletion(true))
		{
			throw new InterruptedException("user songlist recommend contentbased job stage1 " + args[0]);
		}

		Job sumJob = new Job(conf, "user songlist recommend contentbased job stage2");

		sumJob.setJarByClass(UserPlaylistRecByLikedPlaylist_withSort2_test2.class);

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
		FileInputFormat.addInputPath(sumJob, new Path(user_tag_new));
		FileInputFormat.addInputPath(sumJob, new Path(userPortrayPath));//用户profile
		FileInputFormat.addInputPath(sumJob, new Path(userActiveTime));
		FileInputFormat.addInputPath(sumJob, new Path(user_XinJiang));
		for(String path : mainpage_impress)
		{
			FileInputFormat.addInputPath(sumJob, new Path(path));
		}
		
		
		FileOutputFormat.setOutputPath(sumJob, new Path(output + "/final"));
		if (!sumJob.waitForCompletion(true))
		{
			throw new InterruptedException("user songlist recommend contentbased job stage2 " + args[0]);
		}
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new Configuration(), new UserPlaylistRecByLikedPlaylist_withSort2_test2(), args);
	}
}
