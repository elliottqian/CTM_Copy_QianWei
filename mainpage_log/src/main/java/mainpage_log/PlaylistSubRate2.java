package mainpage_log;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

//统计播放率     各算法下的播放次数/总播放次数
public class PlaylistSubRate2 extends Configured implements Tool {
	
	public static class PlayRateMapper extends Mapper<WritableComparable<?>, Text, LongPair, Text> {

		String scene = "";
		String os = "";
		String timeDay ;
		int abtest = -1;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			scene = context.getConfiguration().get("scene");
			os = context.getConfiguration().get("os");
			timeDay = context.getConfiguration().get("timeDay");
			abtest =context.getConfiguration().getInt("abtest", -1);
		}
		
		@Override
		public void map(WritableComparable<?> key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] info = value.toString().split("\t", -1);
			
			if (info.length < 4) {
				return;
			}

			if (info[0].compareTo(scene) == 0) {

			
				String action = info[1];
				if (action.compareTo("recommendclick") == 0) {
					
					String timeStr = info[5];
					
					if(!timeDay.equals(timeStr))
						return;
					
					String curos = info[6];
					if (os.compareTo("all") != 0) {
						if (os.compareToIgnoreCase(curos) != 0) {
							return;
						}
					}
					Long userID = Long.valueOf(info[2]);
					String id = info[3];
					String alg = info[4];
					
					if(alg.contains(":"))
						return;

					if(abtest != -1  && userID > 0L)
					{
						if( userID % abtest == 0)
						{
							alg = alg +"_0";
						}
						else if ( userID % abtest == 1)
						{
							alg = alg +"_1";
						}
						else if ( userID % abtest == 2)
						{
							alg = alg +"_2";
						}
						else if ( userID % abtest == 3)
						{
							alg = alg +"_3";
						}
						else if ( userID % abtest == 4)
						{
							alg = alg +"_4";
						}
						else if ( userID % abtest == 5)
						{
							alg = alg +"_5";
						}else {
							alg = alg;
						}
					}
					
					LongPair outkey = new LongPair();
					outkey.set(userID, 1);
					context.write(outkey, new Text(id + "\t" + alg));
				}
				else if (action.compareTo("subscribe") == 0) {
					//"user-playlist" + "\t" + "subscribe" + "\t" + userID + "\t" + id + "\t" + timeStr + "\t" + os
					String curos = info[5];
					String timeStr = info[4];
					
					if(!timeDay.equals(timeStr))
						return;
					
					if (os.compareTo("all") != 0) {
						if (os.compareToIgnoreCase(curos) != 0) {
							return;
						}
					}
					

					Long userID = Long.valueOf(info[2]);
					String id = info[3];
					
					LongPair outkey = new LongPair();
					outkey.set(userID, 2);
					context.write(outkey, new Text( id));

				}
			}
				
		}
	}

	public static class PlayRateReducer extends Reducer<LongPair, Text, Text, Text> {
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataOutputStream fsoutput = fs.create(new Path("music_recommend/mainpage_log/logs/playreducer.row"));
			writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(fsoutput, "utf-8")));
		}

		PrintWriter writer;

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			writer.close();
		}
		
		@Override
		public void reduce(LongPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			long filetype = -1;
			Iterator<Text> it = values.iterator();
			HashMap<String, String> id_alg_map = new HashMap<String, String>();
			HashMap<String, Integer> click_alg_num  = new HashMap<String, Integer>();
			HashMap<String, Integer> source_map = new HashMap<String, Integer>();
			
			String source = "";
			while (it.hasNext()) {
				String info = it.next().toString();
				filetype = key.getSecond();
				if (filetype == 1) {
					String[] infos = info.split("\t", -1);
					String id = infos[0];
					String alg = infos[1];
					if (!id_alg_map.containsKey(id)) {
						id_alg_map.put(id, alg);
					}
					
//					if (!alg.equals("featured")) 
//						alg = "alg";
					if (click_alg_num.containsKey(alg)) {
						int num = click_alg_num.get(alg);
						click_alg_num.put(alg, num + 1);
					} else {
						click_alg_num.put(alg, 1);
					}
				}
			
				else if (filetype == 2) {

					
					String id = info;


					if (id_alg_map.containsKey(id)) {
						String alg = id_alg_map.get(id);
						
						if (alg.equals("featured")) {
							source = "featured";    //首页运营歌单
							if (source_map.containsKey(source)) {
								int num = source_map.get(source);
								source_map.put(source, num + 1);
							} else {
								source_map.put(source, 1);
							}
						} 
//						else if(alg.equals("hot_server") )
//						{
//							
//							source = source + "_hot_server";    //首页运营歌单
//							if (source_map.containsKey(source)) {
//								int num = source_map.get(source);
//								source_map.put(source, num + 1);
//							} else {
//								source_map.put(source, 1);
//							}
//						
//						}
						else {
							//source = "alg";    //首页算法歌单
							source = alg;
							if (source_map.containsKey(source)) {
								int num = source_map.get(source);
								source_map.put(source, num + 1);
							} else {
								source_map.put(source, 1);
							}
						}
					}
				
					
					/*else if (source.compareTo("userfm") == 0) {   //私人FM
						if (source_map.containsKey(source)) {
							int num = source_map.get(source);
							source_map.put(source, num + 1);
						} else {
							source_map.put(source, 1);
						}
					}*/

					

				}
				
			}
		
			
			Iterator<Entry<String, Integer>> iter = click_alg_num.entrySet().iterator();
			StringBuffer buffer = new StringBuffer();
			while (iter.hasNext()) {
				Entry<String, Integer> entry = iter.next();
				String alg = entry.getKey();
				int clicknum = entry.getValue();

				Integer subnum = source_map.get(alg);
				if(subnum == null)
					subnum = 0;
				buffer.append(alg).append(":").append(subnum).append(":").append(clicknum).append(",");

			}
			if (buffer.length() > 1) {
				context.write(new Text(String.valueOf(key.getFirst())), new Text(buffer.substring(0, buffer.length()-1)));
			}

		}
	}

	public static class TotalMapper extends Mapper<WritableComparable<?>, Text, Text, Text> {

		@Override
		public void map(WritableComparable<?> key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] info = value.toString().split("\t", -1);
			context.write(new Text("1"), new Text(info[1] ));
			//context.write(new Text(info[1]), new Text(info[2] + "\t" + info[3]));
		}

	}

	public static class TotalReducer extends Reducer<Text, Text, Text, NullWritable> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			HashMap<String, Integer> sub_map = new HashMap<String, Integer>();
			HashMap<String, Integer> click_map = new HashMap<String, Integer>();
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
				String value = it.next().toString();


				String[] array = value.split(",", -1);

				for(String soureinfo : array) {
					String[] sourcearray = soureinfo.split(":", -1);
					String alg = sourcearray[0];

					if (sub_map.containsKey(alg)) {
						int num = sub_map.get(alg);
						sub_map.put(alg, num + Integer.valueOf(sourcearray[1]));
					} else {
						sub_map.put(alg, Integer.valueOf(sourcearray[1]));
					}

					if (click_map.containsKey(alg)) {
						int num = click_map.get(alg);
						click_map.put(alg, num + Integer.valueOf(sourcearray[2]));
					} else {
						click_map.put(alg, Integer.valueOf(sourcearray[2]));
					}
				}
			}

			Iterator<Entry<String, Integer>> iter = sub_map.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, Integer> entry = iter.next();
				String alg = entry.getKey();
				
				int subcount = entry.getValue();
				int clickcount = click_map.get(alg);
				float rate = (float) subcount / clickcount;
				//source \t 播放数 \t 总播放数 \t 播放率
//				context.write(new Text(entry.getKey()), new Text(count + "#" + total_paly + "#" + String.valueOf(rate)));
				context.write(new Text(entry.getKey() + "#" + subcount + "#" + clickcount + "#" + String.valueOf(rate)), NullWritable.get());
				
			}
			
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Options options = new Options();
		options.addOption("input", true, "input");
		options.addOption("output", true, "output");
		options.addOption("scene", true, "scene");    //场景
		options.addOption("os", true, "os");     //操作系统
		options.addOption("timeDay", true, "timeDay");  
		options.addOption("abtest", true, "int");

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		String input = cmd.getOptionValue("input");
		String output = cmd.getOptionValue("output");
		String scene = cmd.getOptionValue("scene");
		String os = cmd.getOptionValue("os");
		String timeDay = cmd.getOptionValue("timeDay");
		
		Configuration conf = getConf();
		conf.set("scene", scene);
		conf.set("os", os);
		conf.set("timeDay", timeDay);
		conf.setInt("abtest", Integer.parseInt(cmd.getOptionValue("abtest")));
		
		Job job = new Job(conf, "playrate job");
		job.setJarByClass(PlaylistSubRate2.class);

		job.setMapperClass(PlayRateMapper.class);
		job.setMapOutputKeyClass(LongPair.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(PlayRateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(LongPair.FirstPartitioner.class);
		job.setGroupingComparatorClass(LongPair.FirstGroupingComparator.class);
		job.setSortComparatorClass(LongPair.KeyComparator.class);

		FileInputFormat.addInputPath(job, new Path(input));

		FileOutputFormat.setOutputPath(job, new Path(output + "/stage1"));
		
		if (!job.waitForCompletion(true)) {
			throw new InterruptedException("playrate job failed processing " + args[0]);
		}
		
		Job job2 = new Job(conf, "playrate job");
		job2.setJarByClass(PlaylistSubRate2.class);
		
		job2.setMapperClass(TotalMapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(TotalReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path(output + "/stage1"));
		
		FileOutputFormat.setOutputPath(job2, new Path(output + "/final"));

		return job2.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new PlaylistSubRate2(), args);

	}


}
