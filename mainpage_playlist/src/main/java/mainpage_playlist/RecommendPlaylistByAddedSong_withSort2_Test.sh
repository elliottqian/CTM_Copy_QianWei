#! /bin/bash
source /home/ndir/.bash_profile

cd /data/music_useraction/mainpage_playlist

#./playlist_quality.sh

# 修改
maxImpress=3
daynum=`date +%e`
daymod=$(($daynum%3))
if [ $daymod -eq 1 ]
then
  maxImpress=4
else
  maxImpress=2
fi

#user sub profile
userSubprofile=/user/ndir/music_recommend/recommend_MusicByUser/user_sub_profile
/data/music_useraction/wait_hdfs_file.sh ${userSubprofile}/_SUCCESS
/data/music_useraction/wait_hdfs_file.sh ${userSubprofile}/_mergeOK

# 下面注释掉的那句的输出 output=/user/ndir/music_recommend/mainpage_playlist/user_sub_profile_merge_forPlaylist
# ./data/music_useraction/mainpage_playlist/user_sub_profile_merge_forPlaylist.sh

now_hour=`date +%H`
if [ ${now_hour} -le 1 ]
then
    echo "job end next day"
    exit 1
fi

# 下面注释掉的那句的输出 output=/user/ndir/music_recommend/mainpage_playlist/resort_songPlaylist
# ./data/music_useraction/mainpage_playlist/resort_songPlaylist.sh


#mainpage impress
impress_path=/user/ndir/music_recommend/mainpage/mainpage_impress
for (( i=1;i<=60;i++ ))
do
  date_str=`date -d "$i day ago" +%Y-%m-%d`
  path=$impress_path/$date_str
  #hadoop fs -test -e $path
  #if [ $? -eq 0 ]
  #then
      mainpage_impress=$mainpage_impress" -mainpage_impress "$path
  #fi
done
echo $mainpage_impress

cur_date=`date +%Y-%m-%d`
while(true)
do
  tag=`hadoop fs -ls /user/ndir/music_recommend/metadata/playlist_low_quality/00000*|grep "$cur_date"`
  echo tag=$tag
  #hadoop fs -ls /user/ndir/music_recommend/recommendSonglistBySong/recommendSonglistBySong/final/_SUCCESS > recommendSonglistBySong.tmp
  #tag2=`cat recommendSonglistBySong.tmp | grep "$cur_date"`
  #echo tag2=$tag2

  #tag3=`hadoop fs -ls music_recommend/conversion_model/playlistFeature/_SUCCESS|grep "$cur_date"`
  #echo tag3=$tag3
  if [ -z "$tag" ]
  then
    sleep 30
  else
     break
  fi
done

recommend_output='test/qianwei/recommend_list_by_song'
hadoop fs -rmr $recommend_output

hadoop jar /data/qianwei/jar/mainpage_playlist-1.0-SNAPSHOT.jar mainpage_playlist.RecommendPlaylistByAddedSong_withSort2 \
    -Dmapreduce.reduce.java.opts="-Xmx8000M  -XX:+UseSerialGC" \
    -Dmapreduce.reduce.memory.mb=9000 \
    -Dmapred.job.queue.name=ndir.sla \
    -Dmapred.reduce.tasks=360  \
    -Dmapreduce.task.timeout=7500000  \
    -libjars  /data/music_useraction/json_simple-1.1.jar \
    -user_sub_profile music_recommend/mainpage_playlist/user_sub_profile_merge_forPlaylist  \
    -visited  /user/ndir/music_recommend/user_action/list  \
    -output $recommend_output \
    -songinfo /user/ndir/music_recommend/metadata/id_name_artistId_artists.dump \
    -maxImpress $maxImpress \
    -similarity music_recommend/mainpage_playlist/resort_songPlaylist/final \
    -listQuality /user/ndir/music_recommend/mainpage_playlist/quality2/final \
    -day 1 \
    -userActiveTime /user/ndir/music_recommend/user_latest_active \
    -coverStatus /db_dump/music_ndir/Music_Playlist_BookedCount  \
    $mainpage_impress  \
    -userFeature music_recommend/conversion_model/userFeature \
    -playlistFeature music_recommend/conversion_model/playlistFeature \
    -LRweights music_recommend/mainpage_playlist/playlist_LRWeight_P_itembased_tag \
    -playlistSubRatio_Promoted music_recommend/mainpage_playlist/playlist_rec_subrate_Promoted/final \
    -LRweights_ab music_recommend/mainpage_playlist/playlist_LRWeight_P_itembased_tag \
    -user_tag_new /user/ndir/music_recommend/user_tag_new/ \
    -playlistAreaTag music_recommend/mainpage_playlist/playlistAreaTag2/final \
    -user_XinJiang music_recommend/mainpage_playlist/getUser_XinJiang_test \
    -arabicPlaylist music_recommend/metadata/playlist_low_quality/ \
    -playlist_member_info_path '/user/ndir/music_recommend/mainpage_playlist/qianwei/listMoneyMemberClickRatioLast6M/2017-08-15/part-00000' \
    -divisor '3' \
    -remainder '0'


