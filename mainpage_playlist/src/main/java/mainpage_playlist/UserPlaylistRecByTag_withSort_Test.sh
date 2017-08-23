#!/bin/bash

source /home/ndir/.bash_profile

maxImpress=3
daynum=`date +%e`
daymod=$(($daynum%3))
if [ $daymod -eq 1 ]
then
  maxImpress=4
else
  maxImpress=2
fi

#history
yesterday=`date -d "-1 day"  +%Y-%m-%d`
historyRec=/user/ndir/music_recommend/mainpage_playlist/tagbased_rec_history/${yesterday}
hadoop fs -test -e $historyRec
if [ $? -ne 0 ]
then
   historyRec="null"
fi

#impress
impress_path=/user/ndir/music_recommend/mainpage/mainpage_impress
for (( i=2;i<=90;i++ ))
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

#exit 0
#listQuality=/user/ndir/music_recommend/mainpage_playlist/quality/final
#tag2playlist=/user/ndir/music_recommend/mainpage_playlist/tagToPlaylist
#output=/user/ndir/music_recommend/mainpage_playlist/user_playlist_tagbased_rec

listQuality=/user/ndir/music_recommend/mainpage_playlist/quality/final
tag2playlist=/user/ndir/music_recommend/mainpage_playlist/tagToPlaylist2
output='test/qianwei/mainPageList/UserPlaylistRecByTagWithSort'


# 等待前面的文件好了才开始执行下面的脚本
/data/music_useraction/wait_hdfs_file.sh /user/ndir/music_recommend/user_action/list/_SUCCESS
/data/music_useraction/wait_hdfs_file.sh /user/ndir/music_recommend/user_tag_new/_SUCCESS
/data/music_useraction/wait_hdfs_file.sh music_recommend/mainpage_playlist/playlistAreaTag2/final/_SUCCESS

echo ${output}
# output=music_recommend/mainpage_playlist/recByTag_Rescore_ml
echo 'location:/data/music_useraction/mainpage_playlist/UserPlaylistRecByTag_withSort_Test.sh'
hadoop fs -rmr $output


# 增加4个 用户标签, 歌单标签, abtest的除数, 余数,  修改1个 LRweights

hadoop jar /data/qianwei/jar/mainpage_playlist-1.0-SNAPSHOT.jar  mainpage_playlist.UserPlaylistRecByTag_withSort  \
    -Dmapreduce.task.timeout=6000000  \
    -Dmapreduce.reduce.java.opts="-Xmx15000M  -XX:+UseSerialGC" \
    -Dmapreduce.reduce.memory.mb=9000 \
    -Dmapred.job.queue.name=ndir.sla \
    -Dmapred.reduce.tasks=200 \
    -libjars /data/music_useraction/json_simple-1.1.jar  \
    -userTag music_recommend/tagUGC/usertag/final \
    -tag2playlist ${tag2playlist} \
    -visited /user/ndir/music_recommend/user_action/list \
    -output ${output} \
    -maxRecommendation 64 \
    -badPlaylist /user/ndir/music_recommend/metadata/playlist_low_quality \
    -maxImpress $maxImpress   \
    -listProfile /user/ndir/music_recommend/playlist_profile/pubtime \
    -portrayMetadata /user/ndir/music_recommend/data_analyze/metadata/user_portray.metadata  \
    -userPortrayPath /user/ndir/music_recommend/data_analyze/user_basic_portray \
    -listQuality $listQuality  \
    -day 2 \
    -userActiveTime /user/ndir/music_recommend/user_latest_active \
    -badPlaylist2 /user/ndir/music_recommend/metadata/bad_playlist  \
    -recSubRatio_default 0.06 $mainpage_impress -rec_history $historyRec  \
    -userFeature music_recommend/conversion_model/userFeature \
    -playlistFeature music_recommend/conversion_model/playlistFeature \
    -LRweights 'test/qianwei/tagLRModelWeight/part-00000' \
    -playlistSubRatio_Promoted music_recommend/mainpage_playlist/playlist_rec_subrate_Promoted/final \
    -LRweights_ab music_recommend/mainpage_playlist/playlist_LRWeight_P_bytag \
    -user_tag_new '/user/ndir/music_recommend/user_tag_new/' \
    -playlistAreaTag 'music_recommend/mainpage_playlist/playlistAreaTag2/final' \
    -divisor '6' \
    -remainder '5'
