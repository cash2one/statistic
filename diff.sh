#!/bin/bash
# xulei12@baidu.com 2016.7.13 
# 比较当前git库当前分支上2个版本的差异，并打包成2个压缩包

if [ $# -lt 2 ]
then
    echo "please use:"
    echo "sh ./diff.sh [老版本号] [新版本号]"
    echo "查询提交记录请使用命令："
    echo "git log --abbrev-commit"
    exit -1
fi

echo -e "\033[47;30m重要！\033[0m"
echo -e "\033[47;30m重要！\033[0m"
echo -e "\033[42;31m请确认所有修改都已提交，否则有可能造成修改丢失!\033[0m"
echo "任意键继续, 或者 ctrl+c 退出"
read -n 1

old_version=$1
new_version=$2
echo $old_version
echo $new_version

echo "切换到老版本$old_version……"
git checkout $old_version
git diff $old_version $new_version --name-only | xargs zip old.zip
echo "压缩完毕：old.zip"

echo "切换到新版本$new_version……"
git checkout $new_version
git diff $old_version $new_version --name-only | xargs zip new.zip
echo "压缩完毕：new.zip"
 
echo "完成。传送到本地"

sz -be old.zip new.zip
rm old.zip new.zip
