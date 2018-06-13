#!/bin/sh
if [ -f /dsp/code/freq_writer/tar.lock ];then
    echo "[`date +%Y-%m-%d-%H:%M:%S`][ERROR] tar frequency_data.txt fail, last job not finish!"
    exit 0
else
    touch /dsp/code/freq_writer/tar.lock
    echo "[`date +%Y-%m-%d-%H:%M:%S`] start tar frequency_data.txt"
fi

#dest_dir="/pdata1/log/nginx/dsp" #test
dest_dir="/dsp/code/nginx/html/dsp"
rm -rfv ${dest_dir}/frequency_data.txt
mv frequency_data.txt ${dest_dir}/frequency_data.txt

cd ${dest_dir}
touch frequency_data.txt.done
tar -zcvf new.frequency_data.txt.tar.gz frequency_data.txt
rm -rf frequency_data.txt.tar.gz
mv new.frequency_data.txt.tar.gz frequency_data.txt.tar.gz
timefile=`stat -c %Y  frequency_data.txt`
md5file1=`md5sum frequency_data.txt`
md5file2=`md5sum frequency_data.txt.tar.gz`
echo "${timefile} ${md5file1} ${md5file2}" >frequency_data.txt.done

rm -rfv /dsp/code/freq_writer/tar.lock
echo "`date +%Y-%m-%d-%H:%M:%S` frequency_data.txt tar end"
