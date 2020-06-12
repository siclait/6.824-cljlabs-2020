#!/bin/sh

# set -euxo pipefail

TMP_DIR="mr-tmp"
export TIMBRE_LEVEL=":warn"
# export TIMBRE_LEVEL=":debug"

#
# basic map-reduce test
#

# run the test in a fresh sub-directory.
rm -rf ${TMP_DIR}
mkdir ${TMP_DIR}

failed_any=0

# first word-count

# generate the correct output
clj -m map-reduce.sequential --app=wc resources/books/pg-*.txt
sort ${TMP_DIR}/mr-out-0 > ${TMP_DIR}/mr-correct-wc.txt
rm -f ${TMP_DIR}/mr-out*

echo '***' Starting wc test.

gtimeout -k 2s 180s clj -m map-reduce.master resources/books/pg-*.txt &
sleep 1

# start multiple workers
gtimeout -k 2s 180s clj -m map-reduce.worker --app=wc &
gtimeout -k 2s 180s clj -m map-reduce.worker --app=wc &
gtimeout -k 2s 180s clj -m map-reduce.worker --app=wc &

wait

sort ${TMP_DIR}/mr-out* > ${TMP_DIR}/mr-wc-all
if cmp ${TMP_DIR}/mr-wc-all ${TMP_DIR}/mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and master to exit.
wait ; wait ; wait

# now indexer
rm -f ${TMP_DIR}/mr-*

# generate the correct output
clj -m map-reduce.sequential --app=indexer resources/books/pg-*.txt
sort ${TMP_DIR}/mr-out-0 > ${TMP_DIR}/mr-correct-indexer.txt
rm -f ${TMP_DIR}/mr-out*

echo '***' Starting indexer test.

gtimeout -k 2s 180s clj -m map-reduce.master resources/books/pg-*.txt &
sleep 1

# start multiple workers
gtimeout -k 2s 180s clj -m map-reduce.worker --app=indexer &
clj -m map-reduce.worker --app=indexer

sort ${TMP_DIR}/mr-out* > ${TMP_DIR}/mr-indexer-all
if cmp ${TMP_DIR}/mr-indexer-all ${TMP_DIR}/mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait ; wait


echo '***' Starting map parallelism test.

rm -f ${TMP_DIR}/mr-out* ${TMP_DIR}/mr-worker*

gtimeout -k 2s 180s clj -m map-reduce.master resources/books/pg-*.txt &
sleep 1

gtimeout -k 2s 180s clj -m map-reduce.worker --app=mtiming &
clj -m map-reduce.worker --app=mtiming

NT=`cat ${TMP_DIR}/mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat ${TMP_DIR}/mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait ; wait


echo '***' Starting reduce parallelism test.

rm -f ${TMP_DIR}/mr-out* ${TMP_DIR}/mr-worker*

gtimeout -k 2s 180s clj -m map-reduce.master resources/books/pg-*.txt &
sleep 1

gtimeout -k 2s 180s clj -m map-reduce.worker --app=rtiming &
clj -m map-reduce.worker --app=rtiming

NT=`cat ${TMP_DIR}/mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait ; wait


# # generate the correct output
clj -m map-reduce.sequential --app=nocrash resources/books/pg-*.txt
sort ${TMP_DIR}/mr-out-0 > ${TMP_DIR}/mr-correct-crash.txt
rm -f ${TMP_DIR}/mr-out*

echo '***' Starting crash test.

rm -f ${TMP_DIR}/mr-done
( gtimeout -k 2s 180s clj -m map-reduce.master resources/books/pg-*.txt ; touch ${TMP_DIR}/mr-done ) &
sleep 1

# start multiple workers
gtimeout -k 2s 180s clj -m map-reduce.worker --app=crash &

touch ${TMP_DIR}/mr-socket

( while [ -e ${TMP_DIR}/mr-socket -a ! -f ${TMP_DIR}/mr-done ]
  do
      gtimeout -k 2s 180s clj -m map-reduce.worker --app=crash
      sleep 1
  done ) &

( while [ -e ${TMP_DIR}/mr-socket -a ! -f ${TMP_DIR}/mr-done ]
  do
      gtimeout -k 2s 180s clj -m map-reduce.worker --app=crash
      sleep 1
  done ) &

while [ -e ${TMP_DIR}/mr-socket -a ! -f ${TMP_DIR}/mr-done ]
do
    clj -m map-reduce.worker --app=crash
    sleep 1
done

wait
wait
wait

rm ${TMP_DIR}/mr-socket
sort ${TMP_DIR}/mr-out* > ${TMP_DIR}/mr-crash-all
if cmp ${TMP_DIR}/mr-crash-all ${TMP_DIR}/mr-correct-crash.txt
then
 echo '---' crash test: PASS
else
 echo '---' crash output is not the same as mr-correct-crash.txt
 echo '---' crash test: FAIL
 failed_any=1
fi

if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
