tsdtmp=/tmp/tsd    # For best performance, make sure
mkdir -p "$tsdtmp"             # your temporary directory uses tmpfs
#export tsd.core.auto_create_metrics=true
./build/tsdb tsd --port=4242 --staticroot=build/staticroot --cachedir="$tsdtmp"
#echo $tsd.core.auto_create_metrics
