
# Simple build script for opentsdb
#
# Skips compiling against gnuplot for standalone storage-only server.
# Creates a minimal tarball


cd $(dirname $(readlink -f $0))
./bootstrap
env ac_cv_path_GNUPLOT=`which true` GNUPLOT=`which true` ./build.sh
ver="open$(basename -s .jar build/tsdb-*.jar)"
dest="tmp/$ver/"
tarball="$ver.tar.gz"

rm -rf $dest build/$tarball
mkdir -p $dest/lib $dest/staticroot $dest/tmp $dest/logs
cp -a build/tsdb* $dest
chmod 755 $dest/tsdb

# copy jars except those used by unit tests or ui (gwt)
find build/third_party -type f -name '*.jar' | egrep -v 'hamcrest|junit|javassist|mockito|powermock|gwt' | xargs cp -a --target-directory $dest/lib/

cp -a bixby/start_tsdb.sh.example $dest/
cp -a tools $dest
cp -a src/create_table.sh $dest/tools/
cp -a bixby/logback.xml $dest/lib/

# some fixes for expected files/paths
cp -a src/mygnuplot.sh $dest/lib/
echo "UI not available" > $dest/staticroot/index.html

# version info
git log -1 > $dest/VERSION

cd tmp/
tar -czf ../build/$tarball $ver/

cd ..
rm -rf tmp/

echo
echo "created: build/$tarball"

