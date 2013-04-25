
var ja = Packages.tablesaw.addons.java;
var rules = Packages.tablesaw.rules;
var ts = Packages.tablesaw;

print("===============================================");

/*
Version scheme: The first part is the version of opentsdb.  The number after
the underscore is the revision of our code changes.
*/
var version = '1.1.0_2'; //KairosDB beta2 to allow multiple tags
var buildDir = 'tsbuild';
var classDir = buildDir+'/classes';
var jarDir = buildDir+'/jar';
var target = jarDir+'/hbase_datastore-'+version+'.jar';

var classpath = new ja.Classpath(classDir);

new rules.DirectoryRule(buildDir);
var jarDirRule = new rules.DirectoryRule(jarDir);

saw.addSearchPaths(".*\\.java", "src");
saw.addSearchPaths(".*\\.java", "build/src");

var sources = new ts.RegExFileSet("src", ".*\\.java")
		.addExcludeDirs(["client", "static", "graph", "tools", "tsd"]).recurse();
var thirdPartyLibs = new ts.RegExFileSet("build/third_party", ".*\\.jar").recurse();
classpath.addPaths(thirdPartyLibs.getFullFilePaths());
var libs = new ts.RegExFileSet("lib", ".*\\.jar").recurse();
classpath.addPaths(libs.getFullFilePaths());

//------------------------------------------------------------------------------
compileRule = new rules.PatternRule("compile").multiTarget()
		.addSources(sources.getFilePaths())
		.addSource("BuildData.java")
		.setSourcePattern("(.*)\\.java")
		.setTargetPattern(classDir+"/net/opentsdb/$1.class")
		.setMakeAction("doCompile")
		.addDepend(new rules.DirectoryRule(classDir));

def = saw.getDefinition("sun_javac");
def.setMode("debug");
def.add("class_dir", classDir);
def.add("classpath", classpath.toString());

function doCompile(rule)
	{
	var targets = rule.getRebuildTargets().iterator();
	while (targets.hasNext())
		saw.delete(targets.next());

	sources = rule.getRebuildSources();
	//Add source files to definition
	def.add("sourcefile", sources);
	print("Compiling "+sources.size()+" source files.");

	var cmd = def.getCommand();
	saw.exec(cmd, true);
	}

//------------------------------------------------------------------------------
new rules.SimpleRule().addTarget("build/src/BuildData.java")
		.addSource("Makefile.am")
		.setMakeAction("doTsdbBuild");

function doTsdbBuild(rule)
	{
	saw.exec("/bin/bash build.sh", true);
	}


//------------------------------------------------------------------------------
var jarRule = new ja.JarRule("jar", target).setDescription("Build module jar for KairosDB")
		.addDepend(compileRule)
		.addDepend(jarDirRule)
		.addFileSet(new ts.RegExFileSet(classDir, ".*\\.class").recurse());


saw.setDefaultTarget("jar");
