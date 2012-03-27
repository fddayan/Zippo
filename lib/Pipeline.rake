# TODO 
# JobBriligMonetDBCsv should read IPFilter output files
#

require 'yaml'
require 'net/scp'
require 'net/ssh'
require 'hadoop_tasks'

PROPS_FILE = ENV['envfile'] || 'environments.yml'
JOB_FILE = ENV['jobfile'] || 'job.yml'
PROPS_ENV = ENV["env"] || "default"

YML_FILE = YAML::load(File.open(PROPS_FILE))
JOB_YAML = YAML::load(File.open(JOB_FILE))

abort "Cannot find enviroment '#{PROPS_ENV}' in '#{PROPS_FILE}'" if YML_FILE[PROPS_ENV].nil? # this should fail only when is requeired

PROPS = YML_FILE[PROPS_ENV]

HDSF_WORKING_DIR = PROPS["HDSF_WORKING_DIR"]
INPUT_DIR = PROPS["INPUT_DIR"]
FROM_DATE = PROPS["FROM_DATE"]
TO_DATE = PROPS["TO_DATE"]
MONETDB_HOME = PROPS["MONETDB_HOME"]
MDB_DATABASE = PROPS["MDB_DATABASE"]
SAMPLE_RATE = PROPS["SAMPLE_RATE"]
MONETDB_HOST = PROPS["MONETDB_HOST"]
MONETDB_HOST_USER = PROPS["MONETDB_HOST_USER"]
CAMPIGN_LIST_FILE_ON_DISK = PROPS["CAMPIGN_LIST_FILE_ON_DISK"]
SKU_LIST_FILE_ON_DISK = PROPS["SKU_LIST_FILE_ON_DISK"]
PIDS_LIST_FILE_ON_DISK = PROPS["PIDS_LIST_FILE_ON_DISK"]


OUTPUT_DIR = "#{HDSF_WORKING_DIR}/JobBriligIdAndSkusCsv"
CAMPIGN_LIST_FILE_ON_HDFS = "#{HDSF_WORKING_DIR}/campaign_list.txt"
SKU_LIST_FILE_ON_HDFS = "#{HDSF_WORKING_DIR}/sku_list.txt"
PIDS_LIST_FILE_ON_HDFS = "#{HDSF_WORKING_DIR}/pids.txt"

LOCAL_WORKING_DIR = "./work"
REMOTE_WORKING_DIR = "./briligmonetdb/work"
FILES = "./work/JobBriligIdAndSkusCsv/*.gz"


task :default => [:all]

task :all => ["clean:all","mr:resources","mr:log2csv","monetdb:remote:loadcsv"] do
  puts "Done with the pipe, pass it around."
end

task :status do
  puts "\n"
  puts "CSV MapReduce Output\t" +  (Dir::exists?(File.join(LOCAL_WORKING_DIR,"JobBriligIdAndSkusCsv")) ? "OK" : "Missing")
  puts "File create_table.sql\t" +  (File::exists?(File.join(LOCAL_WORKING_DIR,"create_table.sql")) ? "OK" : "Missing")
  puts "File sql_copy.sh\t" +  (File::exists?(File.join(LOCAL_WORKING_DIR,"sql_copy.sh")) ? "OK" : "Missing")
end


################################################################
#                         MAP REDUCE 
################################################################

job_task "com.brilig.logs.monetdb.JobBriligIdAndSkusCsvFromFilteredEntries" ,:on => "../target/Logs2Monet-1.0-SNAPSHOT.jar",:needs => ["mr:resources"]
job_task "com.brilig.hadoop.IpCookieCountFilterTool", :on => "../target/Logs2Monet-1.0-SNAPSHOT.jar"

pipe_task :bid_skus,[:IpCookieCountFilterTool,:JobBriligIdAndSkusCsvFromFilteredEntries => :JobBriligIdAndSkusCsvFromFilteredEntries2],:hdfs_basedir=>"/user/fddayan/"

namespace :mr do 
  desc "Puts required resources in hdfs."
  task :resources do 
    title "Removing resources from HDFS"
    safe_sh "hadoop fs -rm #{SKU_LIST_FILE_ON_HDFS}"
  	safe_sh "hadoop fs -rm #{CAMPIGN_LIST_FILE_ON_HDFS}"
  	safe_sh "hadoop fs -rm #{PIDS_LIST_FILE_ON_HDFS}"
  	Hadoop::Fs::rm
  	
  	title "Putting resources"
    # sh "hadoop fs -put #{SKU_LIST_FILE_ON_DISK} #{SKU_LIST_FILE_ON_HDFS}"
    # sh "hadoop fs -put #{CAMPIGN_LIST_FILE_ON_DISK} #{CAMPIGN_LIST_FILE_ON_HDFS}"
    # sh "hadoop fs -put #{PIDS_LIST_FILE_ON_DISK} #{PIDS_LIST_FILE_ON_HDFS}"
  	
  	Hadoop::Fs::put SKU_LIST_FILE_ON_DISK, SKU_LIST_FILE_ON_HDFS
  	Hadoop::Fs::put CAMPIGN_LIST_FILE_ON_DISK, CAMPIGN_LIST_FILE_ON_HDFS
  	Hadoop::Fs::put PIDS_LIST_FILE_ON_DISK, PIDS_LIST_FILE_ON_HDFS
  end
end


################################################################
#                         MONETDB
################################################################

namespace :monetdb do 
  
  desc "Gets and creates required files"
  task :files do 
    safe_sh "rm -rf ./work"
    sh "mkdir ./work"
    sh "hadoop fs -get #{OUTPUT_DIR} ./work"
    sh "python create_table.py #{SKU_LIST_FILE_ON_DISK} #{CAMPIGN_LIST_FILE_ON_DISK} #{PIDS_LIST_FILE_ON_DISK} > ./work/create_table.sql"
    Rake::Task["monetdb:sqlcopy"].invoke(FILES)
  end
  
   desc "This creates a .sh file to run on a remote machine and load csv in monetdb"
   task :sqlcopy,:files do |t,args|
      to_file = File.join(LOCAL_WORKING_DIR,"sql_copy.sh")
      File.open(to_file,"w") do |f|
        f.puts "#!/bin/sh"
        Dir.glob(args[:files]).each do |file|
          file = File::absolute_path(file)
          count = `zcat #{file} | wc -l` # this executes the command
          sql_copy = "COPY #{count.chomp.strip} RECORDS INTO brilig_wide FROM '#{file}';"
          f.puts "echo \"#{sql_copy}\" | #{MONETDB_HOME}/bin/mclient #{MDB_DATABASE}"
        end
      end
      puts "Script for loading csv created. Look at #{File::absolute_path(to_file)}"
   end
  
  namespace :local do 
    desc "Loads csv file to monetdb (monetdb must be running) in local monetdb"
    task :loadcsv => ["monetdb:files"] do
      sh "#{MONETDB_HOME}/bin/mclient #{MDB_DATABASE} < ./work/create_table.sql"
      sh "bash #{LOCAL_WORKING_DIR}/sql_copy.sh"
    end
  end
  
  namespace :remote do 
    desc "Loads csv file to monetdb (monetdb must be running) in local monetdb"
    task :loadcsv => ["monetdb:files","monetdb:remote:upload"] do
      Net::SSH.start(MONETDB_HOST,MONETDB_HOST_USER) do |session|
       rsh session,"#{MONETDB_HOME}/bin/mclient #{MDB_DATABASE} < #{REMOTE_WORKING_DIR}/create_table.sql"
       session.loop
       rsh session,"bash #{REMOTE_WORKING_DIR}/sql_copy.sh"
       session.loop
      end
    end
    
    desc "Upload required files to remote monetdb"
    task :upload do
      Net::SSH.start(MONETDB_HOST,MONETDB_HOST_USER) do |session|
        rsh session , "rm -rf #{REMOTE_WORKING_DIR}"
      end
      Net::SCP.start(MONETDB_HOST,MONETDB_HOST_USER) do |scp|
        scp.upload! "./work", REMOTE_WORKING_DIR,:recursive => true
      end
    end
  end
end


################################################################
#                         GENERIC 
################################################################


namespace :clean do 
  desc "Cleans all. Be carefull this deletes files and clean databases."
  task :all => [:db,:mr] do
    
  end
  
  desc "Remove the database."
  task :db do
    title "Dropping databsse"
    safe_sh "#{MONETDB_HOME}/bin/mclient #{MDB_DATABASE} < drop_table.sql"
  end

  desc "Cleans mr output in HDFS and local"  
  task :mr do
    title "Removing files from mr output"
    safe_sh "hadoop fs -rmr #{OUTPUT_DIR}"
    safe_sh "rm -rf ./work"
  end
end

