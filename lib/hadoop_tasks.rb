#java -Ddest.repo.dir=/Users/fddayan/ivytest/myrepo -Divy.cache.dir=/Users/fddayan/ivytest/cache -jar ivy-2.2.0.jar -settings src/example/build-a-ivy-repository/settings/ivysettings-basic.xml -dependency commons-lang commons-lang 2.6

JOBS_TASK_PREFIX = "job"


module Hadoop
  def self.hadoop(command,options={})
    options[:safe] ||= false
    puts "Running  hadoop #{command}"
    if options[:safe]
      safe_sh "hadoop #{command}"
    else
      sh "hadoop #{command}"
    end
  end
  
  def self.jar(command,options={})
    hadoop "jar #{command}",options
  end
  
  def self.fs(command,options={})
    hadoop "fs #{command}",options
  end
  
  module Fs 
    def self.rmr(path,options={})
      Hadoop::fs("-rmr #{path}",options)
    end
    
    def self.rm(path,options={})
      HAdoop::fs "-rm #{path}",options
    end
    
    def self.get(from,to,options={})
      Hadoop::fs("-get #{from} #{to}",options)
    end
    
    def self.put(from,to,options={})
      Hadoop::fs("-put #{from} #{to}",options)
    end
    
    def self.exists?(path)
      sh_ok?("hadoop fs -ls #{path}")
    end
  end
  
end

# Available optioons :name,:args,:setup,:loal_work
def job_task(class_name,attrs) 
  attrs[:name] ||= class_name.split(".").last
  attrs[:args] ||= JOB_YAML[attrs[:setup]] || JOB_YAML[attrs[:name]]
  attrs[:local_work] ||= File.join(".","work",attrs[:name])
  
  namespace JOBS_TASK_PREFIX do
    namespace  attrs[:name] do
      run_attrs = {}
      
      run_attrs[:needs] = attrs[:needs] unless attrs[:needs].nil?
      
      task  :run,:options do |t,args|
        attrs[:needs].each do |n|
            Rake::Task[n].invoke(args[:options])
        end unless attrs[:needs].nil?
        
        options = attrs.clone
        options[:args] = args[:options][:args] unless args[:options].nil? || args[:options][:args].nil?
        
        job class_name, :on => options[:on], :args => options[:args] 
      end
  
      desc"Cleans output for #{class_name} "
      task :clean do
        unless attrs[:args].nil? 
          Hadoop::Fs::rmr attrs[:args]["output"], :safe => true
        else
          puts "[WARNING] Job #{attrs[:name]} does not have an output parameter." 
        end
        safe_sh "rm -rf #{attrs[:local_work]}" 
      end
  
      desc "Copy output from HDFS to local disck for #{class_name} "
      task :get do
        sh "mkdir -p " + attrs[:local_work]  unless Dir::exists?(attrs[:local_work])
        Hadoop::Fs::get attrs[:args]['output'], attrs[:local_work] + "/"
      end
    end

    desc "Runs #{class_name}" 
    task attrs[:name],[:options] => ["job:#{attrs[:name]}:clean","job:#{attrs[:name]}:run"] do |t,args|
    end
    end
end

def pipe_task(name,jobs,attrs={})
  attrs[:local_work] ||= File.join(".","work",name.to_s)
  
  namespace :chain do
    namespace name do 
      desc "Clean output for all job for this chain"
      task :clean do 
        1.upto(jobs.size) do |i|
          Hadoop::Fs::rmr File.join(attrs[:hdfs_basedir],"job#{i}"), :safe => true
        end
      end
      
      desc "Copies the output of last MR job to "
      task :get do
        sh "mkdir -p " + attrs[:local_work]  unless Dir::exists?(attrs[:local_work])
        Hadoop::Fs::get File.join(attrs[:hdfs_basedir],'job' + jobs.size.to_s), attrs[:local_work]
      end
    end
    
    desc "Runs a single job from the pipe"
    task :run,:jobid do |t,args|
      abort "Job position in pipe is out of range" if args[:jobid] > jobs.size || args[:jobid] < 1
      
      if args[:jobid] == 1
        pipe_run_job jobs[args[:jobid]],{ "output" => File.join(attrs[:hdfs_basedir],"job#{args[:jobid]}") } 
      else
        input_path = File.join(attrs[:hdfs_basedir],"job#{args[:jobid]-1}")
        output_poth = File.join(attrs[:hdfs_basedir],"job#{args[:jobid]}")
        
        if Hadoop::Fs::exists?(input_path)
          Hadoop
          pipe_run_job jobs[args[:jobid]],{ "input" => input_path ,"output" => output_path }
        else
          abort "Cannot run job. No input data. Run previous job #{args[:jobid]-1}"
        end
      end
    end
    
    desc "Run in a chain jobs " + jobs.join(",")  
    task name do 
      jobs.each do |j|
        job_name  =  j.kind_of?(Hash) ? j.first[0] : j
        Rake::Task["job:#{job_name}"]
      end
      
      attrs[:setup] ||= name
      run_count = 1
      pipe_run_job jobs.slice!(0), {"output" => File.join(attrs[:hdfs_basedir],"job#{run_count}")}

      for job in jobs
        run_count += 1
        pipe_run_job job, {"input" => JobContext.last_job_output,"output" => File.join(attrs[:hdfs_basedir],"job#{run_count}") }
        puts "Last output #{JobContext.last_job_output}"
      end
      
      puts "Done with the pipe, pass it around"
    end
  end
end

def pipe_run_job(job,io_options)
  title "Running #{job}"
  
  job_name  =  job.kind_of?(Hash) ? job.first[0] : job
  job_setup =  job.kind_of?(Hash) ? job.first[1] : job
  
  options = {}
  options[:args] = JOB_YAML[job_setup.to_s].clone
  options[:args].merge!(io_options)
  
  
  # if sh_ok?("hadoop fs -ls #{options[:args]["output"]}")
  if Hadoop::Fs::exists?(options[:args]["output"])
    puts "WARNING: Output for job #{job_name} is already there. Not Runnong, but we continue with chain"
  else
    Rake::Task["job:#{job_name}"].invoke(options) 
  end
  # puts "Last output #{JobContext.last_job_output}"
end



################################################################
#                         UTILS METHODS 
################################################################

class JobContext
  @@update_last_job_output = nil
  
  def self.update_last_job_output(path)
    @@update_last_job_output = path
  end
  
  def self.last_job_output?
    !@@update_last_job_output.nil?
  end
  
  def self.last_job_output
    return @@update_last_job_output
  end
  
end

def safe_sh(*cmd) 
  sh(*cmd) do |ok,status|
    #eating error
  end
end

def sh_ok?(*cmd) 
  sh(*cmd) do |ok,status|
    if ok 
      return true
    else
      return false
    end
  end
end


def job(class_name,options,&block)
  abort "[ERROR] Jar file does not exist: #{options[:on]} " unless File::exists?(options[:on])
  
  command_string = "hadoop jar #{options[:on]} #{class_name} "
  
  args = options[:args] || {}
  
  yield args if block_given?
  
  args["output"] ||= HDSF_WORKING_DIR + "/" + class_name.split(".").last  
  
  JobContext.update_last_job_output(args["output"])
  
  command_string += args.map{|k,v| ["-"+k.to_s,v]}.flatten.join(' ')
  
  puts "=> Running: #{command_string}"
  sh command_string
end

def rsh(session,cmd)
  session.open_channel do |channel|
    channel.on_data do |ch, data|
      puts "[remote] -> #{data}"
    end
    channel.on_extended_data do |c, type, data|
      #$STDERR.print data
      puts "[remote] -> #{data}"
    end
    
    # channel.exec "echo \"#{cmd}\""
    puts "Running remotly #{cmd}"
    channel.exec cmd
  end
end

def title msg
  puts "===> #{msg}"
end