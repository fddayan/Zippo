require 'rake'
require 'rake/testtask'

desc 'Run unit tests.'
task :default => :test

desc 'Tests'
Rake::TestTask.new :test do |t|
  t.pattern = 'test/**/*_test.rb'
  t.verbose = true
end