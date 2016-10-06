#!/usr/bin/env ruby

partman_dir = "../pg_partman"
if !File.directory?(partman_dir)
  puts "ERROR: partman directory does not exist[%s]" % partman_dir
  puts "       cd .. && git clone https://github.com/keithf4/pg_partman.git"
  exit(1)
end

Dir.chdir("../pg_partman") do
  #system("git pull")
end

current_version = "2.2.2"
found = false
new_files = []
Dir.glob("#{partman_dir}/updates/pg_partman*.sql").sort.each do |path|
  if md = File.basename(path).match(/^pg_partman--(\d+\.\d+\.\d+)--(\d+\.\d+\.\d+).sql$/)
    from = md[1]
    to = md[2]
    if from == current_version
      found = true
    end
    new_files << path if found
  else
    raise "Invalid file path[#{path}]"
  end
end

if new_files.empty?
  puts "Nothing to upgrade"
  exit(0)
end

tmp = "/tmp/upgrade-partman.#{Process.pid}.tmp"
new_files.each do |path|
  system("echo \"\n-- START OF FILE: #{File.basename(path)}\" >> #{tmp}")
  system("echo \"\n\" >> #{tmp}")
  system("cat #{path} >> #{tmp}")
end

puts tmp


