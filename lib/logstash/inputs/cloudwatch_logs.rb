# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "logstash/timestamp"
require "time"
require "stud/interval"
require "aws-sdk"
require "logstash/inputs/cloudwatch_logs/patch"
require "fileutils"

Aws.eager_autoload!

# Stream events from CloudWatch Logs streams.
#
# Specify an individual log group, and this plugin will scan
# all log streams in that group, and pull in any new log events.
#
# Optionally, you may set the `log_group_prefix` parameter to true
# which will scan for all log groups matching the specified prefix
# and ingest all logs available in all of the matching groups.
#
class LogStash::Inputs::CloudWatch_Logs < LogStash::Inputs::Base
  include LogStash::PluginMixins::AwsConfig::V2

  config_name "cloudwatch_logs"

  default :codec, "plain"

  # Log group(s) to use as an input. If `log_group_prefix` is set
  # to `true`, then each member of the array is treated as a prefix
  config :log_group, :validate => :string, :list => true

  # Where to write the since database (keeps track of the date
  # the last handled log stream was updated). The default will write
  # sincedb files to some path matching "$HOME/.sincedb*"
  # Should be a path with filename not just a directory.
  config :sincedb_path, :validate => :string, :default => nil

  # Interval to wait between to check the file list again after a run is finished.
  # Value is in seconds.
  config :interval, :validate => :number, :default => 60

  # Decide if log_group is a prefix or an absolute name
  config :log_group_prefix, :validate => :boolean, :default => false

  # When a new log group is encountered at initial plugin start (not already in
  # sincedb), allow configuration to specify where to begin ingestion on this group.
  # Valid options are: `beginning`, `end`, or an integer, representing number of
  # seconds before now to read back from.
  config :start_position, :default => 'beginning'

  # If set, must be valid integer. Denotes number of seconds to look back in the past for events.
  config :lookback_duration, :validate => :number, :default => nil


  # def register
  public
  def register
    require "digest/md5"
    @logger.debug("[" + @log_group.join(",") + "] Registering cloudwatch_logs input", :log_group => @log_group)
    settings = defined?(LogStash::SETTINGS) ? LogStash::SETTINGS : nil
    @sincedb = {}

    @logger.debug("[" + @log_group.join(",") + "] lookback_duration", :lookback_duration => @lookback_duration)

    check_start_position_validity

    Aws::ConfigService::Client.new(aws_options_hash)
    @cloudwatch = Aws::CloudWatchLogs::Client.new(aws_options_hash)

    # TLK 08/05/2019
    @log_group_md5 = Digest::MD5.hexdigest(@log_group.join(","))
    @logger.debug("[" + @log_group.join(",") + "] Generated log_group_md5 for .sincedb file", :log_group_md5 => @log_group_md5)

    if @sincedb_path.nil?
      if settings
        datapath = File.join(settings.get_value("path.data"), "plugins", "inputs", "cloudwatch_logs")
        # Ensure that the filepath exists before writing, since it's deeply nested.
        FileUtils::mkdir_p datapath

        # TLK 08/05/2019
        @sincedb_path = File.join(datapath, ".sincedb_" + @log_group_md5)

        #@sincedb_path = File.join(datapath, ".sincedb_" + Digest::MD5.hexdigest(@log_group.join(",")))
      end
    end

    # This section is going to be deprecated eventually, as path.data will be
    # the default, not an environment variable (SINCEDB_DIR or HOME)
    if @sincedb_path.nil? # If it is _still_ nil...
      if ENV["SINCEDB_DIR"].nil? && ENV["HOME"].nil?
        @logger.error("No SINCEDB_DIR or HOME environment variable set, I don't know where " \
                      "to keep track of the files I'm watching. Either set " \
                      "HOME or SINCEDB_DIR in your environment, or set sincedb_path in " \
                      "in your Logstash config for the file input with " \
                      "path '#{@path.inspect}'")
        raise
      end

      #pick SINCEDB_DIR if available, otherwise use HOME
      sincedb_dir = ENV["SINCEDB_DIR"] || ENV["HOME"]

      @sincedb_path = File.join(sincedb_dir, ".sincedb_" + Digest::MD5.hexdigest(@log_group.join(",")))

      @logger.info("No sincedb_path set, generating one based on the log_group setting",
                   :sincedb_path => @sincedb_path, :log_group => @log_group)
    end

  end #def register

  public
  def check_start_position_validity
    raise LogStash::ConfigurationError, "No start_position specified!" unless @start_position

    return if @start_position =~ /^(beginning|end)$/
    return if @start_position.is_a? Integer

    raise LogStash::ConfigurationError, "start_position '#{@start_position}' is invalid! Must be `beginning`, `end`, or an integer."
  end # def check_start_position_validity

  # def run
  public
  def run(queue)
    @queue = queue
    @priority = []
    _sincedb_open
    determine_start_position(find_log_groups, @sincedb)

    while !stop?
      begin

        @num_events_processed = 0

        loop_start_time = DateTime.now
        @logger.debug("[" + @log_group.join(",") + "] starting events processing loop", :loop_start_time => loop_start_time.strftime('%T.%3N'))

        # We want to ensure we keep time to milliseconds granularity
        @start_run = loop_start_time.strftime('%Q').to_i
        @logger.debug("[" + @log_group.join(",") + "] setting @start_run time", :start_run => @start_run)

        groups = find_log_groups
        @logger.debug("[" + @log_group.join(",") + "] list of log groups to process #{groups}")

        groups.each do |group|
          @logger.debug("[" + @log_group.join(",") + "] calling process_group on #{group}")
          process_group(group)
        end # groups.each
      rescue Aws::CloudWatchLogs::Errors::ThrottlingException
        @logger.warn("[" + @log_group.join(",") + "] reached rate limit")
      end

      loop_end_time = DateTime.now
      @logger.debug("[" + @log_group.join(",") + "] finished events processing loop", :loop_end_time => loop_end_time.strftime('%T.%3N'))
      total_loop_elapsed_time = ((loop_end_time.strftime('%Q').to_f - loop_start_time.strftime('%Q').to_f) / 1000).round(3)
      @logger.debug("[" + @log_group.join(",") + "] total number of seconds to complete loop", :total_loop_elapsed_time => total_loop_elapsed_time)
      @logger.debug("[" + @log_group.join(",") + "] total processed events during loop", :num_events_processed => @num_events_processed)

      @num_events_processed = 0

      Stud.stoppable_sleep(@interval) { stop? }
    end
  end # def run

  public
  def find_log_groups
    if @log_group_prefix
      @logger.debug("[" + @log_group.join(",") + "] log_group prefix is enabled, searching for log groups")
      groups = []
      next_token = nil
      @log_group.each do |group|
        loop do
          log_groups = @cloudwatch.describe_log_groups(log_group_name_prefix: group, next_token: next_token)
          groups += log_groups.log_groups.map {|n| n.log_group_name}
          next_token = log_groups.next_token
          @logger.debug("[" + @log_group.join(",") + "] found #{log_groups.log_groups.length} log groups matching prefix #{group}")
          break if next_token.nil?
        end
      end
    else
      @logger.debug("[" + @log_group.join(",") + "] log_group_prefix not enabled")
      groups = @log_group
    end
    # Move the most recent groups to the end
    groups.sort{|a,b| priority_of(a) <=> priority_of(b) }
  end # def find_log_groups

  private
  def priority_of(group)
    @priority.index(group) || -1
  end

  public
  def determine_start_position(groups, sincedb)
    groups.each do |group|
      if !sincedb.member?(group)
        case @start_position
          when 'beginning'
            sincedb[group] = 0

          when 'end'
            sincedb[group] = DateTime.now.strftime('%Q')

          else
            sincedb[group] = DateTime.now.strftime('%Q').to_i - (@start_position * 1000)
        end # case @start_position
      end
    end

    @logger.debug("[" + @log_group.join(",") + "] bootstrapping after restart with log group/start position #{sincedb}")

  end # def determine_start_position

  private
  def process_group(group)
    next_token = nil
    loop do
      if !@sincedb.member?(group)
        @sincedb[group] = 0
      end
      params = {
          :log_group_name => group,
          :start_time => @sincedb[group],
          :interleaved => true,
          :next_token => next_token
      }
      @logger.debug("[" + @log_group.join(",") + "] calling filter_log_events with start_time", :start => convert_timestamp_for_display(@sincedb[group]))

      resp = @cloudwatch.filter_log_events(params)

      resp.events.each do |event|
        process_log(event, group)
      end

      # If lookback_duration option is set, we will not use the timestamps of events.
      # Instead, we'll calculate the value to save after we've  processed all events.
      if @lookback_duration.nil?
        @logger.debug("[" + @log_group.join(",") + "] writing next start_time from event timestamp to .sincedb file", :next_start => convert_timestamp_for_display(@sincedb[group]))
        _sincedb_write
      else
        @logger.debug("[" + @log_group.join(",") + "] lookback_duration set, so NOT using event timestamp")
      end

      next_token = resp.next_token

      @logger.debug("[" + @log_group.join(",") + "] finished processing set of events", :next_start => convert_timestamp_for_display(@sincedb[group]))

      if next_token.nil?
        @logger.debug("[" + @log_group.join(",") + "] next_token is nil - we've reached the end of the event set for the current processing period")
      else
        @logger.debug("[" + @log_group.join(",") + "] next_token is not nil - going back to get next set of events...")
      end

      break if next_token.nil?
    end

    if @lookback_duration
      # Calculate value to save into .sincedb based on the the time we started the current processing loop,
      # minus the "lookback" - effectively, we'll revisit a period of time equal to the value
      # of the "lookback_duration" next time we call "filter_log_events". This gives us a sliding window,
      # with overlap between processing loops. The purpose is to ensure we eventually pick up events
      # that are delayed within CloudWatch - e.g. due to CloudWatch's "eventually consistent" read model.
      next_start = @start_run - (@lookback_duration * 1000)
      @logger.debug("[" + @log_group.join(",") + "] calculating next_start. raw values...", :start_run => @start_run, :lookback_duration => @lookback_duration, :next_start => next_start)
      @logger.debug("[" + @log_group.join(",") + "] calculating next_start. converted values...", :start_run => convert_timestamp_for_display(@start_run), :lookback_duration => @lookback_duration, :next_start => convert_timestamp_for_display(next_start))
      @sincedb[group] = next_start
      @logger.debug("[" + @log_group.join(",") + "] saving next start_time value, based on lookback_duration", :next_start => convert_timestamp_for_display(@sincedb[group]))
      _sincedb_write
    end

    @priority.delete(group)
    @priority << group
  end #def process_group

  # def process_log
  private
  def process_log(log, group)

    @codec.decode(log.message.to_str) do |event|
      event.set("@timestamp", parse_time(log.timestamp))
      event.set("[cloudwatch_logs][ingestion_time]", parse_time(log.ingestion_time))
      event.set("[cloudwatch_logs][log_group]", group)
      event.set("[cloudwatch_logs][log_stream]", log.log_stream_name)
      event.set("[cloudwatch_logs][event_id]", log.event_id)
      decorate(event)

      @queue << event

      @logger.debug("[" + @log_group.join(",") + "] placed event on pipeline queue", :event_id => log.event_id, :ingestion_time => convert_timestamp_for_display(log.ingestion_time))

      if @lookback_duration.nil?
        @sincedb[group] = log.timestamp + 1
      end

    @num_events_processed += 1

    end
  end # def process_log

  # def parse_time
  private
  def parse_time(data)
    LogStash::Timestamp.at(data.to_i / 1000, (data.to_i % 1000) * 1000)
  end # def parse_time

  private
  def _sincedb_open
    begin
      File.open(@sincedb_path) do |db|
        @logger.debug? && @logger.debug("[" + @log_group.join(",") + "] _sincedb_open: reading from #{@sincedb_path}")
        db.each do |line|
          group, pos = line.split(" ", 2)
          @logger.debug? && @logger.debug("[" + @log_group.join(",") + "] _sincedb_open: setting #{group} to #{pos.to_i}")
          @sincedb[group] = pos.to_i
        end
      end
    rescue
      #No existing sincedb to load
      @logger.debug? && @logger.debug("[" + @log_group.join(",") + "] _sincedb_open: error: #{@sincedb_path}: #{$!}")
    end
  end # def _sincedb_open

  private
  def _sincedb_write
    begin
      IO.write(@sincedb_path, serialize_sincedb, 0)
    rescue Errno::EACCES
      # probably no file handles free
      # maybe it will work next time
      @logger.debug? && @logger.debug("[" + @log_group.join(",") + "] _sincedb_write: error: #{@sincedb_path}: #{$!}")
    end
  end # def _sincedb_write


  private
  def serialize_sincedb
    @sincedb.map do |group, pos|
      [group, pos].join(" ")
    end.join("\n") + "\n"
  end

  private
  def convert_timestamp_for_display(timestamp)
    formatted_time = Time.strptime(timestamp.to_s,'%Q').strftime('%T.%3N')
  end
end # class LogStash::Inputs::CloudWatch_Logs
