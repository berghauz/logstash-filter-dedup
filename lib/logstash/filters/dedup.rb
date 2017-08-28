# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "redis"

# This  filter will replace the contents of the default 
# message field with whatever you specify in the configuration.
#
# It is only intended to be used as an .
class LogStash::Filters::Dedup < LogStash::Filters::Base

  config_name "dedup"

  # The hostname of your Redis server.
  config :host, :validate => :string, :default => "127.0.0.1"

  # The port to connect on.
  config :port, :validate => :number, :default => 6379

  # The Redis database number.
  config :db, :validate => :number, :default => 0

  # The TTL for Redis records.
  config :ttl, :validate => :number, :default => 3600

  # Password to authenticate with. There is no authentication by default.
  config :password, :validate => :password

  # Interval for reconnecting to failed Redis connections
  config :reconnect_interval, :validate => :number, :default => 1
  
  # Array of keys the unique key will build from.
  config :keys, :validate => :array, :required => true
  
  public
  def register
    if @keys.nil? || @keys.count < 2
	@logger.error("Failed to register dedup: keys attribute is empty or less than two elements")
    else
	@redis ||= connect
    end
  end # def register

  public
  def filter(event)
    @redis ||= connect
    redis_key = @keys.collect { |e| event.get(e).to_s }.join('_')
    begin
      if @redis.exists redis_key
	  event.tag("_dupfailure")
      else
	  @redis.set redis_key, Time.now.to_s
	  @redis.expire redis_key, @ttl
      end
    rescue => e
      @logger.warn("Failed to send event to Redis", :event => event, :key => redis_key, :exception => e, :backtrace => e.backtrace)
      sleep @reconnect_interval
      @redis ||= conect
      retry
    end
  end # def filter

  private
  def connect
    @redis_url = "redis://#{@password}@#{@host}:#{@port}/#{@db}"
    Redis.new(:url => @redis_url)
  end # def connect

end # class LogStash::Filters::Dedup
