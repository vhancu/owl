#!/usr/bin/env ruby


require 'find'

require 'optparse'
require 'ostruct'

module Enumerable
    def sum
        inject &:+
    end

    def vector_add(*others)
        zip(*others).collect &:sum
    end
end

# Return a  structure describing the options.
def parse(args)
    # The options specified on the command line will be collected in *options*.
    # We set default values here.
    options = OpenStruct.new
    options.directory = []
    options.database = []

    options.load = false
    options.show = false
    options.verbose = false

    opt_parser = OptionParser.new do |opts|
        opts.banner = "Usage: cl.rb [options]"

        opts.separator ""
        opts.separator "Specific options:"

        # Mandatory argument.
        opts.on("-d", "--dir DIRECTORY", "Read logs from given DIRECTORY ") do |dir|
            options.directory << dir
        end

        opts.on("-D", "--D SQLITE", "Read data from a SQLITE database") do |db|
            options.database << db
        end

        # Boolean switches
        opts.on("-l", " --load ", "load data from DIRECTORY into SQLITE database") do |l|
            options.load = l
        end

        opts.on("-s", " --show", "show statistics") do |s|
            options.show = s
        end

        opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
            options.verbose = v
        end

        opts.separator ""
        opts.separator "Common options:"

        # No argument, shows at tail.  This will print an options summary.
        opts.on_tail("-h", "--help", "Show this message") do
            puts opts
            exit
        end

        # Another typical switch to print the version.
        opts.on_tail("--version", "Show version") do
            raise NotImplementedError
            # puts ::Version.join('.')
            # exit
        end
    end

    opt_parser.parse!(args)
    options
end  # parse()


options = parse(ARGV)
puts options



### HELPER classes ###

class Batch
    #to prevent concurency
    @@locked = false


    def initialize(path, db)
        @path=path
        @db=db
    end

    def getFiles()
        #return Dir.glob("#{@path}/**/*")
        results = []
        Find.find(@path) do |elem|
            name = File.basename(elem)
            if FileTest.directory?(elem)
                next
            else
                results << elem if name
            end
        end
        return results
    end

    def getLines(filename)
        counter = 1
        file = File.new(filename, "r")
        while (line = file.gets)
            puts "#{counter}: #{line}"
            counter = counter + 1
        end
        file.close
    end



    def getAllLines(filename)
        return File.read(filename).split("\n")
    end

end


regex_string = %r{^
    ([^\s\"\[\]]+)\s    #(g0)  Bucket Owner
    ([^\s\"\[\]]+)\s       #(g1)  Bucket
    (\[[^\]\[]+\])\s       #(g2)  Time
    ([^\s\"\[\]]+)\s       #(g3)  Remote IP
    ([^\s\"\[\]]+)\s       #(g4)  Requester
    ([^\s\"\[\]]+)\s       #(g5)  Request ID
    ([^\s\"\[\]]+)\s       #(g6)  Operation
    ([^\s\"\[\]]+)\s       #(g7)  Key
    (\"[^\"]+\")\s         #(g8)  Request-URI
    (\d+)\s                #(g9)  HTTP status
    ([^\s\"\[\]]+)\s       #(g0)  Error Code
    ([^\s\"\[\]]+)\s       #(g11) Bytes Sent
    ([^\s\"\[\]]+)\s       #(g12) Object Size
    ([^\s\"\[\]]+)\s       #(g13) Total Time
    ([^\s\"\[\]]+)\s       #(g14) Turn-Around Time
    (\"[^\"]+\")\s         #(g15) Referrer
    (\"[^\"]+\")\s         #(g16) User-Agent
    ([^\s\"\[\]]+)         #(g17) Version Id
    }x


colnames = {:bucket_owner => 'Bucket Owner',
    :bucket => 'Bucket',
    :time => 'Time',
    :remote_ip => 'Remote IP',
    :requester => 'Requester',
    :request_id => 'Request ID',
    :operation => 'Operation',
    :key => 'Key',
    :request_uri => 'Request-URI',
    :http_status => 'HTTP Status',
    :error_code => 'Error Code',
    :bytes_sent => 'Bytes Sent',
    :object_size => 'Object Size',
    :total_time => 'Total Time',
    :turn_around_time => 'Turn-Around Time',
    :referrer => 'Referrer',
    :user_agent => 'User-Agent',
    :version_id => 'Version Id' }


require "sqlite3"

if options.load
    # Open databases
    db = SQLite3::Database.new "s3data.db"

    rawdb = SQLite3::Database.new "s3raw.db"

    # Create tables
    rows = rawdb.execute <<-SQL
    CREATE TABLE IF NOT EXISTS  s3data (
        bucket_owner  text,
        bucket  text,
        time  text,
        remote_ip  text,
        requester  text,
        request_id  text,
        operation  text,
        key  text,
        request_uri  text,
        http_status  text,
        error_code  text,
        bytes_sent  text,
        object_size  text,
        total_time  text,
        turn_around_time  text,
        referrer  text,
        user_agent  text,
        version_id text)
    SQL

    # month precision # day precision
    rows = db.execute <<-SQL
    CREATE TABLE IF NOT EXISTS  task_hour_cnt (
        year  text,
        month  text,
        day  text,
        hour  text,
        count  text,
        bytes_sent  text,
        object_size  text,
        total_time  text,
        turn_around_time  text)
    SQL


    rows = db.execute <<-SQL
    CREATE TABLE IF NOT EXISTS  task_geoip (
        year  text,
        month  text,
        day  text,
        remote_ip  text,
        count  text,
        bytes_sent  text,
        object_size  text,
        total_time  text,
        turn_around_time  text)
    SQL
end


## process logs

data = []
lines = []
instance = Batch.new('data/s3logs','')
files = instance.getFiles


files.each do |file|
    lines = instance.getAllLines(file)
    lines.each do |line|
        # data.push('/explor/'.match(line).class)
        begin
            row = regex_string.match(line)[1..-1]
            if options.load
                rawdb.execute "insert into s3data values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )", row
            end
            data.push(row)
        rescue
            puts "error:" + line
        end
        # puts "+++" + line[0..10] + "+++"
        # puts regex_string.match(line)
    end

end

puts "-" * 80
puts data.length
puts "-" * 80
#puts data[0]

require 'date'

require 'browser'



require 'geoip'

gip = GeoIP.new('data/geoip/GeoIP.dat')


times = []
basics        = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
os            = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
browsers      = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
bot           = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
search_engine = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
known         = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
mobile        = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
tablet        = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
console       = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
cnames        = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
countries     = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
ip            = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
referrer      = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}


data.each do |h|
    times.push(DateTime.strptime(h[2][1...-1] , '%d/%b/%Y:%H:%M:%S %z')) 
    dt = DateTime.strptime(h[2][1...-1] , '%d/%b/%Y:%H:%M:%S %z')
    kdt = DateTime.new(dt.year, dt.month, dt.day, dt.hour)

    bytes_sent = h[11].to_i
    object_size = h[12].to_i
    total_time = h[13].to_i
    turn_around_time = h[14].to_i
    vect = [1, bytes_sent, object_size,total_time, turn_around_time]

    basics[kdt] = basics[kdt].vector_add(vect)

    browser = Browser.new(:ua => h[16], :accept_language => "en-us")

    idx = [browser.name,browser.version]
    browsers[idx] = browsers[idx].vector_add(vect)

    os[browser.platform] = os[browser.platform].vector_add(vect)

    bot[browser.bot?] = bot[browser.bot?].vector_add(vect)

    search_engine[browser.search_engine?] = search_engine[browser.search_engine?].vector_add(vect)

    known[browser.known?] = known[browser.known?].vector_add(vect)

    mobile[browser.mobile?] = mobile[browser.mobile?].vector_add(vect)

    tablet[browser.tablet?] = tablet[browser.tablet?].vector_add(vect)

    console[browser.console?] = console[browser.console?].vector_add(vect)

    ip[h[3]] = ip[h[3]].vector_add(vect)

    idx = [gip.country(h[3]).to_hash[:continent_code], gip.country(h[3]).to_hash[:country_name]]
    cnames[idx] = cnames[idx].vector_add(vect)

    idx = gip.country(h[3]).to_hash[:country_code2]
    countries[idx] = countries[idx].vector_add(vect)

    referrer[h[15]] = referrer[h[15]].vector_add(vect)
end


months = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
daymonth = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
daysweek = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
hours = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }

# sorting by datetime for basics
basics.keys.sort.each do |h|
    # puts h.month
    months[h.month] = months[h.month].vector_add(basics[h])

    daymonth[[h.month, h.day]] = daymonth[[h.month, h.day]].vector_add(basics[h])

    daysweek[h.wday] = daysweek[h.wday].vector_add(basics[h])

    hours[h.hour] = hours[h.hour].vector_add(basics[h])

end


def showTable (title, colname, data)
    puts
    puts title.upcase.center(92)

    puts "-" * 92
    print colname[0].center(16)
    print "Count".ljust(12)
    print "Bytes".ljust(16)
    print "Object".ljust(16)
    print "Total".ljust(16)
    print "Turn Around".ljust(16)
    print "\n"

    print colname[1].center(16)
    print "".ljust(12)
    print "Sent".ljust(16)
    print "Size".ljust(16)
    print "Time".ljust(16)
    print "Time".ljust(16)
    print "\n"
    puts "-" * 92

    data.sort.map do |h,k|
        print h.to_s.center(16)
        print k[0].to_s.ljust(12)
        print k[1].to_s.ljust(16)
        print k[2].to_s.ljust(16)
        print k[3].to_s.ljust(16)
        print k[4].to_s.ljust(16)
        print "\n"
    end
    puts "-" * 92
    puts
end

if options.show
    puts "-" * 80
    puts "First visit: " + times.min.httpdate.to_s
    puts "Last visit : " + times.max.httpdate.to_s

    showTable "Monthly history", ["Month",""], months

    showTable "Days of month", ["Day of","Month"], daymonth

    showTable "Days of week", ["Hours of", "the Day"], daysweek

    showTable "Hours of the Days", ["Hours of", "the Day"], hours


    puts "-" * 80
    puts "-" * 80
    puts "-" * 80
    puts 'known traffic' + known[true].to_s
    puts 'non known traffic' + known[false].to_s
    puts "-" * 80

    showTable "Operating Systems", ["Operating", "Systems"], os

    showTable "Browsers", ["Browsers", ""], browsers

    showTable "Bots", ["Bot", "Traffic"], { "bot" => bot[true]}
    # puts 'non bot traffic' + bot[false].to_s

    showTable "Search Engines", ["Search Engines", "Traffic"], {"search engine" => search_engine [true]}
    # puts 'non search_engine traffic' + search_engine[false].to_s

    showTable "Mobiles", ["Mobile", "Traffic"], {"mobile" => mobile[true]}
    # puts 'non mobile traffic' + mobile[false].to_s

    showTable "Tablets", ["Tablet", "Traffic"], {"tablet" => tablet[true]}
    # puts 'non tablet traffic' + tablet[false].to_s

    showTable "Consoles", ["Console", "Traffic"], {"console" => console[true]}
    # puts 'non console traffic' + console[false].to_s

    showTable "IP", ["IP", "Traffic"], ip

    showTable "Countries", ["Region/Country", "Traffic"], cnames
    #cnames.keys.sort.each { |h| puts " " + h.to_s  + ":" + cnames[h].to_s }

    showTable "Countries", ["Country", "Traffic"], countries
    #countries.keys.sort.each { |h| puts " " + h.to_s  + ":" + countries[h].to_s }

    showTable "Referrer", ["Referrer", "Traffic"], referrer
    #referrer.keys.sort.each { |h| puts " " + h.to_s  + ":" + referrer[h].to_s }


    puts "-" * 30 + "HTTP CODES".center(20,' ') + "-" * 30
    result = Hash.new(0)
    data.each { |h| result[h[9]] += 1 }
    result.each { |h,k| puts " " + h.to_s  + ":" + k.to_s }
    #showTable "http codes", ["HTTP Codes", "Traffic"], result

    result = Hash.new  {|h,k| h[k] = [] }
    data.each { |h| result[h[9]].push([h[8],h[15]]) }
    # result["404"].each { |f| puts f[0].ljust(60,' ')  + ":" + f[1] }
    # puts "-" * 80
    puts "-" * 30 + "404 Error code".center(20,' ') + "-" * 30
    result2 = Hash.new(0)
    result["404"].each { |a| result2[a] += 1 }
    result2.each { |h,k| puts h[0][1...-1][0..50].ljust(55, ' ') + k.to_s + ' '*3 + h[1][1...-1][0..50] }

=begin
=end
end
