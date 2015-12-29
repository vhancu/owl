#!/usr/bin/env ruby


require 'find'

require 'optparse'
require 'ostruct'

require "sequel"

require 'date'

require 'browser'

require 'geoip'

######### CONFIGURATION #########
raw_database = 'sqlite://s3raw.db'
work_database = 'sqlite://owl.db'

geoip_path = 'data/geoip/GeoIP.dat'
logs_path = 'data/s3logs'

####### END CONFIGURATION #######

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
    options.input = :d
    options.path = []
    options.db = false
    options.load = false
    options.show = false
    options.sopt = []
    options.verbose = false
    options.clean = false

    opt_parser = OptionParser.new do |opts|
        opts.banner = "Usage: cl.rb [options]"

        opts.separator ""
        opts.separator "Data sources:"

        opts.on("-I INPUT", "--input INPUT", [:d, :r, :p],
            "Select input source", "    d -> directory,", 
            "    r -> raw data from database,", "    p -> already processed data)",
            "default d") do |t|
            options.input = t
        end

        opts.on("-p PATH", "--dir PATH", "Read logs from given PATH ") do |dir|
            options.path << dir
        end

        opts.on("-l", "--load", "save results in database") do |l|
            options.load = l
        end

        opts.on("--clean", "delete results data before insert", "(not working for raw data)") do |c|
            options.clean = c
        end

        opts.on("-r", "--raw", "save raw data in database") do |r|
            options.raw = r
        end

        opts.separator ""
        opts.separator "Display:"

        opts.on("-s", "--show", "show statistics") do |s|
            options.show = s
        end

        opts.separator ""
        opts.separator "Filters:"

        # filtering
        opts.on("-y", "--year YEAR", "filter by YEAR") do |y|
            options.year = y.to_i
        end

        opts.on("-m", "--month MONTH", "filter by MONTH 1..12") do |m|
            options.month = m.to_i
        end

        opts.on("--list=[x,y,z]", Array, "limit what to display", "valid options: all, part1,", 
                "   part2, part3, part4, part5, part6") do |so|
            options.sopt = so
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

        opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
            options.verbose = v
        end
    end

    opt_parser.parse!(args)
    options
end  # parse()


options = parse(ARGV)

if options.verbose
    puts options
end


### HELPER classes ###

class Batch
    #to prevent concurency
    @@locked = false


    def initialize(path)
        @path=path
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



if options.raw || options.input == :r
    DBRAW = Sequel.connect(raw_database)

    # create tables
    DBRAW.create_table? :s3raw do # create the table only if it doesn't exists.
        String :bucket_owner
        String :bucket
        String :time
        String :remote_ip
        String :requester
        String :request_id
        String :operation
        String :key
        String :request_uri
        String :http_status
        String :error_code
        String :bytes_sent
        String :object_size
        String :total_time
        String :turn_around_time
        String :referrer
        String :user_agent
        String :version_id
    end

    # create a dataset from the s3raw table
    raw = DBRAW[:s3raw]
end


if options.load || options.input == :p
    DB = Sequel.connect(work_database)

    ### PART1 ###
    DB.create_table? :task_basics do
        Integer :year
        Integer :month
        Integer :day
        Integer :hour
        Integer :count
        Integer :bytes_sent
        Integer :object_size
        Integer :total_time
        Integer :turn_around_time
    end
    tbasics = DB[:task_basics]

    ### PART2 ###
    DB.create_table? :task_browser do
        Integer :year
        Integer :month
        String :browser_name
        String :browser_version
        String :browser_platform
        String :browser_is_bot
        String :browser_is_search_engine
        String :browser_is_known
        String :browser_is_mobile
        String :browser_is_tablet
        String :browser_is_console
        Integer :count
        Integer :bytes_sent
        Integer :object_size
        Integer :total_time
        Integer :turn_around_time
    end
    tbrowser = DB[:task_browser]

    ### PART 3 ###
    DB.create_table? :task_ip_address do
        Integer :year
        Integer :month
        String :ip_address
        Integer :count
        Integer :bytes_sent
        Integer :object_size
        Integer :total_time
        Integer :turn_around_time
    end
    tip_address = DB[:task_ip_address]

    ### PART4 ###
    DB.create_table? :task_countries do
        Integer :year
        Integer :month
        String :region
        String :country_code
        String :country_name
        Integer :count
        Integer :bytes_sent
        Integer :object_size
        Integer :total_time
        Integer :turn_around_time
    end
    tcountries = DB[:task_countries]

    ### PART 5 ###
    DB.create_table? :task_referrer do
        Integer :year
        Integer :month
        String :referrer
        Integer :count
        Integer :bytes_sent
        Integer :object_size
        Integer :total_time
        Integer :turn_around_time
    end
    treferrer = DB[:task_referrer]

    ### PART 6 ###
    DB.create_table? :task_http_codes do
        Integer :year
        Integer :month
        Integer :httpcode
        Integer :count
        Integer :bytes_sent
        Integer :object_size
        Integer :total_time
        Integer :turn_around_time
    end
    thttp_codes = DB[:task_http_codes]

    DB.create_table? :task_http_440 do
        Integer :year
        Integer :month
        Integer :request
        Integer :count
        Integer :bytes_sent
        Integer :object_size
        Integer :total_time
        Integer :turn_around_time
    end
    thttp_440 = DB[:task_http_440]
end


if options.clean
    # TODO: inplement clean logic for RAW data store
    tbasics.where('year = ? and month = ?', options.year, options.month).delete
    tbrowser.where('year = ? and month = ?', options.year, options.month).delete
    tip_address.where('year = ? and month = ?', options.year, options.month).delete
    tcountries.where('year = ? and month = ?', options.year, options.month).delete
    treferrer.where('year = ? and month = ?', options.year, options.month).delete
    thttp_codes.where('year = ? and month = ?', options.year, options.month).delete
    thttp_440.where('year = ? and month = ?', options.year, options.month).delete
end



## process logs

data = []
lines = []



if options.input == :d
    instance = Batch.new(logs_path)
    files = instance.getFiles
elsif options.input == :r
    raw.each do |r|
        line = []
        line << r[:bucket_owner]
        line << r[:bucket]
        line << r[:time]
        line << r[:remote_ip]
        line << r[:requester]
        line << r[:request_id]
        line << r[:operation]
        line << r[:key]
        line << r[:request_uri]
        line << r[:http_status]
        line << r[:error_code]
        line << r[:bytes_sent]
        line << r[:object_size]
        line << r[:total_time]
        line << r[:turn_around_time]
        line << r[:referrer]
        line << r[:user_agent]
        line << r[:version_id]
        data.push(line)
    end
end

if options.input == :d
    files.each do |file|
        lines = instance.getAllLines(file)
        lines.each do |line|
            begin
                row = regex_string.match(line)[1..-1]
                if options.raw
                    raw.insert( :bucket_owner => row[0],
                                :bucket => row[1],
                                :time => row[2],
                                :remote_ip => row[3],
                                :requester => row[4],
                                :request_id => row[5],
                                :operation => row[6],
                                :key => row[7],
                                :request_uri => row[8],
                                :http_status => row[9],
                                :error_code => row[10],
                                :bytes_sent => row[11],
                                :object_size => row[12],
                                :total_time => row[13],
                                :turn_around_time => row[14],
                                :referrer => row[15],
                                :user_agent => row[16],
                                :version_id => row[17] )
                end
                data.push(row)
            rescue
                if options.verbose
                    puts "error:" + line
                end
            end
        end
    end
end

puts "-" * 80
puts data.length
puts "-" * 80
#puts data[0]


gip = GeoIP.new(geoip_path)


times = []
basics        = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
browsers      = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
countries     = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
ip            = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
referrer      = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
http_codes    = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
http_440      = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}


data.each do |h|
    times.push(DateTime.strptime(h[2][1...-1] , '%d/%b/%Y:%H:%M:%S %z'))
    dt = DateTime.strptime(h[2][1...-1] , '%d/%b/%Y:%H:%M:%S %z')
    if (dt.year == options.year and dt.month == options.month)
        bytes_sent = h[11].to_i
        object_size = h[12].to_i
        total_time = h[13].to_i
        turn_around_time = h[14].to_i
        vect = [1, bytes_sent, object_size,total_time, turn_around_time]

        ### PART 1 ###
        idx = [dt.day, dt.hour]
        basics[idx] = basics[idx].vector_add(vect)

        ### PART 2 ###
        browser = Browser.new(:ua => h[16], :accept_language => "en-us")
        idx = [browser.name, browser.version, browser.platform.to_s, browser.bot?, browser.search_engine?, browser.known?, browser.mobile?, browser.tablet?, browser.console?]
        browsers[idx] = browsers[idx].vector_add(vect)

        ### PART 3 ###
        ip[h[3]] = ip[h[3]].vector_add(vect)

        ### PART 4 ###
        idx = [gip.country(h[3]).to_hash[:continent_code], gip.country(h[3]).to_hash[:country_code2], gip.country(h[3]).to_hash[:country_name]]
        countries[idx] = countries[idx].vector_add(vect)

        #idx = [gip.country(h[3]).to_hash[:continent_code], gip.country(h[3]).to_hash[:country_name]]
        #cnames[idx] = cnames[idx].vector_add(vect)

        #idx = gip.country(h[3]).to_hash[:country_code2]
        #countries[idx] = countries[idx].vector_add(vect)

        ### PART 5 ###
        referrer[h[15]] = referrer[h[15]].vector_add(vect)

        ### PART 6 ###
        http_codes[h[9]] = http_codes[h[9]].vector_add(vect)
        if h[9].to_i == 404
            http_440[h[8]] = http_440[h[8]].vector_add(vect)
        end
    end
end

if options.load
    ### PART 1 ###
    cols = [:year, :month, :day, :hour, :count, :bytes_sent, :object_size, :total_time, :turn_around_time]
    data = basics.map {|k,v|  [options.year, options.month, *k, *v] }
    DB[:task_basics].import(cols, data)

    ### PART 2 ###
    cols = [:year, :month, :browser_name, :browser_version, :browser_platform, :browser_is_bot, :browser_is_search_engine, :browser_is_known, :browser_is_mobile, :browser_is_tablet, :browser_is_console, :count, :bytes_sent, :object_size, :total_time, :turn_around_time]
    data = browsers.map {|k,v|  [options.year, options.month, *k, *v] }
    DB[:task_browser].import(cols, data)

    ### PART 3 ###
    cols = [:year, :month, :ip_address, :count, :bytes_sent, :object_size, :total_time, :turn_around_time]
    data = ip.map {|k,v|  [options.year, options.month, k, *v] }
    DB[:task_ip_address].import(cols, data)

    ### PART 4 ###
    cols = [:year, :month, :region, :country_code, :country_name, :count, :bytes_sent, :object_size, :total_time, :turn_around_time]
    data = countries.map {|k,v|  [options.year, options.month, *k, *v] }
    DB[:task_countries].import(cols, data)

    ### PART 5 ###
    cols = [:year, :month, :referrer, :count, :bytes_sent, :object_size, :total_time, :turn_around_time]
    data = referrer.map {|k,v|  [options.year, options.month, k, *v] }
    DB[:task_referrer].import(cols, data)

    ### PART 6 ###
    cols = [:year, :month, :httpcode, :count, :bytes_sent, :object_size, :total_time, :turn_around_time]
    data = http_codes.map {|k,v|  [options.year, options.month, k, *v] }
    DB[:task_http_codes].import(cols, data)

    cols = [:year, :month, :request, :count, :bytes_sent, :object_size, :total_time, :turn_around_time]
    data = http_440.map {|k,v|  [options.year, options.month, k, *v] }
    DB[:task_http_440].import(cols, data)
end

months = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
daymonth = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
daysweek = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
hours = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }


basics.keys.each do |k|

    h = DateTime.new(options.year, options.month, k[0], k[1])

    months[h.month] = months[h.month].vector_add(basics[k])
    daymonth[[h.month, h.day]] = daymonth[[h.month, h.day]].vector_add(basics[k])
    daysweek[h.wday] = daysweek[h.wday].vector_add(basics[k])
    hours[h.hour] = hours[h.hour].vector_add(basics[k])
end



def showTable (title, colname, data, widths=[16,12,16,16,16,16], sortcat:1)
    puts
    length = widths.inject(0, :+)
    puts title.upcase.center(length)

    puts "-" * length
    print colname[0].center(widths[0])
    print "Count".rjust(widths[1])
    print "Bytes".rjust(widths[2])
    print "Object".rjust(widths[3])
    print "Total".rjust(widths[4])
    print "Turn Around".rjust(widths[5])
    print "\n"

    print colname[1].center(widths[0])
    print "".rjust(widths[1])
    print "Sent".rjust(widths[2])
    print "Size".rjust(widths[3])
    print "Time".rjust(widths[4])
    print "Time".rjust(widths[5])
    print "\n"
    puts "-" * length

    if sortcat == 1
        data = data.sort_by{|k, v| [k[0], k[1].to_i]}
    else
        data = data.sort
    end

    data.map do |h,k|
        if h.is_a? String
            print h.to_s[0..widths[0]].ljust(widths[0]+1)
        elsif h.is_a? Array
            print h.join("/")[0..widths[0]].ljust(widths[0]+1)
        else
            print h.to_s[0..widths[0]].ljust(widths[0]+1)
        end
        print k[0].to_s.rjust(widths[1])
        print k[1].to_s.rjust(widths[2])
        print k[2].to_s.rjust(widths[3])
        print k[3].to_s.rjust(widths[4])
        print k[4].to_s.rjust(widths[5])
        print "\n"
    end
    puts "-" * length
    puts
end


def filterData ()
    filtered = Hash.new {|h,k| h[k] = [0,0,0,0,0] }
    data = yield
    keys = data.keys
    keys.map do |k|
        filtered[k] = data[k].inject([0,0,0,0,0]) { |result, element| result.vector_add(element[1]) }
    end
    return filtered
end

if options.show
    puts "-" * 80
    #TODO: correct this part
    #puts "First visit: " + times.min.httpdate.to_s
    #puts "Last visit : " + times.max.httpdate.to_s

    ### PART 1 ###
    if options.sopt.include? 'part1' or options.sopt.include? 'all'
        showTable "Monthly history", ["Month",""], months, sortcat:2

        showTable "Days of month", ["Day of","Month"], daymonth

        showTable "Days of week", ["Days of", "Week"], daysweek, sortcat:2

        showTable "Hours of the Days", ["Hours of", "the Day"], hours, sortcat:2
    end

    ### PART 2 ###
    if options.sopt.include? 'part2' or options.sopt.include? 'all'
        puts "-" * 120

        os = filterData() {browsers.group_by{|k,v| k[2]}}
        showTable "Operating Systems", ["Operating", "Systems"], os

        brw = filterData() {browsers.group_by {|k,v| [k[0],k[1]]}}
        showTable "Browsers", ["Browsers", ""], brw, [22,12,16,16,16,16]

        bot = filterData() {browsers.group_by {|k,v| k[3]}}
        showTable "Bots", ["Bot", "Traffic"], { "bot" => bot[true]}

        search_engine = filterData() {browsers.group_by {|k,v| k[4]}}
        showTable "Search Engines", ["Search Engines", "Traffic"], {"search engine" => search_engine[true]}

        known  = filterData() {browsers.group_by {|k,v| k[5]}}
        #some renaming
        known["known"] = known.delete true
        known["unknown"] = known.delete false
        showTable "Known", ["Known", "Traffic"], known

        mobile = filterData() {browsers.group_by {|k,v| k[6]}}
        showTable "Mobiles", ["Mobile", "Traffic"], {"mobile" => mobile[true]}

        tablet = filterData() {browsers.group_by {|k,v| k[7]}}
        showTable "Tablets", ["Tablet", "Traffic"], {"tablet" => tablet[true]}

        console =  filterData() {browsers.group_by {|k,v| k[8]}}
        showTable "Consoles", ["Console", "Traffic"], {"console" => console[true]}
    end

    if options.sopt.include? 'part3' or options.sopt.include? 'all'
        showTable "IP", ["IP", "Traffic"], ip
    end

    ### PART 4 ###
    if options.sopt.include? 'part4' or options.sopt.include? 'all'
        showTable "Countries", ["Region/Code/Country", "Traffic"], countries, [22,12,16,16,16,16]

        regions =  filterData() {countries.group_by {|k,v| k[0]}}
        showTable "Regions", ["Regions", "Traffic"], regions
    end

    ### PART 5 ###
    if options.sopt.include? 'part5' or options.sopt.include? 'all'
        showTable "Referrer", ["Referrer", "Traffic"], referrer, [50,12,16,16,16,16]
    end

    ### PART 6 ###
    if options.sopt.include? 'part6' or options.sopt.include? 'all'
        showTable "HTTP Codes", ["HTTP Codes", "Traffic"], http_codes

        showTable "440 Error Code", ["404 Error Code", "Traffic"], http_440, [50,12,16,16,16,16]
    end
end
