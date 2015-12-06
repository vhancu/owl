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
    options.directory = []
    options.database = []

    options.load = false
    options.show = false
    options.sopt = []
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

        # filtering
        opts.on("-y", "--year YEAR", "filter by YEAR") do |y|
            options.year = y.to_i
        end

        opts.on("-m", "--month MONTH", "filter by MONTH 1..12") do |m|
            options.month = m.to_i
        end

        # Boolean switches
        opts.on("-l", "--load", "load data from DIRECTORY into SQLITE database") do |l|
            options.load = l
        end

        opts.on("-r", "--raw", "load raw data from DIRECTORY into DBRAW database") do |r|
            options.raw = r
        end

        opts.on("-s", "--show", "show statistics") do |s|
            options.show = s
        end

        opts.on("--list=[x,y,z]", Array) do |so|
            options.sopt = so
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



if options.raw
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

if options.load
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
    #tbasics = DB[:task_basics]

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
    #tbrowswe = DB[:task_browser]

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
    #tip_address = DB[:task_ip_address]

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
    #tcountries = DB[:task_countries]

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
    #treferrer = DB[:task_referrer]

    # TODO: part6
end




## process logs

data = []
lines = []
instance = Batch.new('data/s3logs','')
files = instance.getFiles


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

puts "-" * 80
puts data.length
puts "-" * 80
#puts data[0]


gip = GeoIP.new('data/geoip/GeoIP.dat')


times = []
basics        = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
#os            = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
browsers      = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
#bot           = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
#search_engine = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
#known         = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
#mobile        = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
#tablet        = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
#console       = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
#cnames        = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
countries     = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
ip            = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}
referrer      = Hash.new  {|h,k| h[k] = [0,0,0,0,0]}


data.each do |h|
    times.push(DateTime.strptime(h[2][1...-1] , '%d/%b/%Y:%H:%M:%S %z'))
    dt = DateTime.strptime(h[2][1...-1] , '%d/%b/%Y:%H:%M:%S %z')
    if (dt.year == options.year and dt.month == options.month)
        #kdt = DateTime.new(dt.year, dt.month, dt.day, dt.hour)

        bytes_sent = h[11].to_i
        object_size = h[12].to_i
        total_time = h[13].to_i
        turn_around_time = h[14].to_i
        vect = [1, bytes_sent, object_size,total_time, turn_around_time]

        ### PART 1 ###
        idx = [dt.day, dt.hour]
        basics[idx] = basics[idx].vector_add(vect)
        #basics[kdt] = basics[kdt].vector_add(vect)

        ### PART 2 ###
        browser = Browser.new(:ua => h[16], :accept_language => "en-us")

        idx = [browser.name, browser.version, browser.platform.to_s, browser.bot?, browser.search_engine?, browser.known?, browser.mobile?, browser.tablet?, browser.console?]
        browsers[idx] = browsers[idx].vector_add(vect)
 
        #idx = [browser.name,browser.version]
        #browsers[idx] = browsers[idx].vector_add(vect)
        #os[browser.platform] = os[browser.platform].vector_add(vect)
        #bot[browser.bot?] = bot[browser.bot?].vector_add(vect)
        #search_engine[browser.search_engine?] = search_engine[browser.search_engine?].vector_add(vect)
        #known[browser.known?] = known[browser.known?].vector_add(vect)
        #mobile[browser.mobile?] = mobile[browser.mobile?].vector_add(vect)
        #tablet[browser.tablet?] = tablet[browser.tablet?].vector_add(vect)
        #console[browser.console?] = console[browser.console?].vector_add(vect)

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

        ### TODO: PART 6 ###
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

    ###TODO: PART 6 ###
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

    if options.sopt.include? 'part2' or options.sopt.include? 'all'
        puts "-" * 80
        puts "-" * 80
        puts "-" * 80
        puts 'known traffic' + known[true].to_s
        puts 'non known traffic' + known[false].to_s
        puts "-" * 80

        showTable "Operating Systems", ["Operating", "Systems"], os

        showTable "Browsers", ["Browsers", ""], browsers, [22,12,16,16,16,16]

        showTable "Bots", ["Bot", "Traffic"], { "bot" => bot[true]}

        showTable "Search Engines", ["Search Engines", "Traffic"], {"search engine" => search_engine [true]}

        showTable "Mobiles", ["Mobile", "Traffic"], {"mobile" => mobile[true]}

        showTable "Tablets", ["Tablet", "Traffic"], {"tablet" => tablet[true]}

        showTable "Consoles", ["Console", "Traffic"], {"console" => console[true]}
    end

    if options.sopt.include? 'part3' or options.sopt.include? 'all'
        showTable "IP", ["IP", "Traffic"], ip
    end

    if options.sopt.include? 'part4' or options.sopt.include? 'all'
        showTable "Countries", ["Region/Country", "Traffic"], cnames, [22,12,16,16,16,16]

        showTable "Countries", ["Country", "Traffic"], countries
    end

    if options.sopt.include? 'part5' or options.sopt.include? 'all'
        showTable "Referrer", ["Referrer", "Traffic"], referrer, [50,12,16,16,16,16]
    end

    if options.sopt.include? 'part6' or options.sopt.include? 'all'
        result = Hash.new(0)
        data.each { |h| result[h[9]] += 1 }

        puts
        puts 'HTTP CODES'.upcase.center(92)
        puts "-" * 92
        print "Code".center(12)
        print "Count".rjust(16)
        print "\n"
        puts "-" * 92
        result.each do |h,k|
            print h.to_s.center(12)
            print k.to_s.rjust(16)
            print "\n"
        end
        puts "-" * 92
        puts


        result = Hash.new  {|h,k| h[k] = [] }
        data.each { |h| result[h[9]].push([h[8],h[15]]) }
        result2 = Hash.new(0)
        result["404"].each { |a| result2[a] += 1 }

        puts
        puts '404 error code'.upcase.center(92)
        puts "-" * 92
        print "Request".ljust(55)
        print "Count".rjust(12)
        print "\n"
        puts "-" * 92
        result2.each do |h,k|
            print h[0][1...-1][0..50].ljust(55, ' ')
            print k.to_s.rjust(12)
            #print h[1][1...-1][0..50]
            print "\n"
        end
        puts "-" * 92
        puts
    end
=begin
=end
end
