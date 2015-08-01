#!/usr/bin/env ruby


require 'find'





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
            data.push(regex_string.match(line)[1..-1])
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
puts data[0]

require 'date'

require 'browser'



require 'geoip'



times = []
basics = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }


os = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
browsers = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
bot = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
search_engine = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
known = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
mobile = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
tablet = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
console = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }


gip = GeoIP.new('data/geoip/GeoIP.dat')

cnames = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
countries = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
ip = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }

referrer = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }

data.each do |h| 
    times.push(DateTime.strptime(h[2][1...-1] , '%d/%b/%Y:%H:%M:%S %z')) 
    dt = DateTime.strptime(h[2][1...-1] , '%d/%b/%Y:%H:%M:%S %z')
    kdt = DateTime.new(dt.year, dt.month, dt.day, dt.hour)
    # puts kdt
    basics[kdt][0] += 1
    basics[kdt][1] += h[11].to_i
    basics[kdt][2] += h[12].to_i
    basics[kdt][3] += h[13].to_i
    basics[kdt][4] += h[14].to_i


    browser = Browser.new(:ua => h[16], :accept_language => "en-us")

    browsers[[browser.name,browser.version]][0] += 1
    browsers[[browser.name,browser.version]][1] += h[11].to_i
    browsers[[browser.name,browser.version]][2] += h[12].to_i
    browsers[[browser.name,browser.version]][3] += h[13].to_i
    browsers[[browser.name,browser.version]][4] += h[14].to_i

    os[browser.platform][0] += 1
    os[browser.platform][1] += h[11].to_i
    os[browser.platform][2] += h[12].to_i
    os[browser.platform][3] += h[13].to_i
    os[browser.platform][4] += h[14].to_i

    bot[browser.bot?][0] += 1
    bot[browser.bot?][1] += h[11].to_i
    bot[browser.bot?][2] += h[12].to_i
    bot[browser.bot?][3] += h[13].to_i
    bot[browser.bot?][4] += h[14].to_i

    search_engine[browser.search_engine?][0] += 1
    search_engine[browser.search_engine?][1] += h[11].to_i
    search_engine[browser.search_engine?][2] += h[12].to_i
    search_engine[browser.search_engine?][3] += h[13].to_i
    search_engine[browser.search_engine?][4] += h[14].to_i

    known[browser.known?][0] += 1
    known[browser.known?][1] += h[11].to_i
    known[browser.known?][2] += h[12].to_i
    known[browser.known?][3] += h[13].to_i
    known[browser.known?][4] += h[14].to_i

    mobile[browser.mobile?][0] += 1
    mobile[browser.mobile?][1] += h[11].to_i
    mobile[browser.mobile?][2] += h[12].to_i
    mobile[browser.mobile?][3] += h[13].to_i
    mobile[browser.mobile?][4] += h[14].to_i

    tablet[browser.tablet?][0] += 1
    tablet[browser.tablet?][1] += h[11].to_i
    tablet[browser.tablet?][2] += h[12].to_i
    tablet[browser.tablet?][3] += h[13].to_i
    tablet[browser.tablet?][4] += h[14].to_i

    console[browser.console?][0] += 1
    console[browser.console?][1] += h[11].to_i
    console[browser.console?][2] += h[12].to_i
    console[browser.console?][3] += h[13].to_i
    console[browser.console?][4] += h[14].to_i


    ip[h[3]][0] += 1
    ip[h[3]][1] += h[11].to_i
    ip[h[3]][2] += h[12].to_i
    ip[h[3]][3] += h[13].to_i
    ip[h[3]][4] += h[14].to_i

    cnames[[gip.country(h[3]).to_hash[:continent_code], gip.country(h[3]).to_hash[:country_name]]][0] += 1
    cnames[[gip.country(h[3]).to_hash[:continent_code], gip.country(h[3]).to_hash[:country_name]]][1] += h[11].to_i
    cnames[[gip.country(h[3]).to_hash[:continent_code], gip.country(h[3]).to_hash[:country_name]]][2] += h[12].to_i
    cnames[[gip.country(h[3]).to_hash[:continent_code], gip.country(h[3]).to_hash[:country_name]]][3] += h[13].to_i
    cnames[[gip.country(h[3]).to_hash[:continent_code], gip.country(h[3]).to_hash[:country_name]]][4] += h[14].to_i

    countries[gip.country(h[3]).to_hash[:country_code2]][0] += 1
    countries[gip.country(h[3]).to_hash[:country_code2]][1] += h[11].to_i
    countries[gip.country(h[3]).to_hash[:country_code2]][2] += h[12].to_i
    countries[gip.country(h[3]).to_hash[:country_code2]][3] += h[13].to_i
    countries[gip.country(h[3]).to_hash[:country_code2]][4] += h[14].to_i

    referrer[h[15]][0] += 1
    referrer[h[15]][1] += h[11].to_i
    referrer[h[15]][2] += h[12].to_i
    referrer[h[15]][3] += h[13].to_i
    referrer[h[15]][4] += h[14].to_i

end


months = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
daymonth = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
daysweek = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }
hours = Hash.new  {|h,k| h[k] = [0,0,0,0,0] }


basics.keys.sort.each do |h|
    # puts h.month
    months[h.month][0] += basics[h][0]
    months[h.month][1] += basics[h][1]
    months[h.month][2] += basics[h][2]
    months[h.month][3] += basics[h][3]
    months[h.month][4] += basics[h][4]

    daymonth[[h.month, h.day]][0] += basics[h][0]
    daymonth[[h.month, h.day]][1] += basics[h][1]
    daymonth[[h.month, h.day]][2] += basics[h][2]
    daymonth[[h.month, h.day]][3] += basics[h][3]
    daymonth[[h.month, h.day]][4] += basics[h][4]

    daysweek[h.wday][0] += basics[h][0]
    daysweek[h.wday][1] += basics[h][1]
    daysweek[h.wday][2] += basics[h][2]
    daysweek[h.wday][3] += basics[h][3]
    daysweek[h.wday][4] += basics[h][4]

    hours[h.hour][0] += basics[h][0]
    hours[h.hour][1] += basics[h][1]
    hours[h.hour][2] += basics[h][2]
    hours[h.hour][3] += basics[h][3]
    hours[h.hour][4] += basics[h][4]

end


puts "-" * 80
puts "First visit: " + times.min.httpdate.to_s 
puts "Last visit: " + times.max.httpdate.to_s

puts "-" * 80
months.each { |h,k| puts " " + h.to_s  + ":" + k.to_s }


puts "-" * 80
daymonth.each { |h,k| puts " " + h.to_s  + ":" + k.to_s }

puts "-" * 80
daysweek.keys.sort.each { |h| puts " " + h.to_s  + ":" + daysweek[h].to_s }


puts "-" * 80
hours.keys.sort.each { |h| puts " " + h.to_s  + ":" + daysweek[h].to_s }


puts "-" * 80
puts "-" * 80
puts "-" * 80
puts 'known traffic' + known[true].to_s
puts 'non known traffic' + known[false].to_s
puts "-" * 80

puts "-" * 80
os.keys.sort.each { |h| puts " " + h.to_s  + ":" + os[h].to_s }

puts "-" * 80
browsers.keys.sort.each { |h| puts " " + h.to_s  + ":" + browsers[h].to_s }


puts "-" * 80
puts 'bot traffic' + bot[true].to_s
# puts 'non bot traffic' + bot[false].to_s

# puts "-" * 80
puts 'search_engine traffic' + search_engine[true].to_s
# puts 'non search_engine traffic' + search_engine[false].to_s

# puts "-" * 80
puts 'mobile traffic' + mobile[true].to_s
# puts 'non mobile traffic' + mobile[false].to_s

# puts "-" * 80
puts 'tablet traffic' + tablet[true].to_s
# puts 'non tablet traffic' + tablet[false].to_s

# puts "-" * 80
puts 'console traffic' + console[true].to_s
# puts 'non console traffic' + console[false].to_s



puts "-" * 80
ip.each { |h,k| puts " " + h.to_s  + ":" + k.to_s }

puts "-" * 80
puts 'continent/country     traffic' 
cnames.keys.sort.each { |h| puts " " + h.to_s  + ":" + cnames[h].to_s }


puts "-" * 80
puts 'country     traffic' 
countries.keys.sort.each { |h| puts " " + h.to_s  + ":" + countries[h].to_s }

puts "-" * 80
puts 'referrer     traffic' 
referrer.keys.sort.each { |h| puts " " + h.to_s  + ":" + referrer[h].to_s }


puts "-" * 80
puts "-" * 30 + "HTTP CODES".center(20,' ') + "-" * 30 
puts "-" * 80

result = Hash.new(0)
data.each { |h| result[h[9]] += 1 }
# puts result
result.each { |h,k| puts " " + h.to_s  + ":" + k.to_s }


result = Hash.new  {|h,k| h[k] = [] }

data.each { |h| result[h[9]].push([h[8],h[15]]) }

# result["404"].each { |f| puts f[0].ljust(60,' ')  + ":" + f[1] }
# puts "-" * 80

puts "-" * 30 + "404 Error code".center(20,' ') + "-" * 30 

result2 = Hash.new(0)
result["404"].each { |a| result2[a] += 1 }

result2.each { |h,k| puts h[0][1...-1][0..50].ljust(55, ' ') + k.to_s + ' '*3 + h[1][1...-1][0..50] }

