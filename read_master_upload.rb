require 'csv'
require 'similar_text'



class String
   def to_xls_date
       str = ''
       begin
          date_time = Date.parse self
          str = date_time.strftime("%Y-%m-%dT00:00:00.00Z")
       rescue
          str = ""
       end
       return str
   end
end


#############################################################################
def get_segment_links(segment_link_headers, segment_link)
  puts "Reading the costing to Salesforce mapping"
  count = 0
  CSV.foreach('./data/salesforce_costing_link.csv') do |row|
    count +=1
    if count == 1
      row.each_with_index {|header,i|
        segment_link_headers[header] = i
      }
      #print segment_link_headers
      next
    end
    #puts row if count < 10
    salesforce_segment_id = row[segment_link_headers["ID"]]
    sam_costing_id = row[segment_link_headers["COSTING_ID__C"]].to_i
    sam_hotel_id = row[segment_link_headers["HOTEL_SAM_ID__C"]]
    segment_link[[sam_hotel_id, sam_costing_id]] = salesforce_segment_id
    #puts "#{row} sam #{sam_costing_id}  sales #{salesforce_segment_id}" if count < 10
    if count % 1000 == 0
       print '.'
    end
  end
  puts 
  puts "Loaded #{count} links"
end

##############################################################################
def get_hotel_links(hotel_link_headers, hotel_link, hotel_email, hotels)
  puts "Loading Salesforce to hotel mapping"
  count = 0
  CSV.foreach("./data/salesforce_hotels_link.csv", :quote_char=>'"') do |row|
    count += 1
    if count == 1
      row.each_with_index{|header, i|
        hotel_link_headers[header] = i
      }
      next
    end
    salesforce_hotel_id = row[hotel_link_headers['ID']]
    sam_hotel_id = row[hotel_link_headers['NAME']].strip.upcase
    hotel_name = row[hotel_link_headers['HOTEL_NAME__C']].strip.upcase
    email = row[hotel_link_headers['HOTEL_EMAIL_ADDRESS__C']]
    hotel_chain =['CHAIN_GROUP__C']
    hotel_link[sam_hotel_id] = salesforce_hotel_id
    hotel_email[sam_hotel_id] = email
    hotels << {salesforce_id: salesforce_hotel_id, sam_hotel_id: sam_hotel_id, hotel_name: hotel_name} 
    
  end  
  puts "Loaded #{count} hotel links"
end

##############################################################################

def link_sabre_data(booking_headers, missing_hotels, 
                  new_bookings, updated_bookings, new_hotels,
                  segment_link,
                  hotel_link, hotel_email)
  puts "Loading the SAM hotel data and integrating data"
  puts "Number of segment links #{segment_link.count}"
  count = 0

  filename = './data/auto_monthly_upload.txt' if File.exists?('./data/auto_monthly_upload.txt')
  filename = './data/paid_last_14_days.txt' if File.exists?('./data/paid_last_14_days.txt')
  CSV.foreach(filename, {:col_sep => "\t", :encoding => 'utf-16le:utf-8'}) do |row|
    count+=1
    #puts row
    if count == 3
      row.each_with_index do |header,i|
        if i == 2
          # Need to have the raw text as SAM doens't provide anything
          header = 'Hotel SAM ID'
        end
        booking_headers[header] = i
      end
      #puts booking_headers
    end
    if count < 4
      next
    end

    chain = row[booking_headers['Chain']].strip
    hotel_name = row[booking_headers['Hotel']].strip
    sam_hotel_id = row[booking_headers['Hotel SAM ID']].strip.upcase
    company = row[booking_headers['Company']].strip
    consultant = row[booking_headers['Consultant']].strip
    booking_id = row[booking_headers['Booking Id']].to_i
    booking_date = row[booking_headers['Booking Date']].to_xls_date
    departure_key = booking_headers['Departure Day'] || booking_headers['Travel Day']
    departure_day = (row[departure_key].to_xls_date || "00/00/00").to_xls_date
    currency_code = row[booking_headers['Currency Code']].strip
    costing_id = row[booking_headers['Costing Unique']].to_i
    transaction_number = row[booking_headers['Transaction Number']].to_i
    booking_status = row[booking_headers['Booking Status']].strip
    passenger_name = row[booking_headers['Passenger Name']].strip
    
    if row[booking_headers['Payment Date']].nil?
      payment_date = ""
    else
      payment_date = row[booking_headers['Payment Date']].to_xls_date 
    end
    dom_int = row[booking_headers['Dom / Int']].strip
   
    rate = row[booking_headers['Rate']].gsub(/[^\d\.]/,'').gsub(/[^\d\.]/,'')
    original_amount= row[booking_headers['Original Amount']].gsub(/[^\d\.]/,'')
    estimated_commission = row[booking_headers['Estimated Commission']].gsub(/[^\d\.]/,'')
    total_inc_gst = row[booking_headers['Total inc GST (AUD)']].gsub(/[^\d\.]/,'')
    commission_rate = row[booking_headers['Commission Rate']].gsub(/[^\d\.]/,'')
    expected_commission = row[booking_headers['Expected Commission AUD']].gsub(/[^\d\.]/,'')
    commission_paid = row[booking_headers['Commission Paid']].gsub(/[^\d\.]/,'')
    room_nights = row[booking_headers['Room Nights']].to_i
    return_day = nil
    ret_day = (Date.parse departure_day) + room_nights
    return_day = ret_day.strftime("%Y-%m-%dT00:00:00.00Z")
   
        
    # link the data up!
    salesforce_booking_ref = segment_link[[sam_hotel_id, costing_id]]
    salesforce_hotel_ref = hotel_link[sam_hotel_id]
    
    #The imported table is slightly different to the TI SAM fields
    booking_array = [
      chain, 
      hotel_name, 
      sam_hotel_id, 
      salesforce_hotel_ref, 
      company, 
      consultant, 
      booking_id, 
      booking_date, 
      departure_day, 
      return_day, 
      currency_code, costing_id, 
      salesforce_booking_ref, 
      transaction_number,
      booking_status, 
      passenger_name, 
      payment_date, 
      dom_int, 
      rate, 
      original_amount, 
      estimated_commission, 
      total_inc_gst, 
      commission_rate, 
      expected_commission, 
      commission_paid, 
      room_nights
    ]
  
    update_array = [
      return_day, 
      salesforce_booking_ref, 
      payment_date, 
      commission_paid, 
      salesforce_hotel_ref
    ]

    if !salesforce_hotel_ref
      missing_hotels[sam_hotel_id] = [chain, hotel_name, sam_hotel_id, "unknown@unknown.com", "unknown address"]
    elsif !salesforce_booking_ref
      new_bookings << booking_array
    else
      updated_bookings << booking_array
    end
  end
  puts "Loaded #{count -4} SAM TI records"
end


###################################################################

def link_galileo_data(booking_headers, missing_hotels, 
                  new_bookings, updated_bookings, new_hotels,
                  segment_link,
                  hotel_link, hotel_email)
  
  return

  puts "Loading the MOS hotel data and integrating data"
  count = 0
  mos_header = Hash.new
  mos_hotel_list = Hash.new

  hotel_match_count = 0
  unmatched_count = 0
  matched_hotels = []
  # there were serveral passengers name missing from the Owens report

  commission_names = {}
  commission_header = {}
  
  puts "Loading the trip id and names"
   
  CSV.foreach('./data/mos_hcr.csv') do |row|
    count+=1
    #puts "#{row}"
    if count == 1
      row.each_with_index do |header,i|
        commission_header[header] = i
      end
      next
    end
     
    trip_id = row[commission_header['trip_id']].to_i
    travellers = row[commission_header['travellers']].strip
    commission_names[trip_id] = travellers
  end


  puts "Loading the MOS hotel data and integrating data"
  count = 0
  CSV.foreach('./data/gal_test.csv') do |row|
    count+=1
    puts "#{row}"
    if count == 1
      row.each_with_index do |header,i|
        mos_header[header] = i
      end
      next
    end
    
     
    booking_id = row[mos_header['trip_id']].to_i 
    company = row[mos_header['company']].strip
    consultant = row[mos_header['consultants']].strip
    costing_id = row[mos_header['costing_id']].to_i
    link = row[mos_header['link']].strip
    hotel_city = row[mos_header['hotel_city']].strip 
    hotel_name = row[mos_header['supplier_name']].strip.upcase + " " + hotel_city
    hotel_address = row[mos_header['hotel_address']].strip
    departure_day = row[mos_header['departure_date']].to_xls_date
    duration = row[mos_header['duration']].to_i
    ret_day = DateTime.strptime(row[mos_header['departure_date']], "%Y-%m-%d") + duration
    return_day = ret_day.strftime("%Y-%m-%dT00:00:00.00Z")
    passenger_name = commission_names[booking_id]
    hotel_city = row[mos_header['hotel_city']].strip 
    confirmation_id = row[mos_header['confirmation_number']].strip
    quantity = row[mos_header['quantity']].strip
    payment_type = row[mos_header['payment_type']].strip
    rate= row[mos_header['rate']].to_f
    total_aud= row[mos_header['total_aud']].to_f
    currency_code= row[mos_header['currency_code']]
    dom_int = "International"
    dom_int = "Domestic" if currency_code == "AUD"
    dom_int = "Trans Tasman" if currency_code == "NZD"

    expected_commission = row[mos_header['expected_commission_aud']].gsub(/[^\d\.]/,'')
    begin
      commission_paid = row[mos_header['commission_recieved']]/100.00 or 0
    rescue
      commission_paid = 0 
    end
    hotel_id = "GAL__#{row[mos_header['hotel_vendor_code']].strip.upcase}"
    chain = row[mos_header['hotel_chain_code']]
    email = row[mos_header['hotel_email']].strip
    puts email


    item = [
       booking_id, 
       company, 
       consultant, 
       costing_id, 
       link, 
       hotel_name,
       departure_day, 
       hotel_city, 
       confirmation_id, 
       quantity,
       payment_type, 
       total_aud,
       currency_code, 
       expected_commission, 
       commission_paid
    ]
    #puts "#{item}"

    return_day = ret_day.strftime("%Y-%m-%dT00:00:00.00Z")
    
    # link the data up!
    salesforce_booking_ref = segment_link[[hotel_id, costing_id]]
    salesforce_hotel_ref = hotel_link[hotel_id]
   
    #The imported table is slightly different to the TI SAM fields
    booking_array = [
      chain, hotel_name, hotel_id, salesforce_hotel_ref, 
      company, consultant, booking_id, 
      "", departure_day, 
      return_day, currency_code, costing_id, 
      salesforce_booking_ref, "",
      "" , passenger_name, "", 
      dom_int, rate, total_aud, 
      expected_commission, 0, 
      10, expected_commission, 
      commission_paid, duration
    ]
  
    update_array = [return_day, salesforce_booking_ref, 
                    "payment_date", commission_paid, 
                    salesforce_hotel_ref]

    if !salesforce_hotel_ref
      missing_hotels[hotel_id] = [chain, hotel_name, hotel_id, email, hotel_address ]
    elsif !salesforce_booking_ref
      new_bookings << booking_array
    else
      updated_bookings << booking_array
    end
  end
  puts "Loaded #{count} MOS Commission records"

end


######################################################################


def print_csv(file_name, data, header)
  puts "Writing #{file_name}"
  CSV.open(file_name, "w") do |csv_file|
    csv_file << header
    if data.class == Array
      data.each do |line|
        csv_file << line
      end
    elsif data.class == Hash
      data.each do |key, field_data|
        csv_file << field_data
      end
    end
  end
end



# The following is basically the main function

segment_link_headers = Hash.new
segment_link = Hash.new
  
hotel_link = Hash.new
hotel_email = Hash.new
hotel_link_headers = Hash.new

booking_headers = Hash.new
missing_hotels = Hash.new

new_bookings = []
updated_bookings = []
new_hotels = []
hotels = []

get_hotel_links(hotel_link_headers, hotel_link, hotel_email, hotels)
get_segment_links(segment_link_headers, segment_link)


link_galileo_data(booking_headers, missing_hotels, 
                  new_bookings, updated_bookings, new_hotels,
                  segment_link,
                  hotel_link, hotel_email)

link_sabre_data(booking_headers, missing_hotels, 
                  new_bookings, updated_bookings, new_hotels,
                  segment_link,
                  hotel_link, hotel_email)

puts "New hotels #{missing_hotels.count}"
puts "Updated bookings #{updated_bookings.count}"
puts "New bookings #{new_bookings.count}"

new_bookings_header = ["Chain","Hotel","Hotel Sam ID","Email ID","Company","Consultant","Booking Id","Booking Date","Departure Day","Return Day","Currency Code","Costing Unique","ID","Transaction Number","Booking Status","Passenger Name","Payment Date","Dom / Int","Rate","Original Amount","Estimated Commission","Total inc GST (AUD)","Commission Rate","Expected Commission AUD","Commission Paid","Room Nights"]

update_bookings_header = new_bookings_header

hotels_header = ["Chain", "Hotel", "Hotel Sam ID", "Email", "Address"]

print_csv("./data/update_segments.csv", updated_bookings, update_bookings_header) 
print_csv("./data/new_segments.csv", new_bookings, new_bookings_header) 
print_csv("./data/new_hotels_for_salesforce.csv", missing_hotels, hotels_header)

#puts segment_link
