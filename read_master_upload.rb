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
    segment_link[sam_costing_id] = salesforce_segment_id
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

  #CSV.foreach('./data/monthly_upload.csv', :quote_char => '"', :encoding => 'utf-16le:utf-8') do |row|
  #CSV.foreach('./data/paid_last_14_days.csv', :quote_char => '"', :encoding => 'utf-16le:utf-8') do |row|
  CSV.foreach('./data/paid_last_14_days.txt', {:col_sep => "\t", :encoding => 'utf-16le:utf-8'}) do |row|
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
 
    #                 row[booking_headers['Travel Day']] ).to_xls_date
    #return_day = (row[booking_headers['Return Day']] || "00/00/00").to_xls_date
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
    original_amount= row[booking_headers['Original Amount']]
    #puts original_amount
    original_amount= row[booking_headers['Original Amount']].gsub(/[^\d\.]/,'')
    #puts original_amount
    estimated_commission = row[booking_headers['Estimated Commission']].gsub(/[^\d\.]/,'')
    total_inc_gst = row[booking_headers['Total inc GST (AUD)']].gsub(/[^\d\.]/,'')
    commission_rate = row[booking_headers['Commission Rate']].gsub(/[^\d\.]/,'')
    expected_commission = row[booking_headers['Expected Commission AUD']].gsub(/[^\d\.]/,'')
    commission_paid = row[booking_headers['Commission Paid']].gsub(/[^\d\.]/,'')
    room_nights = row[booking_headers['Room Nights']].to_i
    return_day = nil
    #puts  row[booking_headers['Return Day']] 
    #if row[booking_headers['Return Day']] == nil
    #  puts "the return date #{row[booking_headers['Return Day']]}"
    #  return_day = (Date.parse"00/00/00").to_xls_date
    #else
      ret_day = (Date.parse departure_day) + room_nights
      return_day = ret_day.strftime("%Y-%m-%dT00:00:00.00Z")
    #end
       
      
        
    # link the data up!
    salesforce_booking_ref = segment_link[costing_id]
    salesforce_hotel_ref = hotel_link[sam_hotel_id]
    email = hotel_email[sam_hotel_id]

    #The imported table is slightly different to the TI SAM fields
    booking_array = [
      chain, hotel_name, sam_hotel_id, salesforce_hotel_ref, 
      company, consultant, booking_id, 
      booking_date, departure_day, 
      return_day, currency_code, costing_id, 
      salesforce_booking_ref, transaction_number,
      booking_status, passenger_name, payment_date, 
      dom_int, rate, original_amount, 
      estimated_commission, total_inc_gst, 
      commission_rate, expected_commission, 
      commission_paid, room_nights
    ]
  
    update_array = [return_day, salesforce_booking_ref, 
                    payment_date, commission_paid, 
                    salesforce_hotel_ref]

    if !salesforce_hotel_ref
      missing_hotels[sam_hotel_id] = [chain, hotel_name, sam_hotel_id]
    elsif !salesforce_booking_ref
      new_bookings << booking_array
    else
      updated_bookings << booking_array
    end
  end
  puts "Loaded #{count -4} SAM TI records"
end


###################################################################

def link_galileo_data(hotels)
  puts "Loading the MOS hotel data and integrating data"
  #puts "Number of segment links #{segment_link.count}"
  count = 0
=begin
    "trip_id" => "Booking Id",
    "company" => "Company",
    "consultants" => "Consultant",
    "travellers" => "Passenger Name",
    "costing_id" => "Costing Unique",
    "supplier_name" => "Hotel",
    "departure_date" => "Departure Day",
=end

  mos_header = Hash.new
  mos_hotel_list = Hash.new

  hotel_match_count = 0
  unmatched_count = 0
  matched_hotels = []
  

  CSV.foreach('./data/locomotemos_upload.csv') do |row|
    count+=1
    #puts row
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
    mos_hotel_name = row[mos_header['supplier_name']].strip.upcase
    departure_day = row[mos_header['depature_date']].to_xls_date
    duration = row[mos_header['duration']].to_i
    puts departure_day
    puts row[mos_header['depature_date']]
    return_day = DateTime.strptime(row[mos_header['depature_date']], "%Y-%m-%d")
    puts return_day
    
    hotel_city = row[mos_header['hotel_city']].strip
    confirmation_id = row[mos_header['confirmation_number']].strip
    quantity = row[mos_header['quantity']].strip
    payment_type = row[mos_header['payment_type']].strip
    rate= row[mos_header['rate']].to_f
    total_aud= row[mos_header['total_aud']].to_f
    currency_code= row[mos_header['currency_code']].to_f
    expected_commission = row[mos_header['expected_commission_aud']].gsub(/[^\d\.]/,'')
    begin
      commission_paid = row[mos_header['commission_recieved']] or 0
    rescue
      commission_paid = 0 
    end

    item = [booking_id, company, consultant, costing_id, link, mos_hotel_name,
            departure_day, hotel_city, confirmation_id, quantity, payment_type, total_aud,
            currency_code, expected_commission, commission_paid]
    #puts item
    
    matches_each_hotel = 0


    if not mos_hotel_list[mos_hotel_name].nil?
      next
    else
      mos_hotel_list[mos_hotel_name] = true
    end

    hotels.each do |hotel|
      match = hotel[:hotel_name].similar(mos_hotel_name)
      if match > 80 
        hotel_match_count += 1 if matches_each_hotel == 0
        matches_each_hotel += 1
        print "." 
        #puts "Match %#{match} #{hotel[:hotel_name]}  #{hotel[:sam_hotel_id]}  #{mos_hotel_name} #{hotel_city}"
        matched_hotels << [match, mos_hotel_name, hotel[:hotel_name], hotel[:sam_hotel_id], hotel_city]
      end
    end
    if matches_each_hotel == 0 
      matched_hotels << [ 0, mos_hotel_name, "" , ""]
      print "-"
      unmatched_count += 1
    else 
      print "+"
    end  
  end
 
  puts 
  puts "match count #{hotel_match_count} unmatched_count #{unmatched_count} unique #{mos_hotel_list.count}"
  matching_header = ["match", "mos_name", "sabre_name", "sabre_code", "mos_city_code"]
  print_csv("mos_matching.csv", matched_hotels, matching_header)

=begin
    # link the data up!
    salesforce_booking_ref = segment_link[costing_id]
    salesforce_hotel_ref = hotel_link[sam_hotel_id]
    email = hotel_email[sam_hotel_id]

    #The imported table is slightly different to the TI SAM fields
    booking_array = [
      chain, hotel_name, sam_hotel_id, salesforce_hotel_ref, 
      company, consultant, booking_id, 
      booking_date, departure_day, 
      return_day, currency_code, costing_id, 
      salesforce_booking_ref, transaction_number,
      booking_status, passenger_name, payment_date, 
      dom_int, rate, original_amount, 
      estimated_commission, total_inc_gst, 
      commission_rate, expected_commission, 
      commission_paid, room_nights
    ]
  
    update_array = [return_day, salesforce_booking_ref, 
                    payment_date, commission_paid, 
                    salesforce_hotel_ref]

    if !salesforce_hotel_ref
      missing_hotels[sam_hotel_id] = [chain, hotel_name, sam_hotel_id]
    elsif !salesforce_booking_ref
      new_bookings << booking_array
    else
      updated_bookings << booking_array
    end
  end
  puts "Loaded #{count -4} SAM TI records"
=end
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

#link_galileo_data(hotels)


link_sabre_data(booking_headers, missing_hotels, 
                  new_bookings, updated_bookings, new_hotels,
                  segment_link,
                  hotel_link, hotel_email)

puts "New hotels #{missing_hotels.count}"
puts "Updated bookings #{updated_bookings.count}"
puts "New bookings #{new_bookings.count}"

new_bookings_header = ["Chain","Hotel","Hotel Sam ID","Email ID","Company","Consultant","Booking Id","Booking Date","Departure Day","Return Day","Currency Code","Costing Unique","ID","Transaction Number","Booking Status","Passenger Name","Payment Date","Dom / Int","Rate","Original Amount","Estimated Commission","Total inc GST (AUD)","Commission Rate","Expected Commission AUD","Commission Paid","Room Nights"]

update_bookings_header = new_bookings_header

hotels_header = ["Chain", "Hotel", "Hotel Sam ID"]

print_csv("./data/update_segments.csv", updated_bookings, update_bookings_header) 
print_csv("./data/new_segments.csv", new_bookings, new_bookings_header) 
print_csv("./data/new_hotels_for_salesforce.csv", missing_hotels, hotels_header)


