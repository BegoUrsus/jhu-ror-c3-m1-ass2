require 'mongo'
require 'json'
require 'pp'
require 'byebug'
Mongo::Logger.logger.level = ::Logger::INFO
#Mongo::Logger.logger.level = ::Logger::DEBUG

class Solution
  MONGO_URL='mongodb://localhost:27017'
  MONGO_DATABASE='test'
  RACE_COLLECTION='race1'

  # helper function to obtain connection to server and set connection to use specific DB
  # set environment variables MONGO_URL and MONGO_DATABASE to alternate values if not
  # using the default.
  def self.mongo_client
    url=ENV['MONGO_URL'] ||= MONGO_URL
    database=ENV['MONGO_DATABASE'] ||= MONGO_DATABASE 
    db = Mongo::Client.new(url)
    @@db=db.use(database)
  end

  # helper method to obtain collection used to make race results. set environment
  # variable RACE_COLLECTION to alternate value if not using the default.
  def self.collection
    collection=ENV['RACE_COLLECTION'] ||= RACE_COLLECTION
    return mongo_client[collection]
  end
  
  # helper method that will load a file and return a parsed JSON document as a hash
  def self.load_hash(file_path) 
    file=File.read(file_path)
    JSON.parse(file)
  end

  # initialization method to get reference to the collection for instance methods to use
  def initialize
    @coll=self.class.collection
  end

  #
  # Lecture 1: Create
  #

  # deletes all documents from the collection 
  # and returns the Mongo result object for the command
  def clear_collection
    return @coll.delete_many({})
  end

  # accepts a file_path argument to a file of JSON data containing race results
  # reads the JSON contents of the file into an array of hashes. 
  # (uses the load_hash() method for this)
  # inserts each of the hash elements of the array into the database collection
  # returns the Mongo result object for the command
  def load_collection(file_path) 
    jsdoc = self.class.load_hash(file_path)
    #pp jsdoc
    return @coll.insert_many(jsdoc)
  end

  # accepts a hash for race result data
  # inserts this one race result into the collection
  # returns the Mongo result object for the command
  def insert(race_result)
    return @coll.insert_one(race_result)
  end

  #
  # Lecture 2: Find By Prototype
  #

  # accepts an optional hash prototype
  # finds all documents that match all parameters in the hash (or all documents if empty hash). 
  # In this case, the caller is required to form the hash for the query that matches the field names. 
  # returns the Mongo result object for the command with all fields of the document included
  def all(prototype={})
    r =  @coll.find(prototype)
    #p "antes"
    #r.each {|x| puts x}
    #p "despues"
    return r
  end

  # accepts a first and last name
  # finds all documents that match the first and last name provided. 
  # In this case, you must actually form the hash for the query.
  # forms a projection that returns only the first_name, last_name, 
  # and number properties (hint: projection 
  # returns the Mongo result object for the command
  def find_by_name(fname, lname)
    prototype = {}
    prototype["first_name"] = fname
    prototype["last_name"] = lname
    #p prototype
    r = @coll.find(prototype).projection(first_name:true, last_name:true, number:true, _id:false)
    return r
  end

  #
  # Lecture 3: Paging
  #

  # Accepts a group name, offset value, and limit value
  # Finds only race results for the specified group
  # Forms a projection that eliminates the group and _id fields from the results (hint: projection)
  # Sorts the results by time (secs), accending (hint: sort)
  # Skips offset documents in the ordered result (hint: skip)
  # Limits the results to only limit documents (hint: limit)
  # Returns the Mongo result object for the command
  def find_group_results(group, offset, limit) 
    prototype = {}
    prototype["group"] = group
    r = @coll.find(prototype).projection(_id:false, group:false).sort({:secs => 1}).skip(offset).limit(limit)
    return r
  end

  #
  # Lecture 4: Find By Criteria
  #

  # accepts a min and max value
  # finds all race results with a time (secs) that is between min and max (exclusive)
  def find_between(min, max)
    r =  @coll.find(:secs => {:$gt => min, :$lt => max})
    return r
  end

  # Accepts a letter, offset, and limit
  # Finds all race results with the last_name that starts with the letter provided 
  # using a regular expression. You only need to treat letter as a string 
  # and do not have to enforce as a character. However, you should convert this value to upper case. 
  # The following REGEX "^S.+" will locate all names starting with the letter S.
  # Orders the results by last_name, ascending
  # Skips the first offset documents
  # Limits results to limit documents
  # Returns the Mongo result object for the command
  def find_by_letter(letter, offset, limit) 
    regex_expr = '^' + letter + '.+'
    r = @coll.find(:last_name => {:$regex => regex_expr}).sort({:last_name => 1}).skip(offset).limit(limit)
    return r
  end

  #
  # Lecture 5: Updates
  #
  
  # Accepts a hash of racer properties
  # Finds the racer associated with the _id property in the input hash
  # Replaces all existing fields for the racer with what is provided (hint: replace_one)
  # Returns the Mongo result object for the command
  def update_racer(racer)
    r = @coll.find(:_id => racer["_id"]).replace_one(racer)
    return r
  end

  # Accepts the racer number and an amount of time in seconds
  # Finds the racer's document and increments the time in the database without retrieving the actual document. 
  # (hint: :$inc)
  def add_time(number, secs)
    r = @coll.find(:number => number).update_one(:$inc => {:secs => secs})
    return r
  end

end

s=Solution.new
race1=Solution.collection
