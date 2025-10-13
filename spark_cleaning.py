import sys
assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types
import string, re

# use spark to clean the opening moves CSV file
# fix a mismatch using Regex 
# and join it with the big hstorical games file

# schema for games
Games_Schema = types.StructType([
    types.StructField('White', types.StringType()),
    types.StructField('Black', types.StringType()),
    types.StructField('WhiteElo', types.IntegerType()),
    types.StructField('BlackElo', types.IntegerType()),
    types.StructField('Result', types.StringType()),
    types.StructField('Date', types.StringType()),
    types.StructField('Event', types.StringType()),
    types.StructField('ECO', types.StringType()),
    types.StructField('Moves', types.StringType()),

])

# schema for openings
Openings_Schema = types.StructType([
    types.StructField('ECO', types.StringType()),
    types.StructField('OpeningName', types.StringType()),
    types.StructField('Moves', types.StringType())

])

# fix the formating using regex
def add_space_after_dot(moves):
    if moves is None:
        return None
    # This regex adds a space after every number+dot
    return re.sub(r"(\d+)\.", r"\1. ", moves)
add_space_udf = functions.udf(add_space_after_dot, types.StringType())


# clean the data
def main(games_directory, openings_directory, output_directory):
    gamesData = spark.read.csv(games_directory, header=True, schema=Games_Schema)
    openingsData = spark.read.csv(openings_directory, header=True, schema=Openings_Schema)
    # Rename the columsn becasue the other DataFrame also has a moves column
    openingsData = openingsData.withColumnRenamed("Moves", "OpeningMovesStar")

    #remove rows with NULL data and drop the columns we won't need 
    gamesData = gamesData.filter(gamesData['Moves'].isNotNull()).filter(gamesData['WhiteElo'].isNotNull()).filter(gamesData['BlackElo'].isNotNull()).\
        filter(gamesData["Result"].isNotNull()).drop(gamesData['Date']).drop(gamesData['Event'])
    
    
    
    #remove rows with NULL data 
    openingsData = openingsData.filter(openingsData['ECO'].isNotNull()).filter(openingsData['OpeningName'].isNotNull()).filter(openingsData['OpeningMovesStar'].isNotNull())
    # filter through and keep the opening moves that start with 1. since that is the begining of the game 
    openingsData = openingsData.filter(openingsData["OpeningMovesStar"].rlike("^1\\."))
    # take off the hanging start for comparision later 
    openingsData = openingsData.withColumn("OpeningMovesNotClean", functions.regexp_replace(openingsData['OpeningMovesStar'], "\\*$", "")).drop(openingsData['OpeningMovesStar'])
    # Use Regex to make the of the opening moves same as the format for the game moves for comarison
    openingsData = openingsData.withColumn("OpeningMoves", add_space_udf("OpeningMovesNotClean"))
    # Removes hanging space charachters for comparison 
    openingsData = openingsData.withColumn("OpeningMoves", functions.trim("OpeningMoves"))

    # join on ECO code, and filter out the unmatching openingsmoves and moves
    Data = gamesData.join(openingsData.hint('broadcast'), on='ECO').filter(functions.col("Moves").startswith(functions.col("OpeningMoves")))

    # take the rows we need 
    Data = Data.select("WhiteElo", "BlackElo", "Result", "Moves", "OpeningMoves","ECO")
    # write out the file
    Data.write.csv(output_directory, header=True)
    
    return


    

if __name__=='__main__':
    games_directory = sys.argv[1]
    openings_directory = sys.argv[2]
    output_directory = sys.argv[3]
    spark = SparkSession.builder.appName('spark cleaning').getOrCreate()
    assert spark.version >= '3.2' # make sure we have Spark 3.2+
    spark.sparkContext.setLogLevel('WARN')

    main(games_directory, openings_directory, output_directory)