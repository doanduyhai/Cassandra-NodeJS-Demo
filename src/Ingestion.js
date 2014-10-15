var fs = require('fs');
var parse = require('csv-parse');
var transform = require('stream-transform');
var cassandra = require('cassandra-driver');

var parser = parse({delimiter: ','})
var input = fs.createReadStream('data/us_unemployment.csv');

var client = new cassandra.Client({contactPoints: ['localhost']});

initCassandraSchema(client)

var insert = "INSERT INTO nodejs_demo.us_unemployment ( \
        year, \
        civil_non_institutional_count, \
        civil_labor_count, \
        labor_population_percentage, \
        employed_count, \
        employed_percentage, \
        agriculture_part_count, \
        non_agriculture_part_count, \
        unemployed_count, \
        unemployed_percentage_to_labor, \
        not_labor_count,\
        footnotes) \
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";



var transformer = transform(function(line){
    client.execute(insert, parseData(line), {prepare:true, consistency:cassandra.types.consistencies.one},displayError);
    return line;
});

transformer.on('finish', function(){
    console.log("Finish importing data into Cassandra")
    process.exit()
});

input.pipe(parser).pipe(transformer);


function initCassandraSchema(client) {
    console.log("********** Initializing schema for demo **********")
    client.execute("CREATE KEYSPACE IF NOT EXISTS nodejs_demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", displayError);

    client.execute("CREATE TABLE IF NOT EXISTS nodejs_demo.us_unemployment ( \
        year int PRIMARY KEY, \
        civil_non_institutional_count int, \
        civil_labor_count int, \
        labor_population_percentage double, \
        employed_count int, \
        employed_percentage double, \
        agriculture_part_count int, \
        non_agriculture_part_count int, \
        unemployed_count int, \
        unemployed_percentage_to_labor double, \
        not_labor_count int,\
        footnotes text)",displayError);

    client.execute("TRUNCATE nodejs_demo.us_unemployment", displayError)
}

function parseData(line) {
    return [parseInt(line[0]), parseInt(line[1]), parseInt(line[2]), parseFloat(line[3]),
        parseInt(line[4]), parseFloat(line[5]), parseInt(line[6]), parseInt(line[7]),
        parseInt(line[8]), parseFloat(line[9]), parseInt(line[10]), line[11]]};

function displayError(err) {
    if(err) console.log("Error encountered : "+err)
}