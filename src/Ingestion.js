var fs = require('fs');
var parse = require('csv-parse');
var transform = require('stream-transform');
var cassandra = require('cassandra-driver');
var async = require('async');


var client = new cassandra.Client({contactPoints: ['localhost']});

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


async.series([
    function(next){initCassandraSchema(client, next)},
    function(next){
        var parser = parse({delimiter: ','})
        var input = fs.createReadStream('data/us_unemployment.csv');

        var transformer = transform(function(line){
            client.execute(insert, parseData(line), {prepare:true, consistency:cassandra.types.consistencies.one},next);
            return line;
        });

        transformer.on('error', next);

        input.pipe(parser).pipe(transformer);
    }
],displayError);



function initCassandraSchema(client, next) {
    console.log("********** Initializing schema for demo **********")


    async.series([
        function(nextCall) {
            client.execute("CREATE KEYSPACE IF NOT EXISTS nodejs_demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",[], nextCall);
        },
        function(nextCall) {
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
                footnotes text)",[],nextCall);
        },
        function(nextCall) {
            client.execute("TRUNCATE nodejs_demo.us_unemployment", [],nextCall)
        }
    ], next);
}

function parseData(line) {
    return [parseInt(line[0]), parseInt(line[1]), parseInt(line[2]), parseFloat(line[3]),
        parseInt(line[4]), parseFloat(line[5]), parseInt(line[6]), parseInt(line[7]),
        parseInt(line[8]), parseFloat(line[9]), parseInt(line[10]), line[11]]};

function displayError(err) {
    if(err) console.log("Error encountered : "+err)
}