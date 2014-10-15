var cassandra = require('cassandra-driver');

var client = new cassandra.Client({contactPoints: ['localhost']});

var select = "SELECT * FROM nodejs_demo.us_unemployment";

var stream = client.stream(select,[], {autoPage:true, fetchSize:5}, displayError);


stream
    .on('readable', function() {
        while (row = this.read()) {
            console.log("Year : %, unemployment rate: %".interpolate(row.year, row.unemployed_percentage_to_labor))
        }
    })
    .on('end', function() {
        console.log("No more data")
        process.exit()
    })
    .on('error', function(err) {
        console.log("Error : "+err)
     });

function displayError(err) {
    if(err) console.log("Error encountered : "+err)
}

if (!String.prototype.interpolate) {
    String.prototype.interpolate = function() {
        var i = 0, text = this;

        for (i = 0; i < arguments.length; i++) {
            text = text.replace('%', arguments[i]);
        }

        return text;
    };
};