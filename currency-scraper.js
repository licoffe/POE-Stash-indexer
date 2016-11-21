// Requirements
var async            = require( "async" );
var Logger           = require( "./modules/logger.js" );
var logger           = new Logger();
logger.set_use_timestamp( true );
logger.set_file_path( "./log-currency-scraper.txt" );
var mongo_client     = require( "mongodb" ).MongoClient;
var scriptName       = "Currency-scraper";
var interrupt        = false;
var collection       = "currency";
var Currency         = require( "./modules/currency.js" );
var leagues          = [ "Standard", "Hardcore", "Essence", "Hardcore+Essence" ];
var refreshInterval  = 5 * 60 * 1000; // 5 min

/**
 * Scrape poe.trade for currency values
 *
 * @param Callback function
 * @return None
 */
function scrapeCurrencies( db ) {
    // If interrupt signal received
    if ( interrupt ) {
        logger.log( "Exiting", scriptName );
        db.close();
        process.exit( 0 );
    }

    db.createCollection( collection, function( err, currencyCollection ) {
        if ( err ) {
            logger.log( "There was an error creating the collection: " + err, scriptName, "e" );
            db.close();
        } else {
            async.eachLimit( leagues, 1, function( league, leagueCb ) {
                var currency   = new Currency( league );
                var currencies = currency.currencies;
                async.eachLimit( currencies, 1, function( cur, currencyCb ) {
                    logger.log( "Checking currency exchange rate for '" + cur + "' in '" + league + "'", scriptName );
                    currency.getAllRates( cur, function( results ) {
                        currencyCollection.insert( 
                            results, { w : 1 }, 
                            function( err, result ) {
                                if ( err ) {
                                    logger.log( "There was an error inserting currency value: " + err, scriptName, "e" );
                                }
                                currencyCb();
                            }
                        );
                    });
                }, function( err ) {
                    if ( err ) {
                        logger.log( err, scriptName, "e" );
                    }
                    leagueCb();
                });
            }, function( err ) {
                if ( err ) {
                    logger.log( err, scriptName, "e" );
                }
                // Schedule to run 5 min after
                setTimeout( scrapeCurrencies, refreshInterval, db );
            });
        }
    });
}

/**
 * Connect to MongoDB. If successfull, run provided callback function
 *
 * @param Callback function
 * @return None
 */
function connectToDB( callback ) {

    // Read config file
    logger.log( "Reading config file", scriptName );
    var config = require( "./config.json" );

    // Connect to the db
    mongo_client.connect( "mongodb://" + config.dbAddress + ":" + config.dbPort + "/" + config.dbName,
                         function( err, db ) {
        if ( err ) {
            logger.log( err, scriptName, "e" );
            logger.log( "Make sure MongoDB has been started", scriptName, "e" );
            process.exit(0);
        }
        logger.log( "Connected to MongoDB", scriptName );
        if ( config.authenticate ) {
            db.authenticate( config.user, config.pass, function( err, res ) {
                if ( err ) {
                    logger.log( err, scriptName, "e" );
                    process.exit(0);
                }
                logger.log( "Logged in " + config.dbName, scriptName );
                callback( db );
            });
        } else {
            logger.log( "Logged in " + config.dbName, scriptName );
            callback( db );
        }
    });
}

// Main loop
function main() {
    connectToDB( function( db ) {
        logger.log( "Attempting to create or reuse POE_price collection", scriptName );
        // Create online status index
        db.createCollection( collection, function( err, collection ) {
            logger.log( "Checking indexes have been generated", scriptName );
            var indexFields = [
                { "timestamp": 1 },
                { "league": 1 },
                { "sell": 1 }
            ];

            function createIndexs(indexFields){
                return indexFields.map(function (key) {
                    return new Promise(function (resolve, reject){
                        collection.createIndex(key, function () {
                            resolve();
                        });
                    });
                });
            }

            // When all indexes are done
            Promise.all(createIndexs(indexFields)).then(value => {

                // Scrape currencies from poe.trade
                scrapeCurrencies( db, function() {
                
                });
            }, err => {
                logger.log( "There was an error creating the collection: " + err, scriptName, "e" );
            });
        });
    });
}

process.on('SIGINT', function() {
    logger.log( "\rCaught interrupt signal, exiting gracefully", scriptName, "e" );
    interrupt = true;
    process.exit( 0 );
});

main();
