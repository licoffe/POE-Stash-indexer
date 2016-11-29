// Requirements
var async            = require( "async" );
var Logger           = require( "./modules/logger.js" );
var logger           = new Logger();
logger.set_use_timestamp( true );
logger.set_file_path( "./log-currency-scraper.txt" );
var mysql            = require( "mysql" );
var scriptName       = "Currency-scraper";
var interrupt        = false;
var Currency         = require( "./modules/currency.js" );
var refreshInterval  = 5 * 60 * 1000; // 5 min
var config           = require( "./config.json" );
var connection;
var credentials      = {
    host     : config.dbAddress,
    port     : config.dbPort,
    user     : config.dbUser,
    password : config.dbPass,
    database : config.dbName
};

/**
 * Scrape poe.trade for currency values
 *
 * @param Callback function
 * @return None
 */
function scrapeCurrencies() {
    var leagues;
    connection.query( "SELECT `LeagueName`, `poeTradeId` FROM `Leagues` WHERE `active` = '1'", function( err, rows ) {
        if ( err ) {
            logger.log( err, scriptName, "w" );
        }
        leagues = rows;
        async.eachLimit( leagues, 1, function( league, leagueCb ) {
            console.log( league );
            var currency   = new Currency( league.poeTradeId );
            var currencies = currency.currencies;
            async.eachLimit( currencies, 1, function( cur, currencyCb ) {
                logger.log( "Checking currency exchange rate for '" + cur + "' in '" + league.LeagueName + "'", scriptName );
                currency.getAllRates( cur, function( results ) {
                    // console.log( results );
                    connection.beginTransaction( function( err ) {
                        if ( err ) {
                            logger.log( err, scriptName, "w" );
                        }
                        var currencyKey = results.sell + "_" + results.timestamp;
                        connection.query( "INSERT INTO `Currencies` (`timestamp`, `league`, `sell`, `currencyKey`) VALUES (?, ?, ?, ?)" , 
                                        [results.timestamp, league.LeagueName, results.sell, currencyKey], function( err, rows ) {
                            if ( err ) {
                                logger.log( "Currencies: " + err, scriptName, "w" );
                            }
                            async.eachLimit( results.rates, 1, function( rate, rateCb ) {
                                connection.query( "INSERT INTO `CurrencyStats` (`buy`, `mean`, `median`, `mode`, `min`, `max`, `currencyKey`) VALUES (?, ?, ?, ?, ?, ?, ?)" , [rate.buy, rate.avg, rate.median, rate.mode, rate.min, rate.max, currencyKey], function( err, rows ) {
                                    if ( err ) {
                                        logger.log( "Currency stats: " + err, scriptName, "w" );
                                    }
                                    rateCb();
                                });
                            }, function( err ) {
                                if ( err ) {
                                    logger.log( err, scriptName, "w" );
                                }
                                connection.commit( function( err ) {
                                    if ( err ) {
                                        logger.log( err, scriptName, "w" );
                                    }
                                    // If interrupt signal received
                                    if ( interrupt ) {
                                        logger.log( "Exiting", scriptName );
                                        connection.end();
                                        process.exit( 0 );
                                    }
                                    currencyCb();
                                });
                            });
                        });
                    });
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
            setTimeout( scrapeCurrencies, refreshInterval );
        });
    });
}

// Main loop
function main() {
    connection = mysql.createConnection( credentials );

    logger.log( "Attempting to connect to POE collection", scriptName );
    connection.connect( function( err ) {
        if ( err ) {
            logger.log( err, scriptName, "e" );
        }
        logger.log( "Connected", scriptName );
            // Scrape currencies from poe.trade
            scrapeCurrencies();
    });
}

process.on('SIGINT', function() {
    logger.log( "\rCaught interrupt signal, exiting gracefully", scriptName, "e" );
    interrupt = true;
});

main();
