/**
 * Currency module to poll poe.trade for latest currency rates
 *
 */

var async   = require( "async" );
var jsdom   = require("jsdom"); 
var request = require( "request" );
var $       = require('jquery')(jsdom.jsdom().defaultView); 
var Logger  = require( "./logger.js" );
var logger  = new Logger();
logger.set_use_timestamp( true );
logger.set_file_path( "log.txt" );
var scriptName = "Currency";

function Currency( league ) {
    this.league = league;
    this.currencies = [
        "alt", "fuse", "alch", "chaos", "prism", "exa", "chrome", "jew", 
        "chance", "chisel", "scouring", "bless", "regret", "regal", "divine", 
        "vaal", "wisdom", "portal", "armour", "stone", "bauble", "trans", "aug", 
        "mirror", "eternal"
    ];
    this.online = true; // Should we search for online offers only or both
    // Fixed prices at vendors 
    this.vendorCurrencyValues = [
        { "type": "alch", "price": 64, "location": 3 },
        { "type": "regret", "price": 64, "location": 2 },
        { "type": "scouring", "price": 32, "location": 2 },
        { "type": "chance", "price": 8, "location": 3 },
        { "type": "fuse", "price": 8, "location": 2 },
        { "type": "chrome", "price": 6, "location": 2 },
        { "type": "jew", "price": 2, "location": 2 },
        { "type": "alt", "price": 1, "location": 1 },
        { "type": "aug", "price": 1/4, "location": 1 },
        { "type": "trans", "price": 1/16, "location": 1 },
        { "type": "portal", "price": 1/112, "location": 1 },
    ];
    // Fixed prices at masters
    this.masterCurrencyValues = [
        { 
            "type": "alt", 
            "price": ( 64 / 4 ) / 20,
            "vendor": "Haku", 
            "amount": 20 
        },
        { 
            "type": "jew", 
            "price": 32 / 20, 
            "vendor": "Elreon", 
            "amount": 20 
        },
        { 
            "type": "scouring", 
            "price": ( 96 * 8 ) / ( 30 * 32 ) * 32, 
            "vendor": "catarina", 
            "amount": 30 
        },
        { 
            "type": "chrome", 
            "price": ( 48 * 2 ) / ( 20 * 6 ) * 6, 
            "vendor": "Tora", 
            "amount": 20 
        },
        { 
            "type": "fuse", 
            "price": ( 64 * 2 ) / ( 20 * 8 ) * 8, 
            "vendor": "Vorici", 
            "amount": 20 
        },
        { 
            "type": "regret", 
            "price": ( 64 * 32 ) / ( 40 * 64 ) * 64, 
            "vendor": "Leo", 
            "amount": 40 
        },
        { 
            "type": "alch", 
            "price": ( 8 * 64 ) / ( 10 * 64 ) * 64, 
            "vendor": "Vagan", 
            "amount": 10 
        },
    ];

    Currency.prototype.searchOnline = function( online ) {
        this.online = online;
    };

    Currency.prototype.getCurrencyIndex = function( currency ) {
        return this.currencies.indexOf( currency ) + 1;
    };

    Currency.prototype.getAllRates = function( currency, callback ) {
        var rates = [];
        var start = new Date();
        if ( this.currencies.indexOf( currency ) === -1 ) {
            logger.log( "Undefined currency: " + currency, scriptName, "e" );
            callback( null );
        }
        var that = this;
        // For each currency, if the currency is different from the base one
        async.eachLimit( this.currencies, 1, function( current, callbackCurrency ) {
            if ( current !== currency ) {
                that.getRate( current, currency, 1, function( value ) {
                    rates.push( value );
                    callbackCurrency();
                });
            } else {
                callbackCurrency();
            }
        }, function( err ) {
            if ( err ) {
                logger.log( err, scriptName, "e" );
            }
            logger.log( "done (" + (new Date() - start) + "ms)", scriptName );
            setTimeout( function(){
                callback({
                    timestamp: Date.now(),
                    league: league,
                    sell: currency,
                    rates: rates
                });
            }, 1000 );
        });
    };

    Currency.prototype.getRate = function( buying, selling, amount, callback ) {

        if ( buying === selling ) {
            callback({
                "buy": buying,
                "ratios": [ 1 ],
                "avg": 1,
                "median": 1,
                "mode": 1,
                "min": 1,
                "max": 1 
            });
        }

        var that = this;
        /**
         * Returns the median value of an array
         *
         * Sort array, then return its median
         * @param array
         * @return median value
         */
        var median = function( values ) {
            values.sort( function( a, b ) { return a - b; });
            if ( values.length % 2 === 1 ) {
                return values[( values.length - 1 ) / 2];
            } else {
                return ( values[( values.length / 2 - 1 )] + 
                        values[values.length / 2]) / 2;
            }
        };

        var mode = function( values, cb ) {
            var modes = {};
            // Count amount of each ratio
            async.each( values, function( value, cbMode ) {
                if ( modes[value] ) {
                    modes[value]++;
                } else {
                    modes[value] = 1;
                }
                cbMode();
            }, function( err ) {
                if ( err ) {
                    logger.log( err, scriptName, "e" );
                }
                // Iterate over object keys
                var maxValue = 0;
                var maxRatio = 0;
                for ( var property in modes ) {
                    if ( modes.hasOwnProperty( property )) {
                        if ( modes[property] > maxValue ) {
                            maxValue = modes[property];
                            maxRatio = property;
                        }
                    }
                }
                cb( parseFloat( maxRatio ));
            });
        };

        var ratios = [];

        /* Check if buying and selling currencies are referenced in currencies 
           object. Just to make sure to do the proper mapping with poe.trade. */
        var buyingIndex  = that.getCurrencyIndex( buying );
        var sellingIndex = that.getCurrencyIndex( selling );
        // If not referenced, print error and return 0 value
        if ( buyingIndex === -1 || sellingIndex === -1 ) {
            logger.log( "Unknown currencies: " + selling + " or " + buying,
                        scriptName, "e" );
            callback({
                "buy": buying,
                "ratios": ratios,
                "avg": -1,
                "median": -1,
                "mode": -1,
                "min": -1,
                "max": -1 
            });
        }

        // Should we search online
        var searchOnline;
        if ( this.online ) {
            searchOnline = "&online=x";
        } else {
            searchOnline = "";
        }

        var url = "http://currency.poe.trade/search?league=" + league + "&want=" + 
                  buyingIndex + "&have=" + sellingIndex + "" + searchOnline;
        request({ "url": url, "gzip": true },
            function( error, response, body ) {
                if ( error ) {
                    logger.log( "Error occured: " + error, scriptName, "e" );
                    // Retry in case of failure
                    setTimeout( that.getRate, 1000, buying, selling, amount, callback );
                } else {
                    var min = 99999999;
                    var max = -1;
                    var values = [];
                    var mod = -1;

                    $( 'body' ).html( body );

                    /* Do the scraping and store buy/sell ratios as well as min
                        and max ratios */
                    $( "div.displayoffer-middle" ).each( function() { 
                        var splitted = $( this ).text().split( " ⇐ " ); 
                        var ratio = splitted[0] / splitted[1];
                        if ( ratio > max ) {
                            max = ratio;
                        }
                        if ( ratio < min ) {
                            min = ratio;
                        }
                        values.push( ratio );
                    });
                    ratios = values;
                    
                    // Compute median
                    var med = median( values );

                    // Compute mode
                    mode( values, function( res ) {
                        mod = res;

                        // Sum all ratios together
                        var sum = values.reduce( 
                            function( previous, current, index, array ) {
                                return previous + current;
                            }, 0 
                        );
                        
                        /* If we have a least a value, compute average ratio by dividing 
                            the sum by the amount of ratios */
                        if ( values.length !== 0 ) {
                            var avg = sum / values.length;
                            // return average ratio value trimmed to 4 numbers precision
                            callback({
                                "buy": buying,
                                "ratios": ratios,
                                "avg": avg.toFixed( 4 ) * amount,
                                "median": med.toFixed( 4 ) * amount,
                                "mode": mod.toFixed( 4 ) * amount,
                                "min": min.toFixed( 4 ) * amount,
                                "max": max.toFixed( 4 ) * amount
                            });
                        /* If there is no ratio, then it means we don't have any offers
                            between these two currencies. Print error message and return 
                            0 */
                        } else {
                            // logger.log( "getRate: No offers for these currencies: " + 
                            //             buying + " <- " + selling,
                            //             scriptName, "w" );
                            callback({
                                "buy": buying,
                                "ratios": ratios,
                                "avg": -1,
                                "median": -1,
                                "mode": -1,
                                "min": -1,
                                "max": -1
                            });
                        }
                    });
                }
            }
        );
    };
}

module.exports = Currency;