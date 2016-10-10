/**
 * Currency module to poll poe.trade for latest currency rates
 *
 */

var async  = require("async");
var jsdom  = require( "jsdom" );
var Logger = require( "./logger.js" );
var logger = new Logger();
logger.set_use_timestamp( true );
logger.set_file_path( "log.txt" );
var scriptName = "currency.js";

function Currency( league ) {
    this.league = league;
    this.currencies = [
        "alt", "fuse", "alch", "chaos", "prism", "exa", "chrome", "jew", 
        "chance", "chisel", "scouring", "bless", "regret", "regal", "divine", 
        "vaal", "wisdom", "portal", "armour", "stone", "bauble", "trans", "aug", 
        "mirror", "eternal"
    ];
    this.onlineSearch = true; // SHould we search for online offers only or both
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
    }

    Currency.prototype.getCurrencyIndex = function( currency ) {
        return this.currencies.indexOf( currency ) + 1;
    }

    Currency.prototype.getAllRates = function( currency, callback ) {
        var rates = {};
        var start = new Date();
        if ( this.currencies.indexOf( currency ) === -1 ) {
            logger.log( "Undefined currency: " + currency, scriptName, "e" );
            callback( null );
        }
        var that = this;
        // For each currency, if the currency is different from the base one
        async.each( this.currencies, function( current, callbackCurrency ) {
            if ( current !== currency ) {
                that.getRate( current, currency, 1, function( value ) {
                    if ( value !== 0 ) {
                        rates[current] = 1 / value;
                    }
                    callbackCurrency();
                });
            } else {
                rates[current] = 1;
                callbackCurrency();
            }
        }, function( err ) {
            if ( err ) {
                logger.log( err, scriptName, "e" );
            }
            logger.log( "Got last rates (" + (new Date() - start) + "ms)", scriptName );
            callback( rates );
        });
    }

    Currency.prototype.getRate = function( buying, selling, amount, callback ) {
        var buyingIndex  = this.getCurrencyIndex( buying );
        var sellingIndex = this.getCurrencyIndex( selling );
    //     console.log( "http://currency.poe.trade/search?league=" + league + "&want=" + 
    //          sellingIndex + "&have=" + buyingIndex + "&online=" + online );
        if ( buyingIndex === -1 || sellingIndex === -1 ) {
            logger.log( "Unknown currencies: " + selling + " or " + buying,
                        scriptName, "e" );
            callback( 0 );
        }

        // Should we search online
        var searchOnline;
        if ( this.online ) {
            searchOnline = "&online=x";
        } else {
            searchOnline = "";
        }

        jsdom.env (
            "http://currency.poe.trade/search?league=" + league + "&want=" + 
            buyingIndex + "&have=" + sellingIndex + "" + searchOnline,
            ["http://code.jquery.com/jquery.js"],
            function ( err, window ) {
                var min = 9999999;
                var max = -1;
                var values = [];

                window.$( "div.displayoffer-middle" ).each( function() { 
                    var splitted = window.$( this ).text().split( " ⇐ " ); 
                    var ratio = splitted[0] / splitted[1]
    //                 console.log( buying + " -> " + selling + ": " + ratio + " " + splitted[0] + "/" + splitted[1] );
                    if ( ratio > max ) {
                        max = ratio;
                    } else if ( ratio < min ) {
                        min = ratio;
                    }
                    values.push( ratio );
                });
                
                var sum = values.reduce( 
                    function( previous, current, index, array ) {
    //                     console.log( "previous:" + previous );
                        return previous + current;
                    }, 0 
                );
                
                if ( values.length !== 0 ) {
    //                 console.log( "sum: " + sum );
                    var avg = sum / values.length;
                    // console.log( 
                    //     buying + " -> " + selling + ": With " + amount + " " + 
                    //     selling + ", you can buy " + avg.toFixed( 4 ) * amount + 
                    //     " " + buying + " (Min: " +  min.toFixed( 4 ) * amount + 
                    //     ", Max: " + max.toFixed( 4 ) * amount + ")" );
                    callback( avg.toFixed( 4 ) * amount );
                } else {
                    logger.log( "getRate: No offers for these currencies: " + 
                                buying + " <- " + selling,
                                scriptName, "w" );
                    callback( 0 );
                }
            }
        );
    }
}

module.exports = Currency;