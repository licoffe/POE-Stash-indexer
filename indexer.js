// Requirements
var async            = require( "async" );
var request          = require( "request" );
var http             = require( "http" ).createServer().listen( 3000, '127.0.0.1' );
var io               = require( "socket.io" ).listen( http );
var Logger           = require( "./modules/logger.js" );
var logger           = new Logger();
logger.set_use_timestamp( true );
logger.set_file_path( "./log.txt" );
var page             = "http://www.pathofexile.com/api/public-stash-tabs";
// Variables that can be tweaked
var downloadInterval = 0; // Time between downloads in seconds
var mongo_client     = require( "mongodb" ).MongoClient;
// MongoDB vars
var script_name      = "Indexer";
var interrupt        = false;
var debug            = false;
var stashCollection  = "stashes";
var insertionError   = 0;
var added            = 0;
var updated          = 0;
var removed          = 0;
var startTime        = Date.now();

io.on( 'connection', function( socket ) {
    logger.log( "Received connection", script_name, "e" );
    socket.emit( 'an event', { some: 'data' });
});

/**
 * Return the next chunk ID to download from last downloaded chunk file
 *
 * @param Mongo database handler
 * @return Next chunk ID
 */
var lastDownloadedChunk = function( db, callback ) {
    var entries = [];
    var cursor = db.collection( 'chunk_id' ).find().sort({$natural:-1}).limit(1);
    if ( cursor !== undefined ) {
        logger.log( "Last chunk ID found", script_name );
        cursor.each( function( err, doc ) {
            if ( doc ) {
                entries.push( doc );
            } else {
                logger.log( "Found " + entries.length + " entries", script_name );
                cursor.close();
                callback( entries );
            }
        });
    } else {
        logger.log( "There was an issue while querying for last chunk ID",
                    script_name, "e" );
    }
};

/**
 * Return items associated to input stash ID
 *
 * @param Mongo database handler, stashID
 * @return items included
 */
var getStashByID = function( db, stashID, callback ) {
    var entries = [];
    var cursor = db.collection( stashCollection ).find({ "stashID": stashID });
    if ( cursor !== undefined ) {
        cursor.each( function( err, doc ) {
            if ( err ) {
                logger.log( "getStashByID: " + err, script_name, "e" );
            }
            if ( doc ) {
                entries.push( doc );
            } else {
                cursor.close();
                callback( entries );
            }
        });
    } else {
        logger.log( "No such stash ID: " + stashID,
                    script_name, "e" );
    }
};

/**
 * Converts a second amount to a higer unit (min, hour, day...) if possible.
 *
 * @param Second amount to convert
 * @return JSON object with the converted value and corresponding unit
 */
var secToNsec = function( secAmount ) {
    var units = [ "ms", "sec", "min", "hour", "day", "week", "month", "year" ];
    var counter = 0;
    if ( secAmount > 1000 ) {
        secAmount /= 1000; // sec
        counter++;
        if ( secAmount > 60 ) {
            secAmount /= 60; // minutes
            counter++;
            if ( secAmount > 60 ) {
                secAmount /= 60; // hours
                counter++;
                if ( secAmount > 24 ) {
                    secAmount /= 24; // days
                    counter++;
                    if ( secAmount > 365 ) {
                        secAmount /= 365; // years
                        counter = 6;
                    } else if ( secAmount > 30 ) {
                        secAmount /= 30; // month
                        counter = 5;
                    } else if ( secAmount > 7 ) {
                        secAmount /= 7; // weeks
                        counter++;
                    }
                }
            }
        }
    }
    return { "amount": secAmount, "unit": units[counter]};
};

/**
 * Computes the amount of links and the socket colors of an item
 *
 * @param item data, callback
 * @return pass the amount to callback
 */
var getLinksAmountAndColor = function( item, callback ) {
    var groups      = {};
    var groupColors = {};
    var colors      = [];
    // FOr each sockets in the item
    async.each( item.sockets, function( socket, cb ) {
        // If we have a new socket group
        if ( !groups[socket.group] ) {
            groups[socket.group] = 1;
            groupColors[socket.group] = [socket.attr];
        // Otherwise, add a new socket to this group
        } else {
            groups[socket.group]++;
            groupColors[socket.group].push( socket.attr );
        }
        colors.push( socket.attr );
        cb();
    }, function( err ) {
        var linkAmount = 0;
        var linkColors = [];
        // Extract largest group
        for ( var key in groups ) {
            if ( groups.hasOwnProperty( key )) {
                if ( groups[key] > linkAmount ) {
                    linkAmount = groups[key];
                    linkColors = groupColors[key];
                }
            }
        }
        callback({ "linkAmount": linkAmount, "colors": colors, "linkedColors": linkColors });
    });
};

/**
 * Compare two arrays (old and new) and return an object containing an array
 * of removed, added and common elements to the second array.
 *
 * @param old and new arrays + callback
 * @return return object containing removed, added and common elements
 */
var compareArrays = function( old, young, cb ) {
    var removed    = [];
    var added      = [];
    var common     = [];
    var discovered = {};

    // For each item in old array, check if this item is in the new array
    async.each( old, function( itemOld, cbOld ) {
        var found = false;
        async.each( young, function( itemYoung, cbYoung ) {
            // If we have an item in the new array with the same id, then it is
            if ( itemYoung.id === itemOld.id ) {
                if ( !discovered[itemYoung.id] ) {
                    discovered[itemYoung.id] = 1;
                    if ( itemYoung.note ) {
                        itemOld.note = itemYoung.note;
                    }
                }
                found = true;
                cbYoung({error:"breakAlready"});
            } else {
                cbYoung();
            }
        }, function( err ) {
            if ( err && err.error !== "breakAlready" ) {
                logger.log( "compareArrays: " + err, script_name, "e" );
            }
            if ( found ) {
                common.push( itemOld );
                cbOld();
            } else {
                removed.push( itemOld );
                cbOld();
            }
        });
    }, function( err ) {
        if ( err ) {
            logger.log( "compareArrays: " + err, script_name, "e" );
        }
        async.each( young, function( itemYoung, cbYoung ) {
            if ( !discovered[itemYoung.id]) {
                added.push( itemYoung );
            }
            cbYoung();
        }, function( err ) {
            if ( err ) {
                logger.log( "compareArrays: " + err, script_name, "e" );
            }
            cb({
                "removed": removed,
                "added": added,
                "common": common
            });
        });
    });
};

/**
 * Extract mods with their values from input item
 *
 * Extract implicit, explicit, crafted and enchanted mods from item.
 * @param item from stash API, callback
 * @return Pass four arrays to callback with extracted mods
 */
var parseMods = function( item, callback ) {
    var parsedExplicitMods  = [];
    var parsedImplicitMods  = [];
    var parsedCraftedMods   = [];
    var parsedEnchantedMods = [];
    // Parse explicit mods
    async.each( item.explicitMods, function( mod, cbMod ) {
        var re = /([0-9.]+)/g;
        var match = re.exec( mod );
        var matches = [];
        while ( match !== null ) {
            matches.push( parseFloat( match[1]));
            match = re.exec( mod );
        }
        mod = mod.replace( re, "#" );
        parsedExplicitMods.push({
            "mod": mod,
            "values": matches
        });
        cbMod();
    }, function( err ) {
        if ( err ) {
            logger.log( "Error: " + err, script_name, "w" );
        }
    });
    // Parse implicit mods
    async.each( item.implicitMods, function( mod, cbMod ) {
        var re = /([0-9.]+)/g;
        var match = re.exec( mod );
        var matches = [];
        while ( match !== null ) {
            matches.push( parseFloat( match[1]));
            match = re.exec( mod );
        }
        mod = mod.replace( re, "#" );
        parsedImplicitMods.push({
            "mod": mod,
            "values": matches
        });
        cbMod();
    }, function( err ) {
        if ( err ) {
            logger.log( "Error: " + err, script_name, "w" );
        }
    });
    // Parse crafted mods
    async.each( item.craftedMods, function( mod, cbMod ) {
        var re = /([0-9.]+)/g;
        var match = re.exec( mod );
        var matches = [];
        while ( match !== null ) {
            matches.push( parseFloat( match[1]));
            match = re.exec( mod );
        }
        mod = mod.replace( re, "#" );
        parsedCraftedMods.push({
            "mod": mod,
            "values": matches
        });
        cbMod();
    }, function( err ) {
        if ( err ) {
            logger.log( "Error: " + err, script_name, "w" );
        }
    });
    // Parse enchanted mods
    async.each( item.enchantMods, function( mod, cbMod ) {
        var re = /([0-9.]+)/g;
        var match = re.exec( mod );
        var matches = [];
        while ( match !== null ) {
            matches.push( parseFloat( match[1]));
            match = re.exec( mod );
        }
        mod = mod.replace( re, "#" );
        parsedEnchantedMods.push({
            "mod": mod,
            "values": matches
        });
        cbMod();
    }, function( err ) {
        if ( err ) {
            logger.log( "Error: " + err, script_name, "w" );
        }
    });
    callback( parsedExplicitMods, parsedImplicitMods, 
              parsedCraftedMods, parsedEnchantedMods );
};

/**
 * Download all public stashes starting with input chunk ID.
 *
 * Download chunk from POE stash API using wget command with compression.
 * Extract downloaded data and check if next chunk is available. If yes,
 * recurse with next chunk ID.
 * @param chunk ID to download
 * @return next chunk ID to download
 */
var downloadChunk = function( chunkID, collection, db, callback ) {

    var download = function( chunkID ) {
        // Download compressed gzip data and extract it
        logger.log( "Downloading compressed data[" + chunkID + "]", script_name );
        console.time( "Downloading JSON" );
        request({ "url": page + "?id=" + chunkID, "gzip": true },
            function( error, response, body ) {
                if ( error ) {
                    console.timeEnd( "Downloading JSON" );
                    logger.log( "Error occured, retrying: " + error, script_name, "e" );
                    setTimeout(download, downloadInterval, chunkID );
                } else {
                    logger.log( "Downloaded and extracted", script_name );
                    console.timeEnd( "Downloading JSON" );
                    loadJSON( body );
                }
            }
        );
    };

    var loadJSON = function( data ) {
        try {
            console.time( "Parsing JSON" );
            data = JSON.parse( data, 'utf8' );
            console.timeEnd( "Parsing JSON" );
            logger.log( "Data loaded", script_name );
            // If we reached the top and next_change_id is null
            if ( !data.next_change_id ) {
                logger.log( "Top reached, waiting", script_name );
                setTimeout( download, 2, chunkID );
            } else {
                parseData( data, chunkID );
            }
        } catch ( e ) {
            logger.log( "Error occured, retrying: " + e, script_name, "e" );
            setTimeout( download, downloadInterval, chunkID );
        }
    };

    var parseData = function( data ) {
        // Store last chunk ID
        db.createCollection( 'chunk_id', function( err, chunk_collection ) {
            if ( err ) {
                logger.log( "There was an error creating the collection: " + err, script_name, "e" );
                db.close();
            } else {
                logger.log( "Adding chunk ID to DB", script_name );
                chunk_collection.insert(
                    { "next_chunk_id" : data.next_change_id }, { w : 1 }, function( err, result ) {
                    if ( err ) {
                        logger.log( "There was an error inserting chunk_id value: " + err, script_name, "w" );
                    }
                    logger.log( "Reading data file", script_name );
                    console.time( "Loading data into DB" );
                    // For each stashes in the new data file
                    async.each( data.stashes, function( stash, callbackStash ) {
                        // If stash is updated, the account is online
                        db.createCollection( 'online_status', function( err, onlineCollection ) {
                            if ( err ) {
                                logger.log( "Online collection error: " + err, script_name, "w" );
                            }
                            var onlineStatus = {
                                "accountName": stash.accountName,
                                "lastSeen": Date.now()
                            };
                            onlineCollection.update(
                                { "accountName": stash.accountName },
                                onlineStatus,
                                { "upsert": true, "multi": false },
                                function( err, result ) {
                                if ( err ) {
                                    logger.log( "Online collection: There was an error inserting value: " + err, script_name, "w" );
                                    insertionError++;
                                }
                            });
                        });

                        // Get previously stored stash contents
                        getStashByID( db, stash.id, function( results ) {
                            // If the stash does not exist, store all items
                            if ( results.length === 0 ) {
                                logger.log( "Stash " + stash.id + " does not exist, creating it", script_name, "", true );
                                logger.log( "Stash contains " + stash.items.length + " items", script_name, "", true );
                                console.time( "DB insertion" );
                                async.each( stash.items, function( item, cb ) {

                                    parseMods( item, function( explicit, implicit, crafted, enchanted ) {
                                        item.accountName = stash.accountName;
                                        item.lastCharacterName = stash.lastCharacterName;
                                        item.stashID      = stash.id;
                                        item.stashName    = stash.stash;
                                        item.stashType    = stash.stashType;
                                        item.publicStash  = stash.public;
                                        item.socketAmount = item.sockets.length;
                                        item._id          = item.id;
                                        item.available    = true;
                                        item.addedTs      = Date.now();
                                        item.updatedTs    = Date.now();
                                        item.parsedImplicitMods  = implicit;
                                        item.parsedExplicitMods  = explicit;
                                        item.parsedCraftedMods   = crafted;
                                        item.parsedEnchantedMods = enchanted;
                                        getLinksAmountAndColor( item, function( res ) {
                                            item.linkAmount   = res.linkAmount;
                                            item.colors       = res.colors;
                                            item.linkedColors = res.linkedColors;
                                            // Store this item
                                            collection.save( item, function( err, result ) {
                                                if ( err ) {
                                                    logger.log( "New stash: There was an error inserting value: " + err, script_name, "w" );
                                                    insertionError++;
                                                } else {
                                                    added++;
                                                }
                                                if ( !item.name ) {
                                                    logger.log(
                                                        "Adding new item \x1b[35m" +
                                                        item.typeLine.replace( "<<set:MS>><<set:M>><<set:S>>", "" ) +
                                                        "\x1b[0m to " + stash.id, script_name, "", true );
                                                } else {
                                                    logger.log(
                                                        "Adding new item \x1b[35m" +
                                                        item.name.replace( "<<set:MS>><<set:M>><<set:S>>", "" ) +
                                                        "\x1b[0m to " + stash.id, script_name, "", true );
                                                }

                                                cb();
                                            });
                                        }); 
                                    });
                                }, function( err ) {
                                    if ( err ) {
                                        logger.log( "New stash: There was an error inserting value: " + err, script_name, "w" );
                                    }
                                    console.timeEnd( "DB insertion" );
                                    callbackStash();
                                });
                            // If the stash already exists
                            } else {
                                /* If there are less items in new stash then
                                   there used to be */
                                if ( results.length > stash.items.length ) {
                                    logger.log(
                                        ( results.length - stash.items.length ) +
                                        " items out of " + results.length + " were removed from stash " +
                                        stash.id, script_name, "", true );
                                } else if ( results.length < stash.items.length ) {
                                    logger.log(
                                        ( stash.items.length - results.length ) +
                                        " items were added to the stash " +
                                        stash.id, script_name, "", true );
                                }

                                logger.log( "Updating existing stash " + stash.id, script_name, "", true );
                                /* Check which item has been removed, added or
                                   kept */
                                compareArrays( results, stash.items, function( res ) {
                                    logger.log( res.added.length + " items added", script_name, "", true );
                                    logger.log( res.removed.length + " items removed", script_name, "", true );
                                    logger.log( res.common.length + " items to update", script_name, "", true );
                                    // For each removed item
                                    async.each( res.removed, function( removedItem, cbRemoved ) {
                                        parseMods( removedItem, function( explicit, implicit, crafted, enchanted ) {
                                            removedItem.parsedImplicitMods  = implicit;
                                            removedItem.parsedExplicitMods  = explicit;
                                            removedItem.parsedCraftedMods   = crafted;
                                            removedItem.parsedEnchantedMods = enchanted;
                                            removedItem.socketAmount        = removedItem.sockets.length;
                                            // Set item status to unavailable
                                            logger.log( removedItem.id + " removed", script_name, "", true );
                                            removedItem.available = false;
                                            getLinksAmountAndColor( removedItem, function( res ) {
                                                removedItem.linkAmount        = res.linkAmount;
                                                removedItem.colors            = res.colors;
                                                removedItem.linkedColors      = res.linkedColors;
                                                // Update status in DB
                                                collection.save( removedItem, function( err, result ) {
                                                    if ( err ) {
                                                        logger.log(
                                                            "Stash update -> unavailable: There was an error inserting value: " + err,
                                                            script_name, "w" );
                                                        insertionError++;
                                                    } else {
                                                        if ( !removedItem.name ) {
                                                            logger.log(
                                                                "Removing item \x1b[35m" +
                                                                removedItem.typeLine.replace( "<<set:MS>><<set:M>><<set:S>>", "" ) +
                                                                "\x1b[0m to " + stash.id, script_name, "", true );
                                                        } else {
                                                            logger.log(
                                                                "Removing item \x1b[35m" +
                                                                removedItem.name.replace( "<<set:MS>><<set:M>><<set:S>>", "" ) +
                                                                "\x1b[0m to " + stash.id, script_name, "", true );
                                                        }
                                                        removed++;
                                                    }
                                                    cbRemoved();
                                                });
                                            });
                                        });
                                    }, function( err ) {
                                        if ( err ) {
                                            logger.log( "parseData: " + err, script_name, "e" );
                                        }
                                    });
                                    // For each item added
                                    async.each( res.added, function( addedItem, cbAdded ) {
                                        logger.log( addedItem.id + " added", script_name, "", true );
                                        parseMods( addedItem, function( explicit, implicit, crafted, enchanted ) {
                                            addedItem.accountName  = stash.accountName;
                                            addedItem.stashID      = stash.id;
                                            addedItem.stashName    = stash.stash;
                                            addedItem.stashType    = stash.stashType;
                                            addedItem.publicStash  = stash.public;
                                            addedItem.socketAmount = addedItem.sockets.length;
                                            addedItem._id          = addedItem.id;
                                            addedItem.available    = true;
                                            addedItem.addedTs      = Date.now();
                                            addedItem.updatedTs    = Date.now();
                                            addedItem.lastCharacterName = stash.lastCharacterName;
                                            addedItem.parsedImplicitMods  = implicit;
                                            addedItem.parsedExplicitMods  = explicit;
                                            addedItem.parsedCraftedMods   = crafted;
                                            addedItem.parsedEnchantedMods = enchanted;
                                            getLinksAmountAndColor( addedItem, function( res ) {
                                                addedItem.linkAmount   = res.linkAmount;
                                                addedItem.colors       = res.colors;
                                                addedItem.linkedColors = res.linkedColors;
                                                // Store this item
                                                collection.save( addedItem, function( err, result ) {
                                                    if ( err ) {
                                                        logger.log( "Stash update -> added: There was an error inserting value: " + err, script_name, "w" );
                                                        insertionError++;
                                                    } else {
                                                        added++;
                                                    }
                                                    if ( !addedItem.name ) {
                                                        logger.log(
                                                            "Adding new item \x1b[35m" +
                                                            addedItem.typeLine.replace( "<<set:MS>><<set:M>><<set:S>>", "" ) +
                                                            "\x1b[0m to " + stash.id, script_name, "", true );
                                                    } else {
                                                        logger.log(
                                                            "Adding new item \x1b[35m" +
                                                            addedItem.name.replace( "<<set:MS>><<set:M>><<set:S>>", "" ) +
                                                            "\x1b[0m to " + stash.id, script_name, "", true );
                                                    }
                                                    cbAdded();
                                                });
                                            });
                                        });
                                    }, function( err ) {
                                        if ( err ) {
                                            logger.log( err, script_name, "e" );
                                        }
                                    });
                                    // For each item kept
                                    async.each( res.common, function( commonItem, cbCommon ) {
                                        logger.log( commonItem.id + " updated", script_name, "", true );
                                        parseMods( commonItem, function( explicit, implicit, crafted, enchanted ) {
                                            commonItem.parsedImplicitMods  = implicit;
                                            commonItem.parsedExplicitMods  = explicit;
                                            commonItem.parsedCraftedMods   = crafted;
                                            commonItem.parsedEnchantedMods = enchanted;
                                            commonItem.socketAmount        = commonItem.sockets.length;
                                            // Update its update timestamp
                                            commonItem.updatedTs = Date.now();
                                            getLinksAmountAndColor( commonItem, function( res ) {
                                                commonItem.linkAmount   = res.linkAmount;
                                                commonItem.colors       = res.colors;
                                                commonItem.linkedColors = res.linkedColors;
                                                // Store this item
                                                collection.save( commonItem, function( err, result ) {
                                                    if ( err ) {
                                                        logger.log( "Stash update -> kept: There was an error inserting value: " + err, script_name, "w" );
                                                        insertionError++;
                                                    } else {
                                                        if ( !commonItem.name ) {
                                                            logger.log(
                                                                "Updating item \x1b[35m" +
                                                                commonItem.typeLine.replace( "<<set:MS>><<set:M>><<set:S>>", "" ) +
                                                                "\x1b[0m to " + stash.id, script_name, "", true );
                                                        } else {
                                                            logger.log(
                                                                "Updating item \x1b[35m" +
                                                                commonItem.name.replace( "<<set:MS>><<set:M>><<set:S>>", "" ) +
                                                                "\x1b[0m to " + stash.id, script_name, "", true );
                                                        }
                                                        updated++;
                                                    }
                                                    cbCommon();
                                                });
                                            });
                                        });
                                    }, function( err ) {
                                        if ( err ) {
                                            logger.log( err, script_name, "e" );
                                        }
                                        callbackStash();
                                    });
                                });
                            }
                        });
                    }, function( err ) {
                        if ( err ) {
                            logger.log( err, script_name, "e" );
                        }
                        console.timeEnd( "Loading data into DB" );
                        done( data );
                    });
                });
            }
        });
    };

    var done = function( data ) {
        var nextID = data.next_change_id;
        logger.log( "Next ID: " + nextID, script_name );

        if ( interrupt ) {
            logger.log( "Exiting", script_name );
            db.close();
            process.exit( 0 );
        } else {
            /* Sleep n seconds and call the script on the
               next chunk ID */
            var elapsed = secToNsec( Date.now() - startTime );
            var speed   = ( added + removed + updated ) /
                          (( Date.now() - startTime ) / 1000 ); // insert per sec
            logger.log( "Entries added: " + added +
                        ", removed: " + removed +
                        ", updated: " + updated +
                        ", insert errors: " + insertionError +
                        " over " + Math.round( elapsed.amount ) +
                        " " + elapsed.unit +
                        " at " + Math.round( speed ) +
                        " insert/sec", script_name );
            logger.log( "Sleeping " + downloadInterval + "ms", script_name );
            setTimeout( callback, downloadInterval,
                        nextID, collection, db, callback );
        }
    };

    download( chunkID );
};

/**
 * Connect to MongoDB. If successfull, run provided callback function
 *
 * @param Callback function
 * @return None
 */
function connectToDB( callback ) {

    // Read config file
    logger.log( "Reading config file", script_name );
    var config = require( "./config.json" );

    // Connect to the db
    mongo_client.connect( "mongodb://" + config.dbAddress + ":" + config.dbPort + "/" + config.dbName,
                         function( err, db ) {
        if ( err ) {
            logger.log( err, script_name, "e" );
            logger.log( "Make sure MongoDB has been started", script_name, "e" );
            process.exit(0);
        }
        logger.log( "Connected to MongoDB", script_name );
        if ( config.authenticate ) {
            db.authenticate( config.user, config.pass, function( err, res ) {
                if ( err ) {
                    logger.log( err, script_name, "e" );
                    process.exit(0);
                }
                logger.log( "Logged in " + config.dbName, script_name );
                callback( db );
            });
        } else {
            logger.log( "Logged in " + config.dbName, script_name );
            callback( db );
        }
    });
}

// Main loop
function main() {

    // Parse argv
    process.argv.forEach(( val, index ) => {
        if ( val === "-d" ) {
            logger.log( "Activating debug", script_name, "e" );
            debug = true;
        }
    });

    if ( debug ) {
        // write to log.txt
        logger.set_use_file( true );
    }
    connectToDB( function( db ) {
        logger.log( "Attempting to create or reuse POE_price collection", script_name );
        // Create online status index
        db.createCollection('online_status', function( err, collection ){
            collection.createIndex({ "accountName": 1 }, { "unique": true });
        });
        db.createCollection( stashCollection, function( err, collection ) {
            logger.log( "Checking indexes have been generated", script_name );
            var indexFields = [
                { "name": 1 },
                { "explicitMods": 1 },
                { "accountName": 1 },
                { "id": 1 },
                { "properties": 1 },
                { "implicitMods": 1 },
                { "league": 1 },
                { "typeLine": 1 },
                { "identified": 1 },
                { "corrupted": 1 },
                { "stashName": 1 },
                { "frameType": 1 },
                { "lastCharacterName": 1 },
                { "craftedMods": 1 },
                { "enchantMods": 1 },
                { "stashID": 1 },
                { "available": 1 },
                { "ilvl": 1 },
                { "addedTs": 1 },
                { "socketAmount": 1 },
                { "linkAmount": 1 },
                { "colors": 1 },
                { "linkedColors": 1 },
                { "parsedImplicitMods.mod": 1 },
                { "parsedExplicitMods.mod": 1 },
                { "parsedCraftedMods.mod": 1 },
                { "parsedEnchantedMods.mod": 1 },
                { "name": 1, "socketAmount": 1 },
                { "name": 1, "socketAmount": 1, "linkAmount": 1 },
                { "parsedExplicitMods.mod": 1, "socketAmount": 1, "linkAmount": 1 }
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

                // Check last downloaded chunk ID
                lastDownloadedChunk( db, function( entry ) {
                    try {
                        logger.log( "Next chunk ID: " + entry[0].next_chunk_id, script_name );
                        downloadChunk( entry[0].next_chunk_id, collection, db, downloadChunk );
                    } catch ( e ) {
                        logger.log( "Starting new indexation", script_name, "w" );
                        // Should create indexes here
                        downloadChunk( "", collection, db, downloadChunk );
                   }
                });
            }, err => {
                logger.log( "There was an error creating the collection: " + err, script_name, "e" );
            });
        });
    });
}

process.on('SIGINT', function() {
    logger.log( "\rCaught interrupt signal, exiting gracefully", script_name, "e" );
    interrupt = true;
});

// process.on('uncaughtException', (err) => {
//     logger.log( "Caught exception: " + err, script_name, "e" );
// });

main();
