// Requirements
var async            = require( "async" );
var request          = require( "request" );
var Logger           = require( "./modules/logger.js" );
var logger           = new Logger();
logger.set_use_timestamp( true );
logger.set_file_path( "./log.txt" );
var page             = "http://www.pathofexile.com/api/public-stash-tabs";
// Variables that can be tweaked
var downloadInterval = 0; // Time between downloads in seconds
var mongo_client     = require( "mongodb" ).MongoClient;
// MongoDB vars
var address          = "localhost";
var port             = 27017;
var database         = "POE_price";
var script_name      = "Indexer";
var interrupt        = false;
var debug            = false;
var stashCollection  = "stashes";
var insertionError   = 0;
var added            = 0;
var updated          = 0;
var removed          = 0;
var startTime        = Date.now();

/**
 * Return the next chunk ID to download from last downloaded chunk file
 * 
 * @params Mongo database handler
 * @return Next chunk ID
 */
var lastDownloadedChunk = function( db, callback ) {
    var entries = [];
    var cursor = db.collection('chunk_id').find().sort({$natural:-1}).limit(1);
    if ( cursor !== undefined ) {
        logger.log( "Last chunk ID found", script_name );
        cursor.each( function( err, doc ) {
            if ( doc ) {
                entries.push( doc );
            } else {
                logger.log( "Found " + entries.length + " entries", script_name );
                callback( entries );
            }
        });
    } else {
        logger.log( "There was an issue while querying for last chunk ID", 
                    script_name, "e" );
    }
}

/**
 * Return items associated to input stash ID
 *
 * @params Mongo database handler, stashID
 * @return items included
 */
var getStashByID = function( db, stashID, callback ) {
    var entries = [];
    var cursor = db.collection(stashCollection).find({ "stashID": stashID });
    if ( cursor !== undefined ) {
        cursor.each( function( err, doc ) {
            if ( err ) {
                logger.log( "getStashByID: " + err, script_name, "e" );
            }
            if ( doc ) {
                entries.push( doc );
            } else {
                callback( entries );
            }
        });
    } else {
        logger.log( "No such stash ID: " + stashID, 
                    script_name, "e" );
    }
}

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
}

/**
 * Compare two arrays (old and new) and return an object containing an array
 * of removed, added and common elements to the second array. 
 *
 * @params old and new arrays + callback
 * @return return object containing removed, added and common elements
 */
var compareArrays = function( old, young, cb ) {
    var removed = [];
    var added   = [];
    var common  = [];
    var discovered = {};

    async.each( old, function( itemOld, cbOld ) {
        var found = false;
        async.each( young, function( itemYoung, cbYoung ) {
            if ( itemYoung.id === itemOld.id ) {
                if ( !discovered[itemYoung.id] ) {
                    discovered[itemYoung.id] = 1;
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
}

/**
 * Download all public stashes starting with input chunk ID.
 * 
 * Download chunk from POE stash API using wget command with compression.
 * Extract downloaded data and check if next chunk is available. If yes,
 * recurse with next chunk ID.
 * @params chunk ID to download
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
    }

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
    }

    var parseData = function( data ) {
        // Store last chunk ID
        db.createCollection( 'chunk_id', function( err, chunk_collection ) {
            if ( err ) {
                logger.log( "There was an error creating the collection: " + err, script_name, "e" );
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
                                true, false,
                                function( err, result ) {
                                if ( err ) {
                                    // logger.log( "Online collection: There was an error inserting value: " + err, script_name, "w" );
                                }
                            });
                        });

                        // Get previously stored stash contents
                        getStashByID( db, stash.id, function( results ) {
                            // If the stash does not exist, store all items
                            if ( results.length === 0 ) {
                                logger.log( "Stash " + stash.id + " does not exist, creating it", script_name, "", true );
                                logger.log( "Stash contains " + stash.items.length + " items", script_name, "", true );

                                async.each( stash.items, function( item, cb ) {
                                    item.accountName = stash.accountName;
                                    item.lastCharacterName = stash.lastCharacterName;
                                    item.stashID     = stash.id;
                                    item.stashName   = stash.stash;
                                    item.stashType   = stash.stashType;
                                    item.publicStash = stash.public;
                                    item._id         = item.id;
                                    item.available   = true;
                                    item.addedTs     = Date.now();
                                    item.updatedTs   = Date.now();

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
                                }, function( err ) {
                                    if ( err ) {
                                        logger.log( "New stash: There was an error inserting value: " + err, script_name, "w" );
                                    }
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
                                        // Set item status to unavailable
                                        logger.log( removedItem.id + " removed", script_name, "", true );
                                        removedItem.available = false;
                                        // Update status in DB
                                        collection.save( removedItem, function( err, result ) {
                                            if ( err ) {
                                                logger.log(
                                                    "Stash update: There was an error inserting value: " + err, 
                                                    script_name, "w" );
                                                insertionError++;
                                            } else {
                                                removed++;
                                            }
                                            cbRemoved();
                                        });
                                    }, function( err ) {
                                        if ( err ) {
                                            logger.log( "parseData: " + err, script_name, "e" );
                                        }
                                        // For each item added
                                        async.each( res.added, function( addedItem, cbAdded ) {
                                            logger.log( addedItem.id + " added", script_name, "", true );
                                            addedItem.accountName = stash.accountName;
                                            addedItem.stashID     = stash.id;
                                            addedItem.stashName   = stash.stash;
                                            addedItem.stashType   = stash.stashType;
                                            addedItem.publicStash = stash.public;
                                            addedItem._id         = addedItem.id;
                                            addedItem.available   = true;
                                            addedItem.addedTs     = Date.now();
                                            addedItem.updatedTs   = Date.now();
                                            addedItem.lastCharacterName = stash.lastCharacterName;
                                            // Store this item
                                            collection.save( addedItem, function( err, result ) {
                                                if ( err ) {
                                                    logger.log( "Stash update: There was an error inserting value: " + err, script_name, "w" );
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
                                        }, function( err ) {
                                            if ( err ) {
                                                logger.log( err, script_name, "e" );
                                            }
                                            // For each item kept
                                            async.each( res.common, function( commonItem, cbCommon ) {
                                                logger.log( commonItem.id + " updated", script_name, "", true );
                                                // Update its update timestamp
                                                commonItem.updatedTs = Date.now();
                                                // Store this item
                                                collection.save( commonItem, function( err, result ) {
                                                    if ( err ) {
                                                        logger.log( "Stash update: There was an error inserting value: " + err, script_name, "w" );
                                                        insertionError++;
                                                    } else {
                                                        updated++;
                                                    }
                                                    cbCommon();
                                                });
                                            }, function( err ) {
                                                if ( err ) {
                                                    logger.log( err, script_name, "e" );
                                                }
                                                callbackStash();
                                            });
                                        });
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
    }

    var done = function( data ) {
        var nextID = data.next_change_id;
        logger.log( "Next ID: " + nextID, script_name );
    
        if ( interrupt ) {
            logger.log( "Exiting", script_name );
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
                        " at " + Math.round( speed ) + " insert/sec" 
                        , script_name );
            logger.log( "Sleeping " + downloadInterval + "ms", script_name );
            setTimeout( callback, downloadInterval, 
                        nextID, collection, db, callback );
        }
    }
    
    download( chunkID );
}

/**
 * Connect to MongoDB. If successfull, run provided callback function
 * 
 * @params Callback function
 * @return None
 */
function connectToDB( callback ) {
    // Connect to the db
    mongo_client.connect( "mongodb://" + address + ":" + port + "/" + database, 
                         function( err, db ) {
        if ( err ) {
            logger.log( err, script_name, "e" );
            logger.log( "Make sure MongoDB has been started", script_name, "e" );
            process.exit(0);
        }
        logger.log( "Connected to MongoDB.", script_name );
        
        callback( db );
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
        db.createCollection( stashCollection, function( err, collection ) {
            logger.log( "Checking indexes have been generated", script_name );
            var indexFields = [
                "name",
                "explicitMods",
                "accountName",
                "id",
                "properties",
                "implicitMods",
                "league",
                "typeLine",
                "identified",
                "corrupted",
                "stashName",
                "frameType",
                "lastCharacterName",
                "craftedMods",
                "enchantMods",
                "stashID",
                "available",
                "ilvl",
                "addedTs"
            ];

            function createIndexs(indexFields){
                return indexFields.map(function (key) {
                    return new Promise(function (resolve, reject){
                        var keys = {};
                        keys[key] = 1;
                        collection.createIndex(keys, function () {
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

process.on('uncaughtException', (err) => {
    logger.log( "Caught exception: " + err, script_name, "e" );
});

main();
