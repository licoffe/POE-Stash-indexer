#!/usr/local/bin/node

// Requirements
var exec             = require( "child_process" ).exec;
var fs               = require( "fs" );
var async            = require("async");
var Logger           = require( "./modules/logger.js" );
var logger           = new Logger();
logger.set_use_timestamp( true );
logger.set_file_path( "log.txt" );
var page             = "http://www.pathofexile.com/api/public-stash-tabs";
// Variables that can be tweaked
var downloadInterval = 2000; // Time between downloads in seconds
var mongo_client     = require( "mongodb" ).MongoClient;
// MongoDB vars
var address          = "localhost";
var port             = 27017;
var database         = "POE_price";
var script_name      = "Indexer";
var interrupt        = false;


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
            if ( doc !== null ) {
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
    var cursor = db.collection('stashes').find({ "stashID": stashID });
    if ( cursor !== undefined ) {
        cursor.each( function( err, doc ) {
            if ( doc !== null ) {
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
        // Check if data folder exists, create if it doesn't
        try {
            fs.accessSync( "./data", fs.F_OK );
        } catch ( e ) {
            logger.log( "Data folder does not exist, creating", script_name );
            fs.mkdirSync( "./data" );
        }
        logger.log( "Downloading compressed data[" + chunkID + "]", script_name );
        // Download compressed gzip data
        exec( "wget --header='accept-encoding: gzip' " + page + "?id=" + chunkID + 
          " -O ./data/data_" + chunkID + ".gzip", 
          ( error, stdout, stderr ) => {
            if ( error ) {
                logger.log( "Error occured, retrying", script_name, "e" );
                console.error( `exec error: ${error}` );
                setTimeout(download, downloadInterval, chunkID );
            } else {
                logger.log( "Downloaded", script_name );
                // Extract data
                logger.log( "Extracting data", script_name );
                extract( chunkID );
            }
        });
    }

    var extract = function( chunkID ) {
        exec( "gunzip --force -c ./data/data_" + chunkID + 
                ".gzip > ./data/data_" + chunkID + ".json",
            ( error, stdout, stderr ) => {
            // If there is an error with extraction, redownload
            if ( error ) {
                logger.log( "Error occured, retrying", script_name, "e" );
                // console.error( `exec error: ${error}` );
                setTimeout( download, downloadInterval, chunkID );
            } else {
                logger.log( "Extracted, loading data", script_name );
                var data;
                try {
                    data = JSON.parse( fs.readFileSync( 
                        "./data/data_" + chunkID + ".json", 'utf8' ));
                    logger.log( "Data loaded", script_name );
                    parseData( data, chunkID );
                } catch ( e ) {
                    logger.log( e, script_name, "e" );
                    process.exit(0);
                }
            }
        });
    }

    var parseData = function( data, chunkID ) {
        // Store last chunk ID
        db.createCollection( 'chunk_id', function( err, chunk_collection ) {
            if ( err !== null ) {
                logger.log( "There was an error creating the collection: " + err, script_name, "e" );
            } else {
                logger.log( "Adding chunk ID to DB", script_name );
                chunk_collection.insert( { "next_chunk_id" : data.next_change_id }, { w : 1 }, function( err, result ) {
                    if ( err !== null ) {
                        logger.log( "There was an error inserting value: " + err, script_name, "w" );
                    }
                    logger.log( "Reading data file", script_name );
                    console.time( "Loading data into DB" );
                    // For each stashes in the new data file
                    async.each( data.stashes, function( stash, callbackStash ) {
                        // Get previously stored stash contents
                        getStashByID( db, stash.id, function( results ) {
                            // If the stash does not exist
                            if ( results.length === 0 ) {
                                // logger.log( "Stash " + stash.id + " does not exist, creating it", script_name );
                                async.each( stash.items, function( item, cb ) {
                                    item.accountName = stash.accountName;
                                    item.lastCharacterName = stash.lastCharacterName;
                                    item.stashID     = stash.id;
                                    item.stashName   = stash.stash;
                                    item.stashType   = stash.stashType;
                                    item.publicStash = stash.public;
                                    item._id         = item.id;
                                    item.available   = true;
                                    // Store this item
                                    collection.save( item, function( err, result ) {
                                        if ( err !== null ) {
                                            logger.log( "There was an error inserting value: " + err, script_name, "w" );
                                        }
                                        // logger.log( "Adding item to " + stash.id, script_name );
                                        cb();
                                    });
                                }, function( err ) {
                                    if ( err !== null ) {
                                        logger.log( "There was an error inserting value: " + err, script_name, "w" );
                                    }
                                    callbackStash();
                                });
                            } else {
                                // If there are less items in new stash then 
                                // there used to be
                                if ( results.length > stash.items.length ) {
                                    logger.log(
                                        ( results.length - stash.items.length ) + 
                                        " items out of " + results.length + " were removed from stash " + 
                                        stash.id, script_name );
                                }

                                // Find missing item and change its
                                // available status to false
                                // For each item in the old stash
                                async.each( results, function( oldItem, presence ) {
                                    var currentID = oldItem.id;
                                    var found     = false;
                                    // For each item in the new stash
                                    async.each( stash.items, function( item, cb ) {
                                        // If the old item is found in the new items
                                        // set its status to available
                                        if ( item.id === currentID ) {
                                            found = true;
                                            item.accountName = stash.accountName;
                                            item.lastCharacterName = stash.lastCharacterName;
                                            item.stashID     = stash.id;
                                            item.stashName   = stash.stash;
                                            item.stashType   = stash.stashType;
                                            item.publicStash = stash.public;
                                            item._id         = item.id;
                                            item.available   = true;
                                            // Store this item
                                            collection.save( item, function( err, result ) {
                                                if ( err !== null ) {
                                                    logger.log( "There was an error inserting value: " + err, script_name, "w" );
                                                }
                                                cb();
                                            });
                                        } else {
                                            cb();
                                        }
                                    }, function( err ) {
                                        if ( err ) {
                                            logger.log( err, script_name, "e" );
                                        }
                                        // If item was not found, update its status in db
                                        if ( !found ) {
                                            oldItem.available = false;
                                            // logger.log( "Item " + oldItem.id + " no longer available", script_name );
                                            collection.save( oldItem, function( err, result ) {
                                                if ( err !== null ) {
                                                    logger.log( "There was an error inserting value: " + err, script_name, "w" );
                                                }
                                            });
                                        }
                                        // Go to next item
                                        presence();
                                    });
                                }, function( err ) {
                                    if ( err ) {
                                        logger.log( err, script_name, "e" );
                                    }
                                    callbackStash();
                                });
                            }
                        });
                    }, function( err ) {
                        if ( err ) {
                            logger.log( err, script_name, "e" );
                        }
                        console.timeEnd( "Loading data into DB" );
                        done( data, chunkID );
                    });
                });
            }
        });
    }

    var done = function( data, chunkID ) {
        var nextID = data.next_change_id;
        logger.log( "Next ID: " + nextID, script_name );
        // Cleanup by removing downloaded archive
        logger.log( "Cleaning up", script_name );
        fs.unlinkSync( "./data/data_" + chunkID + ".gzip" );
        fs.unlinkSync( "./data/data_" + chunkID + ".json" );
    
        if ( interrupt ) {
            process.exit( 0 );
        } else {
            // Sleep n seconds and call the script on the 
            // next chunk ID
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
        if ( err !== null ) {
            logger.log( err, script_name, "e" );
            logger.log( "Make sure MongoDB has been started", script_name, "e" );
        }
        logger.log( "Connected to MongoDB.", script_name );
        
        callback( db );
    });
}

// Main loop
function main() {
    connectToDB( function( db ) {
        logger.log( "Attempting to create or reuse POE_price collection", script_name );
        db.createCollection( 'stashes', function( err, collection ) {
            if ( err !== null ) {
                logger.log( "There was an error creating the collection: " + err, script_name, "e" );
            } else {
                // Check last downloaded chunk ID
                lastDownloadedChunk( db, function( entry ) {
                    try {
                        logger.log( "Next chunk ID: " + entry[0].next_chunk_id, 
                                    script_name );
                        downloadChunk( entry[0].next_chunk_id, collection, db, 
                                       downloadChunk );
                    } catch ( e ) {
                        logger.log( "Starting new indexation", 
                                    script_name, "w" );
                        // Should create indexes here
                        downloadChunk( "", collection, db, downloadChunk );
                    }
                });
            }
        });
    });
}

process.on('SIGINT', function() {
    logger.log( "Caught interrupt signal, exiting gracefully", script_name, "e" );
    interrupt = true;
});

main();