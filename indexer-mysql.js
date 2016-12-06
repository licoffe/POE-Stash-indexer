// Requirements
var async            = require( "async" );
var request          = require( "request" );
var http             = require( "http" ).createServer().listen( 3000, '127.0.0.1' );
var io               = require( "socket.io" ).listen( http );
var Logger           = require( "./modules/logger.js" );
var config           = require( "./config.json" );
var logger           = new Logger();
logger.set_use_timestamp( true );
logger.set_file_path( "./log.txt" );
var page             = "http://www.pathofexile.com/api/public-stash-tabs";
// Variables that can be tweaked
var downloadInterval = 0; // Time between downloads in seconds
var mysql            = require( "mysql" );
// MongoDB vars
var scriptName       = "Indexer";
var interrupt        = false;
var debug            = false;
var cleanup          = false;
var insertionError   = 0;
var added            = 0;
var updated          = 0;
var removed          = 0;
var startTime        = Date.now();
var pool;
var credentials      = {
    host     : config.dbAddress,
    port     : config.dbPort,
    user     : config.dbUser,
    password : config.dbPass,
    database : config.dbName,
    connectionLimit: 100
};
var modParsingTime = 0;
var addedNew       = 0;
var removedNew     = 0; 
var updatedNew     = 0;
var startInsert;

io.on( 'connection', function( socket ) {
    logger.log( "Received connection", scriptName, "e" );
    socket.emit( 'an event', { some: 'data' });
});

/**
 * Return the next change ID to download from last downloaded chunk file
 *
 * @param Mysql database handler
 * @return Next change ID
 */
var lastDownloadedChunk = function( db, callback ) {
    db.query( "SELECT `nextChangeId` FROM `ChangeId` ORDER BY ID DESC LIMIT 1", function( err, rows ) {
        if ( err ) {
            logger.log( err, scriptName, "e" );
        }
        callback( rows );
    });
};

/**
 * Erase all data from tables
 *
 * Erase all data from the 9 tables used by the indexer
 * @param Mysql database handler, callback
 * @return nothing
 */
var cleanupDB = function( db, callback ) {
    db.beginTransaction( function( err ) {
        if ( err ) {
            logger.log( err, scriptName, "e" );
        }
        logger.log( "Cleaning up 'ChangeId' table" );
        db.query( "DELETE FROM `ChangeId`", function( err, rows ) {
            if ( err ) {
                logger.log( err + ", rolling back", scriptName, "e" );
                return db.rollback( function() {});
            }
            logger.log( "Cleaning up 'Mods' table" );
            db.query( "DELETE FROM `Mods`", function( err, rows ) {
                if ( err ) {
                    logger.log( err + ", rolling back", scriptName, "e" );
                    return db.rollback( function() {});
                }
                logger.log( "Cleaning up 'properties' table" );
                db.query( "DELETE FROM `Properties`", function( err, rows ) {
                    if ( err ) {
                        logger.log( err + ", rolling back", scriptName, "e" );
                        return db.rollback( function() {});
                    }
                    logger.log( "Cleaning up 'requirements' table" );
                    db.query( "DELETE FROM `Requirements`", function( err, rows ) {
                        if ( err ) {
                            logger.log( err + ", rolling back", scriptName, "e" );
                            return db.rollback( function() {});
                        }
                        logger.log( "Cleaning up 'sockets' table" );
                        db.query( "DELETE FROM `Sockets`", function( err, rows ) {
                            if ( err ) {
                                logger.log( err + ", rolling back", scriptName, "e" );
                                return db.rollback( function() {});
                            }
                            logger.log( "Cleaning up 'items' table" );
                            db.query( "DELETE FROM `Items`", function( err, rows ) {
                                if ( err ) {
                                    logger.log( err + ", rolling back", scriptName, "e" );
                                    return db.rollback( function() {});
                                }
                                logger.log( "Cleaning up 'Leagues' table" );
                                db.query( "DELETE FROM `Leagues`", function( err, rows ) {
                                    if ( err ) {
                                        logger.log( err + ", rolling back", scriptName, "e" );
                                        return db.rollback( function() {});
                                    }
                                    logger.log( "Cleaning up 'Accounts' table" );
                                    db.query( "DELETE FROM `Accounts`", function( err, rows ) {
                                        if ( err ) {
                                            logger.log( err + ", rolling back", scriptName, "e" );
                                            return db.rollback( function() {});
                                        }
                                        db.commit( function( err ) {
                                            if ( err ) {
                                                logger.log( "commit: " + err, scriptName, "e" );
                                            }
                                            callback();
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    });
};

/**
 * Return items associated to input stash ID
 *
 * @param MySQL connection, stashID
 * @return items included
 */
var getStashByID = function( db, stashID, callback ) {
    // console.time( "Fetching stash" );
    db.query( "SELECT * FROM `Items` WHERE `stashId` = ?", [stashID], function( err, rows ) {
        if ( err ) {
            logger.log( err, scriptName, "e" );
        }
        // console.timeEnd( "Fetching stash" );
        callback( rows );
    });
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
    // console.time( "Getting link and color" );
    var groups      = {};
    var groupColors = {};
    var colors      = [];
    // FOr each sockets in the item
    async.eachLimit( item.sockets, 1, function( socket, cb ) {
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
        if ( err ) {
            logger.log( err, scriptName, "e" );
        }
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
        // console.timeEnd( "Getting link and color" );
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

    // logger.log( "Checking added, removed or kept items", scriptName );
    // For each item in old array, check if this item is in the new array
    async.each( old, function( itemOld, cbOld ) {
        itemOld.id = itemOld.itemId;
        var found = false;
        var foundItem;
        async.each( young, function( itemYoung, cbYoung ) {
            /* If we have an item in the new array with the same id, then this
               item already exists in the stash */
            if ( !found && itemYoung.id === itemOld.itemId ) {
                if ( !discovered[itemYoung.id] ) {
                    discovered[itemYoung.id] = 1;
                    if ( itemYoung.stashName ) {
                        itemOld.stashName = itemYoung.stashName;
                    }
                }
                found = true;
                foundItem = itemYoung;
                cbYoung({error:"breakAlready"});
            } else {
                cbYoung();
            }
        }, function( err ) {
            if ( err && err.error !== "breakAlready" ) {
                logger.log( "compareArrays: " + err, scriptName, "e" );
            }
            // If the item was marked as found, put it into the kept array
            if ( found ) {
                common.push( foundItem );
                cbOld();
            // Otherwise, it was removed and put it into the removed array
            } else {
                removed.push( itemOld );
                cbOld();
            }
        });
    }, function( err ) {
        if ( err ) {
            logger.log( "compareArrays: " + err, scriptName, "e" );
        }
        /* For each new items, if we didn't check this item yet, it means it 
           was added, so we add it to the added array */
        async.each( young, function( itemYoung, cbYoung ) {
            if ( !discovered[itemYoung.id]) {
                added.push( itemYoung );
            }
            cbYoung();
        }, function( err ) {
            if ( err ) {
                logger.log( "compareArrays: " + err, scriptName, "e" );
            }
            // logger.log( "added: " + added.length + ", removed: " + removed.length + ", kept: " + common.length + " items", scriptName );
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
    // console.time( "Parsing mods" );
    var parsedExplicitMods  = [];
    var parsedImplicitMods  = [];
    var parsedCraftedMods   = [];
    var parsedEnchantedMods = [];
    // console.time( "Parsing mods" );
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
            logger.log( "Error: " + err, scriptName, "w" );
        }
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
                logger.log( "Error: " + err, scriptName, "w" );
            }
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
                    logger.log( "Error: " + err, scriptName, "w" );
                }
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
                        logger.log( "Error: " + err, scriptName, "w" );
                    }
                    // console.timeEnd( "Parsing mods" );
                    callback( parsedExplicitMods, parsedImplicitMods, 
                              parsedCraftedMods, parsedEnchantedMods );
                });
            });
        });
    });
};

/**
 * Insert item mods, properties, requirements and sockets into the right tables
 *
 * For each data (mods, properties, requirements and sockets), remove existing
 * entries ( same itemId ) if they exist and add the new ones 
 * @param New item and callback
 * @return Nothing
 */
var insertOtherProperties = function( item, cb ) {
    // console.time( "Inserting other properties" );
    pool.getConnection( function( err, connection ) {
        if ( err ) {
            logger.log( err, scriptName, "e" );
        }
        connection.beginTransaction( function( err ) {
            if ( err ) {
                logger.log( err, scriptName, "w" );
            }
            // Insert into mods
            connection.query( "DELETE FROM `Mods` WHERE `itemId` = ?", [item.id], function( err, rows ) {
                if ( err ) {
                    logger.log( err, scriptName, "w" );
                }
                async.each( item.parsedImplicitMods, function( mod, cbMod ) {
                    for ( var i = 0 ; i < 3 - mod.values.length ; i++ ) {
                        mod.values.push( 0 );
                    }
                    connection.query( 
                        "INSERT INTO `Mods` (`itemId`, `modName`, `modValue1`, `modValue2`, `modValue3`, `modValue4`, `modType`) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `itemId` = `itemId`", [item.id, mod.mod, mod.values[0], mod.values[1], mod.values[2], mod.values[3], 'IMPLICIT'], function( err, rows ) {
                        if ( err ) {
                            logger.log( "Insert issue implicit: " + err, scriptName, "w" );
                            insertionError++;
                        }
                        cbMod();
                    });
                }, function( err ) {
                    if ( err ) {
                        logger.log( err, scriptName, "w" );
                    }
                    async.each( item.parsedExplicitMods, function( mod, cbMod ) {
                        for ( var i = 0 ; i < 3 - mod.values.length ; i++ ) {
                            mod.values.push( 0 );
                        }
                        connection.query( 
                            "INSERT INTO `Mods` (`itemId`, `modName`, `modValue1`, `modValue2`, `modValue3`, `modValue4`, `modType`) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `itemId` = `itemId`", [item.id, mod.mod, mod.values[0], mod.values[1], mod.values[2], mod.values[3], 'EXPLICIT'], function( err, rows ) {
                            if ( err ) {
                                logger.log( "Insert issue explicit: " + err, scriptName, "w" );
                                insertionError++;
                            }
                            cbMod();
                        });
                    }, function( err ) {
                        if ( err ) {
                            logger.log( err, scriptName, "w" );
                        }
                        async.each( item.parsedCraftedMods, function( mod, cbMod ) {
                            for ( var i = 0 ; i < 3 - mod.values.length ; i++ ) {
                                mod.values.push( 0 );
                            }
                            connection.query( 
                                "INSERT INTO `Mods` (`itemId`, `modName`, `modValue1`, `modValue2`, `modValue3`, `modValue4`, `modType`) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `itemId` = `itemId`", [item.id, mod.mod, mod.values[0], mod.values[1], mod.values[2], mod.values[3], 'CRAFTED'], function( err, rows ) {
                                if ( err ) {
                                    logger.log( "Insert issue crafted: " + err, scriptName, "w" );
                                    insertionError++;
                                }
                                cbMod();
                            });
                        }, function( err ) {
                            if ( err ) {
                                logger.log( err, scriptName, "w" );
                            }
                            async.each( item.parsedEnchantedMods, function( mod, cbMod ) {
                                for ( var i = 0 ; i < 3 - mod.values.length ; i++ ) {
                                    mod.values.push( 0 );
                                }
                                connection.query( 
                                    "INSERT INTO `Mods` (`itemId`, `modName`, `modValue1`, `modValue2`, `modValue3`, `modValue4`, `modType`) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `itemId` = `itemId`", [item.id, mod.mod, mod.values[0], mod.values[1], mod.values[2], mod.values[3], 'ENCHANTED'], function( err, rows ) {
                                    if ( err ) {
                                        logger.log( "Insert issue enchanted: " + err, scriptName, "w" );
                                        insertionError++;
                                    }
                                    cbMod();
                                });
                            }, function( err ) {
                                if ( err ) {
                                    logger.log( err, scriptName, "w" );
                                }
                                // Insert into properties
                                connection.query( "DELETE FROM `Properties` WHERE `itemId` = ?", [item.id], function( err, rows ) {
                                    if ( err ) {
                                        logger.log( err, scriptName, "w" );
                                    }
                                    async.each( item.properties, function( property, cbProperty ) {
                                        for ( var i = property.values.length ; i < 1 - property.values.length ; i++ ) {
                                            property.values[i] = [];
                                            property.values[i].push( 0 );
                                        }
                                        // console.log( property );
                                        connection.query( 
                                            "INSERT INTO `Properties` (`itemId`, `propertyName`, `propertyValue1`, `propertyValue2`, `propertyKey`) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `itemId` = `itemId`", [item.id, property.name, property.values[0][0], property.values[0][1], item.id + "_" + property.name], function( err, rows ) {
                                            if ( err ) {
                                                logger.log( "Insert issue properties: " + err, scriptName, "w" );
                                                insertionError++;
                                            }
                                            cbProperty();
                                        });
                                    }, function( err ) {
                                        if ( err ) {
                                            logger.log( err, scriptName, "w" );
                                        }
                                        // Insert into requirements
                                        connection.query( "DELETE FROM `Requirements` WHERE `itemId` = ?", [item.id], function( err, rows ) {
                                            if ( err ) {
                                                logger.log( err, scriptName, "w" );
                                            }
                                            async.each( item.requirements, function( requirement, cbRequirement ) {
                                                connection.query( 
                                                    "INSERT INTO `Requirements` (`itemId`, `requirementName`, `requirementValue`, `requirementKey`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `itemId` = `itemId`", [item.id, requirement.name, requirement.values[0][0], item.id + "_" + requirement.name], function( err, rows ) {
                                                    if ( err ) {
                                                        logger.log( "Insert issue requirements: " + err, scriptName, "w" );
                                                        insertionError++;
                                                    }
                                                    cbRequirement();
                                                });
                                            }, function( err ) {
                                                if ( err ) {
                                                    logger.log( err, scriptName, "w" );
                                                }
                                                // Insert into sockets
                                                connection.query( "DELETE FROM `Sockets` WHERE `itemId` = ?", [item.id], function( err, rows ) {
                                                    if ( err ) {
                                                        logger.log( err, scriptName, "w" );
                                                    }
                                                    var counterSocket = 0;
                                                    async.each( item.sockets, function( socket, cbSocket ) {
                                                        counterSocket++;
                                                        connection.query( 
                                                            "INSERT INTO `Sockets` (`itemId`, `socketGroup`, `socketAttr`, `socketKey`) VALUES (?, ?, ?, ?)", [item.id, socket.group, socket.attr, item.id + "_" + counterSocket], function( err, rows ) {
                                                            if ( err ) {
                                                                logger.log( "Insert issue sockets: " + err, scriptName, "w" );
                                                                insertionError++;
                                                            }
                                                            cbSocket();
                                                        });
                                                    }, function( err ) {
                                                        if ( err ) {
                                                            logger.log( err, scriptName, "w" );
                                                        }
                                                        connection.commit( function( err ) {
                                                            if ( err ) {
                                                                logger.log( err, scriptName, "w" );
                                                            }
                                                            connection.release();
                                                            // console.timeEnd( "Inserting other properties" );
                                                            cb();
                                                        });
                                                    });
                                                });
                                            });
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    });
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
var downloadChunk = function( chunkID, connection, callback ) {

    var download = function( chunkID ) {
        // Download compressed gzip data and extract it
        logger.log( "Downloading compressed data[" + chunkID + "]", scriptName );
        console.time( "Downloading JSON" );
        request({ "url": page + "?id=" + chunkID, "gzip": true },
            function( error, response, body ) {
                if ( error ) {
                    console.timeEnd( "Downloading JSON" );
                    logger.log( "Error occured, retrying: " + error, scriptName, "e" );
                    setTimeout(download, downloadInterval, chunkID );
                } else {
                    logger.log( "Downloaded and extracted", scriptName );
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
            logger.log( "Data loaded", scriptName );
            // If we reached the top and next_change_id is null
            if ( !data.next_change_id ) {
                logger.log( "Top reached, waiting", scriptName );
                setTimeout( download, 2000, chunkID );
            } else {
                parseData( data, chunkID );
            }
        } catch ( e ) {
            logger.log( "Error occured, retrying: " + e, scriptName, "e" );
            setTimeout( download, downloadInterval, chunkID );
        }
    };

    var parseData = function( data ) {
        // Store last chunk ID
        logger.log( "Adding chunk ID to DB", scriptName );
        connection.query( "INSERT INTO `ChangeId` (`nextChangeId`) VALUES (?)", [data.next_change_id], function( err, rows ) {
            if ( err ) {
                logger.log( "There was an error inserting chunk_id value: " + err, scriptName, "w" );
            }
            logger.log( "Reading data file", scriptName );
            console.time( "Loading data into DB" );

            // Vars for time profiling
            var propertiesTime    = 0;
            var compareArraysTime = 0;
            var itemInsertTime    = 0;
            var itemTotal         = 0;
            startInsert           = Date.now();
            addedNew       = 0;
            updatedNew     = 0;
            removedNew     = 0;
            modParsingTime = 0;
            // For each stashes in the new data file
            async.each( data.stashes, function( stash, callbackStash ) {
                // If stash is updated, the account is online
                // If accountName is null, skip
                itemTotal += stash.items.length;
                var dateTime = Date.now();
                if ( stash.accountName ) {
                    /* If account has already been inserted, update the last 
                       recorded activity field and the last character name */
                    connection.query( "INSERT INTO `Accounts` (`accountName`, `lastCharacterName`, `lastSeen`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `lastSeen` = ?, `lastCharacterName` = ?",
                    [stash.accountName, stash.lastCharacterName, dateTime, dateTime, stash.lastCharacterName], function( err, rows ) {
                        if ( err ) {
                            logger.log( "Online collection: There was an error inserting value: " + err, scriptName, "w" );
                            insertionError++;
                        }
                    });
                }

                /* Insert the league of the item in the DB, no update if the 
                   league already exists */
                if ( stash.items.length > 0 ) {
                    connection.query( "INSERT INTO `Leagues` (`leagueName`) VALUES (?) ON DUPLICATE KEY UPDATE `leagueName` = `leagueName`",
                    [stash.items[0].league], function( err, rows ) {
                        if ( err ) {
                            console.log( stash.items[0].league );
                            logger.log( "league: " + err, scriptName, "w" );
                        }
                    });
                }

                /* Create a new stash in the DB, update the stash name, stash 
                   type and public status if the stash ID already exists */
                connection.query( "INSERT INTO `Stashes` (`stashId`, `stashName`, `stashType`, `publicStash`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `stashName` = ?, `stashType` = ?, `publicStash` = ?",
                    [stash.id, stash.stash, stash.stashType, stash.public ? 1 : 0, stash.stash, stash.stashType, stash.public ? 1 : 0], function( err, rows ) {
                    if ( err ) {
                        logger.log( "Stash: " + err, scriptName, "w" );
                    }
                });
                

                // Get previously stored stash contents
                getStashByID( connection, stash.id, function( results ) {
                    // console.log( results );
                    // If the stash does not exist, store all items
                    if ( results && results.length === 0 ) {
                        logger.log( "Stash " + stash.id + " does not exist, creating it", scriptName, "", true );
                        logger.log( "Stash contains " + stash.items.length + " items", scriptName, "", true );

                        // For each item in the stash
                        async.each( stash.items, function( item, cb ) {
                            var modParsingStart = Date.now();
                            // Parse its mods
                            parseMods( item, function( explicit, implicit, crafted, enchanted ) {
                                modParsingTime += Date.now() - modParsingStart;
                                var socketAmount = item.sockets.length;
                                var available    = 1;
                                var addedTs      = Date.now();
                                var updatedTs    = Date.now();
                                item.parsedImplicitMods  = implicit;
                                item.parsedExplicitMods  = explicit;
                                item.parsedCraftedMods   = crafted;
                                item.parsedEnchantedMods = enchanted;
                                // Cleanup name and typeLine attributes
                                var name                = item.name.replace( "<<set:MS>><<set:M>><<set:S>>", "" );
                                var typeLine            = item.typeLine.replace( "<<set:MS>><<set:M>><<set:S>>", "" );
                                var verified            = item.verified ? 1 : 0;
                                var identified          = item.identified ? 1 : 0;
                                var corrupted           = item.corrupted ? 1 : 0;
                                var lockedToCharacter   = item.lockedToCharacter ? 1 : 0;
                                var flavourText         = !item.flavourText ? "" : item.flavourText.join("\n");
                                var price;
                                crafted                 = item.parsedCraftedMods.length > 0 ? 1 : 0;
                                enchanted               = item.parsedEnchantedMods.length > 0 ? 1 : 0;
                                // If note exists on the item, set price to note
                                if ( item.note ) {
                                    price = item.note;
                                // Otherwise, price is in the name of the stash
                                } else {
                                    price = stash.stash;
                                }

                                // Get sockets and links for item
                                getLinksAmountAndColor( item, function( res ) {
                                    var linkAmount   = res.linkAmount;
                                    // Store this item
                                    var itemInsertStart = Date.now();
                                    /* Insert item. If item already exists in the
                                       DB, update its attributes */
                                    connection.query( 
                                        "INSERT INTO `Items` (`w`, `h`, `ilvl`, `icon`, `league`, `itemId`, `name`, `typeLine`, `identified`, `verified`, `crafted`, `enchanted`, `corrupted`, `lockedToCharacter`, `frameType`, `x`, `y`, `inventoryId`, `accountName`, `stashId`, `socketAmount`, `linkAmount`, `available`, `addedTs`, `updatedTs`, `flavourText`, `price`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `verified` = ?, `crafted` = ?, `enchanted` = ?, `corrupted` = ?, `x` = ?, `y` = ?, `inventoryId` = ?, `accountName` = ?, `stashId` = ?, `socketAmount` = ?, `linkAmount` = ?, `available` = ?, `updatedTs` = ?, `price` = ?", 
                                        [item.w, item.h, item.ilvl, item.icon, item.league, item.id, name, typeLine, identified, verified, crafted, enchanted, corrupted, lockedToCharacter, item.frameType, item.x, item.y, item.inventoryId, stash.accountName, stash.id, socketAmount, linkAmount, available, addedTs, updatedTs, flavourText, price, name, verified, crafted, enchanted, corrupted, item.x, item.y, item.inventoryId, stash.accountName, stash.id, socketAmount, linkAmount, available, Date.now(), price], function( err, rows ) {
                                        if ( err ) {
                                            logger.log( "New stash here: There was an error inserting value: " + err, scriptName, "w" );
                                            insertionError++;
                                        } else {
                                            added++;
                                            addedNew++;
                                        }
                                        itemInsertTime += Date.now() - itemInsertStart;
                                        
                                        var propertiesStart = Date.now();
                                        /* Insert other item properties such as
                                           mods and requirements */ 
                                        insertOtherProperties( item, function() {
                                            propertiesTime += Date.now() - propertiesStart;
                                            if ( !item.name ) {
                                                logger.log(
                                                    "Adding new item \x1b[35m" +
                                                    item.typeLine +
                                                    "\x1b[0m to " + stash.id, scriptName, "", true );
                                            } else {
                                                logger.log(
                                                    "Adding new item \x1b[35m" +
                                                    item.name +
                                                    "\x1b[0m to " + stash.id, scriptName, "", true );
                                            }
                                            cb();
                                        });
                                    });
                                }); 
                            });
                        }, function( err ) {
                            if ( err ) {
                                logger.log( "New stash: There was an error inserting value: " + err, scriptName, "w" );
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
                                stash.id, scriptName, "", true );
                        } else if ( results.length < stash.items.length ) {
                            logger.log(
                                ( stash.items.length - results.length ) +
                                " items were added to the stash " +
                                stash.id, scriptName, "", true );
                        }

                        logger.log( "Updating existing stash " + stash.id, scriptName, "", true );
                        /* Check which item has been removed, added or kept by 
                           comparing the items in the current stash with the new
                           ones */
                        var compareArraysStart = Date.now();
                        compareArrays( results, stash.items, function( res ) {
                            compareArraysTime = Date.now() - compareArraysStart;
                            // console.log( res );
                            logger.log( res.added.length + " items added", scriptName, "", true );
                            logger.log( res.removed.length + " items removed", scriptName, "", true );
                            logger.log( res.common.length + " items to update", scriptName, "", true );
                            // For each removed item
                            async.each( res.removed, function( removedItem, cbRemoved ) {
                                var modParsingStart = Date.now();
                                parseMods( removedItem, function( explicit, implicit, crafted, enchanted ) {
                                    modParsingTime += Date.now() - modParsingStart;
                                    removedItem.parsedImplicitMods  = implicit;
                                    removedItem.parsedExplicitMods  = explicit;
                                    removedItem.parsedCraftedMods   = crafted;
                                    removedItem.parsedEnchantedMods = enchanted;
                                    // Set item status to unavailable
                                    logger.log( removedItem.id + " removed", scriptName, "", true );
                                    removedItem.available           = 0;
                                    removedItem.name                = removedItem.name.replace( "<<set:MS>><<set:M>><<set:S>>", "" );
                                    removedItem.typeLine            = removedItem.typeLine.replace( "<<set:MS>><<set:M>><<set:S>>", "" );
                                    removedItem.verified            = removedItem.verified ? 1 : 0;
                                    removedItem.identified          = removedItem.identified ? 1 : 0;
                                    removedItem.corrupted           = removedItem.corrupted ? 1 : 0;
                                    removedItem.lockedToCharacter   = removedItem.lockedToCharacter ? 1 : 0;
                                    getLinksAmountAndColor( removedItem, function( res ) {
                                        removedItem.linkAmount        = res.linkAmount;
                                        removedItem.colors            = res.colors;
                                        removedItem.linkedColors      = res.linkedColors;
                                        var updatedDate = Date.now();
                                        // Update status in DB
                                        var itemInsertStart = Date.now();
                                        connection.query( 
                                            "UPDATE `Items` SET " + 
                                            "`w` = ?, `h` = ?, `ilvl` = ?, `icon` = ?, `league` = ?, `name` = ?, `typeLine` = ?, `identified` = ?, `verified` = ?, `corrupted` = ?, `lockedToCharacter` = ?, `frameType` = ?, `x` = ?, `y` = ?, `inventoryId` = ?, `accountName` = ?, `stashId` = ?, `socketAmount` = ?, `linkAmount` = ?, `available` = ?, `updatedTs` = ? WHERE `itemId` = ?", 
                                            [removedItem.w, removedItem.h, removedItem.ilvl, removedItem.icon, removedItem.league, removedItem.name, removedItem.typeLine, removedItem.identified, removedItem.verified, removedItem.corrupted, removedItem.lockedToCharacter, removedItem.frameType, removedItem.x, removedItem.y, removedItem.inventoryId, removedItem.accountName, stash.id, removedItem.socketAmount, removedItem.linkAmount, removedItem.available, updatedDate, removedItem.id], function( err, rows ) {
                                            if ( err ) {
                                                logger.log(
                                                    "Stash update -> unavailable: There was an error inserting value: " + err,
                                                    scriptName, "w" );
                                                    console.log( removedItem );
                                                insertionError++;
                                            } else {
                                                removed++;
                                                removedNew++;
                                            }
                                            itemInsertTime += Date.now() - itemInsertStart;
                                            // console.log( removedItem );
                                            // insertOtherProperties( removedItem, function() {
                                                if ( !removedItem.name ) {
                                                    logger.log(
                                                        "Removing item \x1b[35m" +
                                                        removedItem.typeLine +
                                                        "\x1b[0m to " + stash.id, scriptName, "", true );
                                                } else {
                                                    logger.log(
                                                        "Removing item \x1b[35m" +
                                                        removedItem.name +
                                                        "\x1b[0m to " + stash.id, scriptName, "", true );
                                                }
                                                cbRemoved();
                                            // }); 
                                        });
                                    });
                                });
                            }, function( err ) {
                                if ( err ) {
                                    logger.log( "parseData: " + err, scriptName, "e" );
                                }
                                // For each item added
                                async.each( res.added, function( addedItem, cbAdded ) {
                                    logger.log( addedItem.id + " added", scriptName, "", true );
                                    var modParsingStart = Date.now();
                                    parseMods( addedItem, function( explicit, implicit, crafted, enchanted ) {
                                        modParsingTime += Date.now() - modParsingStart;
                                        addedItem.accountName  = stash.accountName;
                                        addedItem.stashID      = stash.id;
                                        var socketAmount        = addedItem.sockets.length;
                                        var available           = 1;
                                        var name                = addedItem.name.replace( "<<set:MS>><<set:M>><<set:S>>", "" );
                                        var typeLine            = addedItem.typeLine.replace( "<<set:MS>><<set:M>><<set:S>>", "" );
                                        var verified            = addedItem.verified ? 1 : 0;
                                        var identified          = addedItem.identified ? 1 : 0;
                                        var corrupted           = addedItem.corrupted ? 1 : 0;
                                        var lockedToCharacter   = addedItem.lockedToCharacter ? 1 : 0;
                                        var addedTs      = Date.now();
                                        var updatedTs    = Date.now();
                                        addedItem.parsedImplicitMods  = implicit;
                                        addedItem.parsedExplicitMods  = explicit;
                                        addedItem.parsedCraftedMods   = crafted;
                                        addedItem.parsedEnchantedMods = enchanted;
                                        var flavourText         = !addedItem.flavourText ? "" : addedItem.flavourText.join("\n");
                                        var price;
                                        crafted                 = addedItem.parsedCraftedMods.length > 0 ? 1 : 0;
                                        enchanted               = addedItem.parsedEnchantedMods.length > 0 ? 1 : 0;
                                        if ( addedItem.note ) {
                                            price = addedItem.note;
                                        } else {
                                            price = stash.stash;
                                        }
                                        getLinksAmountAndColor( addedItem, function( res ) {
                                            var linkAmount   = res.linkAmount;
                                            // Store this item
                                            var itemInsertStart = Date.now();
                                            connection.query( 
                                                "INSERT INTO `Items` (`w`, `h`, `ilvl`, `icon`, `league`, `itemId`, `name`, `typeLine`, `identified`, `verified`, `crafted`, `enchanted`, `corrupted`, `lockedToCharacter`, `frameType`, `x`, `y`, `inventoryId`, `accountName`, `stashId`, `socketAmount`, `linkAmount`, `available`, `addedTs`, `updatedTs`, `flavourText`, `price`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `verified` = ?, `crafted` = ?, `enchanted` = ?, `corrupted` = ?, `x` = ?, `y` = ?, `inventoryId` = ?, `accountName` = ?, `stashId` = ?, `socketAmount` = ?, `linkAmount` = ?, `available` = ?, `updatedTs` = ?, `price` = ?", 
                                                [addedItem.w, addedItem.h, addedItem.ilvl, addedItem.icon, addedItem.league, addedItem.id, name, typeLine, identified, verified, crafted, enchanted, corrupted, lockedToCharacter, addedItem.frameType, addedItem.x, addedItem.y, addedItem.inventoryId, addedItem.accountName, stash.id, socketAmount, linkAmount, available, addedTs, updatedTs, flavourText, price, name, verified, crafted, enchanted, corrupted, addedItem.x, addedItem.y, addedItem.inventoryId, addedItem.accountName, addedItem.stashID, socketAmount, linkAmount, available, Date.now(), price], function( err, rows ) {
                                                if ( err ) {
                                                    logger.log( "Stash update -> added: There was an error inserting value: " + err, scriptName, "w" );
                                                    insertionError++;
                                                } else {
                                                    added++;
                                                    addedNew++;
                                                }
                                                itemInsertTime += Date.now() - itemInsertStart;
                                                var propertiesStart = Date.now();
                                                insertOtherProperties( addedItem, function() {
                                                    if ( !addedItem.name ) {
                                                        logger.log(
                                                            "Adding new item \x1b[35m" +
                                                            typeLine +
                                                            "\x1b[0m to " + stash.id, scriptName, "", true );
                                                    } else {
                                                        logger.log(
                                                            "Adding new item \x1b[35m" +
                                                            name +
                                                            "\x1b[0m to " + stash.id, scriptName, "", true );
                                                    }
                                                    propertiesTime += Date.now() - propertiesStart;
                                                    cbAdded();
                                                });
                                            });
                                        });
                                    });
                                }, function( err ) {
                                    if ( err ) {
                                        logger.log( err, scriptName, "e" );
                                    }
                                    // For each item kept
                                    async.each( res.common, function( commonItem, cbCommon ) {
                                        logger.log( commonItem.id + " updated", scriptName, "", true );
                                        var modParsingStart = Date.now();
                                        parseMods( commonItem, function( explicit, implicit, crafted, enchanted ) {
                                            modParsingTime += Date.now() - modParsingStart;
                                            commonItem.parsedImplicitMods  = implicit;
                                            commonItem.parsedExplicitMods  = explicit;
                                            commonItem.parsedCraftedMods   = crafted;
                                            commonItem.parsedEnchantedMods = enchanted;
                                            var socketAmount        = commonItem.sockets.length;
                                            var name                = commonItem.name.replace( "<<set:MS>><<set:M>><<set:S>>", "" );
                                            var typeLine            = commonItem.typeLine.replace( "<<set:MS>><<set:M>><<set:S>>", "" );
                                            var verified            = commonItem.verified ? 1 : 0;
                                            var identified          = commonItem.identified ? 1 : 0;
                                            var corrupted           = commonItem.corrupted ? 1 : 0;
                                            var lockedToCharacter   = commonItem.lockedToCharacter ? 1 : 0;
                                            crafted                 = commonItem.parsedCraftedMods.length > 0 ? 1 : 0;
                                            enchanted               = commonItem.parsedEnchantedMods.length > 0 ? 1 : 0;
                                            // Update its update timestamp
                                            commonItem.updatedTs = Date.now();
                                            getLinksAmountAndColor( commonItem, function( res ) {
                                                commonItem.linkAmount   = res.linkAmount;
                                                var updatedDate = Date.now();
                                                // Update status in DB
                                                var itemInsertStart = Date.now();
                                                connection.query( 
                                                    "UPDATE `Items` SET " + 
                                                    "`w` = ?, `h` = ?, `ilvl` = ?, `icon` = ?, `league` = ?, `name` = ?, `typeLine` = ?, `identified` = ?, `verified` = ?, `crafted` = ?, `enchanted` = ?, `corrupted` = ?, `lockedToCharacter` = ?, `frameType` = ?, `x` = ?, `y` = ?, `inventoryId` = ?, `accountName` = ?, `stashId` = ?, `socketAmount` = ?, `linkAmount` = ?, `updatedTs` = ? WHERE `itemId` = ?", 
                                                    [commonItem.w, commonItem.h, commonItem.ilvl, commonItem.icon, commonItem.league, name, typeLine, identified, verified, crafted, enchanted, corrupted, lockedToCharacter, commonItem.frameType, commonItem.x, commonItem.y, commonItem.inventoryId, stash.accountName, stash.id, socketAmount, commonItem.linkAmount, updatedDate, commonItem.id], function( err, rows ) {
                                                    if ( err ) {
                                                        logger.log( "Stash update -> kept: There was an error inserting value: " + err, scriptName, "w" );
                                                        insertionError++;
                                                    } else {
                                                        updated++;
                                                        updatedNew++;
                                                    }
                                                    itemInsertTime += Date.now() - itemInsertStart;
                                                    var propertiesStart = Date.now();
                                                    insertOtherProperties( commonItem, function() {
                                                        if ( !commonItem.name ) {
                                                            logger.log(
                                                                "Updating item \x1b[35m" +
                                                                typeLine +
                                                                "\x1b[0m to " + stash.id, scriptName, "", true );
                                                        } else {
                                                            logger.log(
                                                                "Updating item \x1b[35m" +
                                                                name +
                                                                "\x1b[0m to " + stash.id, scriptName, "", true );
                                                        }
                                                        propertiesTime += Date.now() - propertiesStart;
                                                        cbCommon();
                                                    });
                                                });
                                            });
                                        });
                                    }, function( err ) {
                                        if ( err ) {
                                            logger.log( err, scriptName, "e" );
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
                    logger.log( err, scriptName, "e" );
                }
                logger.log( "Took " + itemInsertTime + " ms (" + Math.round(itemInsertTime / itemTotal * 100)/100 + " ms/item ) for item insertion", scriptName );
                logger.log( "Took " + propertiesTime + " ms (" + Math.round(propertiesTime / itemTotal * 100)/100 + " ms/item ) for other table insertion", scriptName );
                logger.log( "Took " + modParsingTime + " ms (" + Math.round(modParsingTime / itemTotal * 100)/100 + " ms/item ) for mod parsing", scriptName );
                logger.log( "Took " + compareArraysTime + " ms (" + Math.round(compareArraysTime / itemTotal * 100)/100 + " ms/item ) for comparing arrays", scriptName );
                console.timeEnd( "Loading data into DB" );
                done( data );
            });
        });
    };

    var done = function( data ) {
        var nextID = data.next_change_id;
        logger.log( "Next ID: " + nextID, scriptName );

        if ( interrupt ) {
            logger.log( "Exiting", scriptName );
            pool.end();
            process.exit( 0 );
        } else {
            /* Sleep n seconds and call the script on the
               next chunk ID */
            var elapsed = secToNsec( Date.now() - startTime );
            var speed   = ( added + removed + updated ) /
                          (( Date.now() - startTime ) / 1000 ); // insert per sec
            var elapsedInstant = secToNsec( Date.now() - startInsert );
            var speedInstant  = ( addedNew + removedNew + updatedNew ) /
                                (( Date.now() - startInsert ) / 1000 ); // instant insert per sec
            logger.log( "Entries total: " + ( addedNew + removedNew + updatedNew ) +
                        ", added: " + addedNew +
                        ", removed: " + removedNew +
                        ", updated: " + updatedNew +
                        ", insert errors: " + insertionError +
                        " over " + Math.round( elapsedInstant.amount ) +
                        " " + elapsedInstant.unit +
                        " at " + Math.round( speedInstant ) +
                        " insert/sec", scriptName );
            logger.log( "Total entries added: " + added +
                        ", removed: " + removed +
                        ", updated: " + updated +
                        ", insert errors: " + insertionError +
                        " over " + Math.round( elapsed.amount ) +
                        " " + elapsed.unit +
                        " at " + Math.round( speed ) +
                        " insert/sec", scriptName );
            logger.log( "Sleeping " + downloadInterval + "ms", scriptName );
            setTimeout( callback, downloadInterval,
                        nextID, connection, callback );
        }
    };

    download( chunkID );
};

// Main loop
function main() {
    // Parse argv
    process.argv.forEach(( val, index ) => {
        if ( val === "-d" ) {
            logger.log( "Activating debug", scriptName, "e" );
            debug = true;
        } else if ( val === "-c" ) {
            logger.log( "Cleaning up DB", scriptName, "e" );
            cleanup = true;
        }
    });

    if ( debug ) {
        // write to log.txt
        logger.set_use_file( true );
    }

    pool = mysql.createPool( credentials );

    logger.log( "Attempting to connect to POE collection", scriptName );
    pool.getConnection( function( err, connection ) {
        if ( err ) {
            logger.log( err, scriptName, "e" );
            process.exit(0);
        }
        logger.log( "Connected", scriptName );
        if ( cleanup ) {
            cleanupDB( connection, function() {
                logger.log( "Cleanup finished, exiting", scriptName );
                pool.end();
                process.exit(0);
            });
        } else {
            // Check last downloaded chunk ID
            lastDownloadedChunk( connection, function( entry ) {
                try {
                    logger.log( "Next chunk ID: " + entry[0].nextChangeId, scriptName );
                    downloadChunk( entry[0].nextChangeId, connection, downloadChunk );
                } catch ( e ) {
                    logger.log( "Starting new indexation", scriptName, "w" );
                    downloadChunk( "", connection, downloadChunk );
                }
            });
        }
    });
}

process.on('SIGINT', function() {
    logger.log( "\rCaught interrupt signal, exiting gracefully", scriptName, "e" );
    interrupt = true;
});

main();
