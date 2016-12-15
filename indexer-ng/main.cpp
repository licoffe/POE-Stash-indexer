#include <stdio.h>
#include <signal.h>
#include <algorithm>
#include <thread>
#include <string>
#include <ctime>
#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <deque>
#include <regex>
#include <curl/curl.h>
#include <mysql_connection.h>
#include <mysql_driver.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include "./include/rapidjson/document.h"
#include "Item.h"
#include "Misc.h"
#include "Colors.h"
#include "main.h"

#define FUNCTION __FUNCTION__


const std::string URL          = "http://api.pathofexile.com/public-stash-tabs";
const std::string download_dir = "./data/";
std::string next_change_id;
std::deque<std::string> downloaded_files = std::deque<std::string>();
const std::string DB_HOST = "tcp://127.0.0.1";
const std::string DB_PORT = "8889";
const std::string DB_USER = "root";
const std::string DB_PASS = "timmy2887theGIANT1511";
const std::string DB_NAME = "POE";
bool interrupt            = false;
int item_added             = 0;
int item_removed           = 0;
int item_updated           = 0;
int total_item_added       = 0;
int total_item_removed     = 0;
int total_item_updated     = 0;
int errors                 = 0;
int total_errors           = 0;
int total_sum              = 0; 
float total_time           = 0.0;
float time_mods            = 0.0;
float time_properties      = 0.0;
float time_requirements    = 0.0;
float time_sockets         = 0.0;
float time_item            = 0.0;

size_t write_data( void *ptr, size_t size, size_t nmemb, FILE *stream ) {
    size_t written = fwrite( ptr, size, nmemb, stream );
    return written;
}

std::string stamp( std::string sender ) {
    return YELLOW + date() + RED + " > " + RESET + "[" + CYAN + sender + RESET + "] ";
}

void print_sql_error( const sql::SQLException e ) {
    std::cout << std::endl << "# ERR: SQLException in " << __FILE__;
    std::cout << std::endl << "(" << __FUNCTION__ << ") on line "
            << __LINE__ << std::endl;
    std::cout << std::endl << "# ERR: " << e.what();
    std::cout << std::endl << " (MySQL error code: " << e.getErrorCode();
    std::cout << std::endl << ", SQLState: " << e.getSQLState() << " )" << std::endl;
}

/**
 * Download target change id and return the path of the downloaded JSON file
 *
 * @param Change ID
 * @return Path to downloaded file
 */
std::string download_JSON( std::string change_id ) {
    CURL *curl;
    FILE *fp;
    CURLcode res;
    std::string path;
    std::string url = URL + "?id=" + change_id;
    // std::cout << std::endl << "Url:" << url << std::endl;

    if ( change_id.compare( "" ) == 0 ) {
        // std::cout << std::endl << "Downloading first JSON " << change_id;
        path = std::string( download_dir + "indexer_first.json" );
    } else {
        // std::cout << std::endl << "Downloading " << change_id;
        path = std::string( download_dir + "indexer_" + change_id + ".json" );
    }

    const char* outfilename = path.c_str();
    curl = curl_easy_init();
    if ( curl ) {
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        fp = fopen( outfilename, "wb" );
        curl_easy_setopt( curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, write_data );
        curl_easy_setopt( curl, CURLOPT_WRITEDATA, fp );
        res = curl_easy_perform( curl );
        /* always cleanup */
        curl_easy_cleanup( curl );
        fclose( fp );
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        std::cout << stamp( __FUNCTION__ ) << "Downloaded " << change_id 
                  << " (" << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0
                  << "sec )" << std::endl;
        // Add file to the queue
        downloaded_files.push_back( change_id );
        return path;
    }
    return "";
}

/**
 * Return the next change ID to download from last downloaded chunk file.
 * Update the queue to hold all change id to be processed.
 *
 * @param Nothing
 * @return Next change ID
 */
std::string last_downloaded_chunk() {
    std::vector<std::string> results;
	
    try {
        sql::mysql::MySQL_Driver *driver;
        sql::Connection *con;
        sql::Statement *stmt;
        sql::ResultSet  *res;

        driver = sql::mysql::get_mysql_driver_instance();
        con    = driver->connect( DB_HOST + ":" + DB_PORT, DB_USER, DB_PASS );

        stmt = con->createStatement();
        stmt->execute( "USE POE"  );
        res  = stmt->executeQuery( "SELECT `nextChangeId` FROM `ChangeId` ORDER BY ID DESC LIMIT 1"  );
        while ( res->next()) {
            results.push_back( res->getString( "nextChangeId" ));
        }
        res  = stmt->executeQuery( "SELECT `nextChangeId` FROM `ChangeId` WHERE `processed` = 0 ORDER BY ID ASC"  );
        while ( res->next()) {
            downloaded_files.push_back( res->getString( "nextChangeId" ));
        }

        delete res;
        delete stmt;
        delete con;
    } catch ( sql::SQLException &e ) {
        print_sql_error( e );
        return "";
    }
    if ( results.size() > 0 ) {
        return results[0];
    } else {
        return "-1";
    }
};

/**
 * Return the content of a stash id
 *
 * @param Stash id
 * @return Content of the stash
 */
std::vector<Item> get_stash_by_ID( std::string stash_id ) {
    std::vector<Item> results;
	
    try {
        sql::mysql::MySQL_Driver *driver;
        sql::Connection *con;
        sql::Statement *stmt;
        sql::ResultSet  *res;

        driver = sql::mysql::get_mysql_driver_instance();
        con    = driver->connect( DB_HOST + ":" + DB_PORT, DB_USER, DB_PASS );

        stmt = con->createStatement();
        stmt->execute( "USE POE"  );
        res  = stmt->executeQuery( "SELECT * FROM `Items` WHERE `stashId` = '" + stash_id + "'" );
        while ( res->next()) {
            Item item = Item(
                res->getInt( "w" ), res->getInt( "h" ), res->getInt( "ilvl" ),
                res->getString( "icon" ), res->getString( "league" ),
                res->getString( "itemId" ), res->getString( "name" ),
                res->getString( "typeLine" ), res->getBoolean( "identified" ),
                res->getBoolean( "verified" ), res->getBoolean( "corrupted" ),
                res->getBoolean( "lockedToCharacter" ), res->getInt( "frameType" ),
                res->getInt( "x" ), res->getInt( "y" ), res->getString( "inventoryId" ),
                res->getString( "accountName" ), res->getString( "stashId" ),
                res->getInt( "socketAmount" ), res->getInt( "linkAmount" ),
                res->getBoolean( "available" ), res->getInt( "addedTs" ),
                res->getInt( "updatedTs" ), res->getString( "flavourText" ),
                res->getString( "price" ), res->getBoolean( "crafted" ),
                res->getBoolean( "enchanted" )
            );
            results.push_back( item );
        }

        delete res;
        delete stmt;
        delete con;
    } catch ( sql::SQLException &e ) {
        print_sql_error( e );
        return results;
    }
    return results;
};

/**
 * Computes the amount of links and the socket colors of an item
 *
 * @param item data
 * @return pass the amount to callback
 */
// void get_links_amount_and_color( Item item ) {
//     var groups      = {};
//     var groupColors = {};
//     var colors      = [];
//     // FOr each sockets in the item
//     for ( int i = 0 ; i < item.socket )
//     async.each( item.sockets, function( socket, cb ) {
//         // If we have a new socket group
//         if ( !groups[socket.group] ) {
//             groups[socket.group] = 1;
//             groupColors[socket.group] = [socket.attr];
//         // Otherwise, add a new socket to this group
//         } else {
//             groups[socket.group]++;
//             groupColors[socket.group].push( socket.attr );
//         }
//         colors.push( socket.attr );
//         cb();
//     }, function( err ) {
//         if ( err ) {
//             logger.log( err, scriptName, "e" );
//         }
//         var linkAmount = 0;
//         var linkColors = [];
//         // Extract largest group
//         for ( var key in groups ) {
//             if ( groups.hasOwnProperty( key )) {
//                 if ( groups[key] > linkAmount ) {
//                     linkAmount = groups[key];
//                     linkColors = groupColors[key];
//                 }
//             }
//         }
//         // console.timeEnd( "Getting link and color" );
//         callback({ "linkAmount": linkAmount, "colors": colors, "linkedColors": linkColors });
//     });
// };

// void parse_mods( std::string item_id, 
//                  std::vector<std::string> explicitMods, 
//                  std::vector<std::string> implicit, 
//                  std::vector<std::string> crafted, 
//                  std::vector<std::string> enchanted ) {
//     const std::regex re( "([0-9.]+)" );
//     sql::mysql::MySQL_Driver *driver;
//     sql::Connection *con;
//     sql::Statement *stmt;

//     driver = sql::mysql::get_mysql_driver_instance();
//     con    = driver->connect( DB_HOST + ":" + DB_PORT, DB_USER, DB_PASS );

//     stmt = con->createStatement();
//     stmt->execute( "USE POE"  );
    
//     int counter_mods = 0;
//     for ( int i = 0 ; i < explicitMods.size() ; i++ ) {
//         counter_mods++;
//         std::string mod  = explicitMods[i];
//         std::string name = std::regex_replace( mod, re, "#" );
//         std::smatch sm;
//         std::vector<std::string> values = std::vector<std::string>();
//         values.assign( 4, "" );

//         std::string::const_iterator searchStart( mod.cbegin());
//         int index = 0;
//         while ( regex_search( searchStart, mod.cend(), sm, re )) {
//             values[index] = sm[0];
//             // std::cout << ( searchStart == mod.cbegin() ? "" : " " ) << sm[0];
//             searchStart += sm.position() + sm.length();
//             index++;
//         }
//         try {
//             stmt->execute( "INSERT INTO `Mods` (`itemId`, `modName`, `modValue1`, `modValue2`, `modValue3`, `modValue4`, `modType`, `modKey`) VALUES ('" +
//              item_id + "', \"" + name + "\", '" + values[0] + "', '" + values[1] + "', '" + values[2] + "', '" + values[3] + "', 'EXPLICIT', '" + 
//              ( item_id + "_" + std::to_string(counter_mods )) + "') ON DUPLICATE KEY UPDATE `modName` = \"" + name + "\", `modValue1` = '" + values[0] + "', `modValue2` = '" + values[1] + "', `modValue3` = '" + values[2] + "', `modValue4` = '" + values[3] + "', `modType` = 'EXPLICIT'" );
//         } catch ( sql::SQLException &e ) {
//             print_sql_error( e );
//         }
//     }
// }

void parse_JSON( std::string path ) {
    rapidjson::Document document;
    time_mods         = 0.0;
    time_properties   = 0.0;
    time_requirements = 0.0;
    time_sockets      = 0.0;
    time_item         = 0.0;

    // Read all JSON file
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    path = download_dir + "indexer_" + path + ".json";
    if ( !std::ifstream( path )) {
        std::cout << stamp( __FUNCTION__ ) << "File does not exist, skipping: " 
                  << path << std::endl;
        return;
    }
    std::cout << stamp( __FUNCTION__ ) << "Reading data file: " << path << std::endl;
    std::ifstream file( path.c_str() );
    std::stringstream sstr;
    sstr << file.rdbuf();
    // Parse the JSON using RapidJSON
    document.Parse( sstr.str().c_str());
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    float time_loading_JSON = ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );
    std::cout << stamp( __FUNCTION__ ) << "Loaded: " << path << " in " 
              << time_loading_JSON << " sec" << std::endl;

    sql::mysql::MySQL_Driver *driver;
    sql::Connection          *con;

    // Connect to DB
    driver = sql::mysql::get_mysql_driver_instance();
    con    = driver->connect( DB_HOST + ":" + DB_PORT, DB_USER, DB_PASS );
    con->setSchema( DB_NAME );

    sql::PreparedStatement   *stmt;
    sql::PreparedStatement   *account_stmt = con->prepareStatement( "INSERT INTO `Accounts` (`accountName`, `lastCharacterName`, `lastSeen`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `lastSeen` = ?, `lastCharacterName` = ?" );
    sql::PreparedStatement   *stash_stmt = con->prepareStatement( "INSERT INTO `Stashes` (`stashId`, `stashName`, `stashType`, `publicStash`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `stashName` = ?, `stashType` = ?, `publicStash` = ?" ); 
    sql::PreparedStatement   *league_stmt = con->prepareStatement( "INSERT INTO `Leagues` (`leagueName`, `active`, `poeTradeId`) VALUES (?, '1', ?) ON DUPLICATE KEY UPDATE `leagueName` = `leagueName`" );
    sql::PreparedStatement   *item_stmt = con->prepareStatement( "INSERT INTO `Items` (`w`, `h`, `ilvl`, `icon`, `league`, `itemId`, `name`, `typeLine`, `identified`, `verified`, `crafted`, `enchanted`, `corrupted`, `lockedToCharacter`, `frameType`, `x`, `y`, `inventoryId`, `accountName`, `stashId`, `socketAmount`, `linkAmount`, `available`, `addedTs`, `updatedTs`, `flavourText`, `price`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '1', ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `verified` = ?, `crafted` = ?, `enchanted` = ?, `corrupted` = ?, `x` = ?, `y` = ?, `inventoryId` = ?, `accountName` = ?, `stashId` = ?, `socketAmount` = ?, `linkAmount` = ?, `available` = '1', `updatedTs` = ?, `price` = ?" );
    sql::PreparedStatement   *mod_stmt = con->prepareStatement( "INSERT INTO `Mods` (`itemId`, `modName`, `modValue1`, `modValue2`, `modValue3`, `modValue4`, `modType`, `modKey`) VALUES (?, ?, ?, ?, ?, ?, 'EXPLICIT', ?) ON DUPLICATE KEY UPDATE `modName` = ?, `modValue1` = ?, `modValue2` = ?, `modValue3` = ?, `modValue4` = ?, `modType` = 'EXPLICIT'" );
    sql::PreparedStatement   *socket_stmt = con->prepareStatement( "INSERT INTO `Sockets` (`itemId`, `socketGroup`, `socketAttr`, `socketKey`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `socketGroup` = ?, `socketAttr` = ?" );
    sql::PreparedStatement   *property_stmt = con->prepareStatement( "INSERT INTO `Properties` (`itemId`, `propertyName`, `propertyValue1`, `propertyValue2`, `propertyKey`) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `propertyName` = ?, `propertyValue1` = ?, `propertyValue2` = ?" );
    sql::PreparedStatement   *requirement_stmt = con->prepareStatement( 
        "INSERT INTO `Requirements` (`itemId`, `requirementName`, `requirementValue`, `requirementKey`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `requirementName` = ?, `requirementValue` = ?" );

    try {
        // stmt = con->prepareStatement( "START TRANSACTION" );
        // stmt->execute();
        // delete stmt;
    } catch ( sql::SQLException &e ) {
        print_sql_error( e );
    }
    
    // const rapidjson::Value& change_id = document["next_change_id"];
    // next_change_id = (char*) change_id.GetString();

    const rapidjson::Value& stashes = document["stashes"];
    // For each stash
    for ( rapidjson::SizeType i = 0; i < stashes.Size(); i++ ) {
        const rapidjson::Value& array   = stashes[i];
        std::string account_name        = "";
        if ( array.HasMember( "accountName" ) && !array["accountName"].IsNull()) {
            assert(array["accountName"].IsString());
            account_name = array["accountName"].GetString();
        } else {
            continue;
        }
        std::string last_character_name = array["lastCharacterName"].GetString();
        std::string stash_id            = array["id"].GetString();
        std::string stash_name          = array["stash"].GetString();
        std::string stash_type          = array["stashType"].GetString();
        bool public_stash               = array["public"].GetBool();
        const rapidjson::Value& items   = array["items"];
        int timestamp                   = get_current_timestamp().count();
        // If stash is updated, the account is online
        try {
            account_stmt->setString( 1, account_name );
            account_stmt->setString( 2, last_character_name );
            account_stmt->setInt(    3, timestamp );
            account_stmt->setInt(    4, timestamp );
            account_stmt->setString( 5, last_character_name );
            account_stmt->execute();
            // delete stmt;
            // stmt->execute( "INSERT INTO `Accounts` (`accountName`, `lastCharacterName`, `lastSeen`) VALUES ('" + account_name + "', '" + last_character_name + "', '" + timestamp + "') ON DUPLICATE KEY UPDATE `lastSeen` = '" + timestamp + "', `lastCharacterName` = '" + last_character_name + "'" ); 
        } catch ( sql::SQLException &e ) {
            errors++;
            print_sql_error( e );
        }

        /* Create a new stash in the DB, update the stash name, stash 
           type and public status if the stash ID already exists */
        try {
            stash_stmt->setString( 1, stash_id );
            stash_stmt->setString( 2, stash_name );
            stash_stmt->setString( 3, stash_type );
            stash_stmt->setString( 4, public_stash ? "1" : "0" );
            stash_stmt->setString( 5, stash_name );
            stash_stmt->setString( 6, stash_type );
            stash_stmt->setString( 7, public_stash ? "1" : "0" );
            stash_stmt->execute();
            // delete stmt;
            // stmt->execute( "INSERT INTO `Stashes` (`stashId`, `stashName`, `stashType`, `publicStash`) VALUES ('" + stash_id + "', \"" + stash_name + "\", '" + stash_type + "', '" + ( public_stash ? "1" : "0" ) + "') ON DUPLICATE KEY UPDATE `stashName` = \"" + stash_name + "\", `stashType` = '" + stash_type + "', `publicStash` = '" + ( public_stash ? "1" : "0" ) + "'" ); 
        } catch ( sql::SQLException &e ) {
            errors++;
            print_sql_error( e );
        }

        // Get previously stored stash contents
        std::vector<Item> previous_items = get_stash_by_ID( stash_id );

        /* If the stash does not exist or there are no stored items, 
           store all items */
        if ( previous_items.size() == 0 ) {
            // std::cout << std::endl << "Stash " + stash_id 
                    //   << " does not exist, creating it" << std::endl;
            // For each item in the stash
            for ( rapidjson::SizeType j = 0; j < items.Size(); j++ ) {
                const rapidjson::Value& item = items[j];
                if ( item.IsObject()) {
                    bool verified              = item["verified"].GetBool();
                    int w                      = item["w"].GetInt();
                    int h                      = item["h"].GetInt();
                    int ilvl                   = item["ilvl"].GetInt();
                    std::string icon           = item["icon"].GetString();
                    std::string league         = item["league"].GetString();
                    std::string poe_trade_id   = league;
                    std::replace( poe_trade_id.begin(), poe_trade_id.end(), ' ', '+');
                    /* Insert the league of the item in the DB, no update if the 
                    league already exists */
                    if ( j == 0 ) {
                        try {
                            league_stmt->setString( 1, league );
                            league_stmt->setString( 2, poe_trade_id );
                            league_stmt->execute();
                            // delete stmt;
                            // stmt->execute( "INSERT INTO `Leagues` (`leagueName`, `active`, `poeTradeId`) VALUES ('" + league + "', '1', '" + poe_trade_id + "') ON DUPLICATE KEY UPDATE `leagueName` = `leagueName`" );
                        } catch ( sql::SQLException &e ) {
                            errors++;
                            print_sql_error( e );
                        }
                    }
                    std::string item_id        = item["id"].GetString();
                    std::string item_name      = replace_string( 
                        item["name"].GetString(), "<<set:MS>><<set:M>><<set:S>>", "" );
                    std::string type_line      = replace_string(
                        item["typeLine"].GetString(), "<<set:MS>><<set:M>><<set:S>>", "" );
                    bool identified            = item["identified"].GetBool();
                    bool corrupted             = item["corrupted"].GetBool();
                    bool locked                = item["lockedToCharacter"].GetBool();
                    std::string note;
                    std::string flavour_text;
                    int frame_type = 0;
                    if ( item.HasMember( "note" )) {
                        note = item["note"].GetString();
                    }
                    std::string price;
                    if ( note.compare( "" ) != 0 ) {
                        price = note;
                    } else {
                        price = stash_name;
                    }
                    // if ( item.HasMember( "flavourText" )) {
                    //     flavour_text = item["flavourText"].GetString();
                    // }
                    if ( item.HasMember( "frameType" )) {
                        frame_type = item["frameType"].GetInt();
                    }

                    int x                      = item["x"].GetInt();
                    int y                      = item["y"].GetInt();
                    std::string inventory_id   = item["inventoryId"].GetString();

                    bool crafted = false;
                    if ( item.HasMember( "craftedMods" )) {
                        const rapidjson::Value& mods = item["craftedMods"];
                        crafted = mods.Size() > 0;
                    }
                    bool enchanted = false;
                    if ( item.HasMember( "enchantMods" )) {
                        const rapidjson::Value& mods = item["enchantMods"];
                        enchanted = mods.Size() > 0;
                    }
                    
                    int link_amount = 0;
                    
                    const rapidjson::Value& sockets = item["sockets"];
                    int socket_amount = sockets.Size();

                    begin = std::chrono::steady_clock::now();
                    try {
                        item_stmt->setInt(    1, w );
                        item_stmt->setInt(    2, h );
                        item_stmt->setInt(    3, ilvl );
                        item_stmt->setString( 4, icon );
                        item_stmt->setString( 5, league );
                        item_stmt->setString( 6, item_id );
                        item_stmt->setString( 7, item_name );
                        item_stmt->setString( 8, type_line );
                        item_stmt->setInt(    9, identified );
                        item_stmt->setInt(    10, verified );
                        item_stmt->setInt(    11, crafted );
                        item_stmt->setInt(    12, enchanted );
                        item_stmt->setInt(    13, corrupted );
                        item_stmt->setInt(    14, locked );
                        item_stmt->setInt(    15, frame_type );
                        item_stmt->setInt(    16, x );
                        item_stmt->setInt(    17, y );
                        item_stmt->setString( 18, inventory_id );
                        item_stmt->setString( 19, account_name );
                        item_stmt->setString( 20, stash_id );
                        item_stmt->setInt(    21, socket_amount );
                        item_stmt->setInt(    22, link_amount );
                        item_stmt->setInt(    23, timestamp );
                        item_stmt->setInt(    24, timestamp );
                        item_stmt->setString( 25, flavour_text );
                        item_stmt->setString( 26, price );
                        item_stmt->setString( 27, item_name );
                        item_stmt->setInt(    28, verified );
                        item_stmt->setInt(    29, crafted );
                        item_stmt->setInt(    30, enchanted );
                        item_stmt->setInt(    31, corrupted );
                        item_stmt->setInt(    32, x );
                        item_stmt->setInt(    33, y );
                        item_stmt->setString( 34, inventory_id );
                        item_stmt->setString( 35, account_name );
                        item_stmt->setString( 36, stash_id );
                        item_stmt->setInt(    37, socket_amount );
                        item_stmt->setInt(    38, link_amount );
                        item_stmt->setInt(    39, timestamp );
                        item_stmt->setString( 40, price );
                        item_stmt->execute();
                        // delete stmt;
                        // stmt->execute( "INSERT INTO `Items` (`w`, `h`, `ilvl`, `icon`, `league`, `itemId`, `name`, `typeLine`, `identified`, `verified`, `crafted`, `enchanted`, `corrupted`, `lockedToCharacter`, `frameType`, `x`, `y`, `inventoryId`, `accountName`, `stashId`, `socketAmount`, `linkAmount`, `available`, `addedTs`, `updatedTs`, `flavourText`, `price`) VALUES ('" + 
                        // std::to_string(w) + "', '" + std::to_string(h) + "', '" + 
                        // std::to_string(ilvl) + "', '" + icon + "', '" + league + 
                        // "', '" + item_id + "', \"" + item_name + "\", \"" + type_line + 
                        // "\", '" + ( identified ? "1" : "0" ) + "', '" + 
                        // ( verified ? "1" : "0" ) +  "', '" + 
                        // ( crafted ? "1" : "0" ) + "', '" + 
                        // ( enchanted ? "1" : "0" ) + "', '" + 
                        // ( corrupted ? "1" : "0" ) + "', '" + 
                        // ( locked ? "1" : "0" ) + "', '" + std::to_string(frame_type) + 
                        // "', '" + std::to_string(x) + "', '" + std::to_string(y) + 
                        // "', '" + inventory_id + "', '" + account_name + "', '" + 
                        // stash_id + "', '" + std::to_string(socket_amount) + 
                        // "', '" + std::to_string(link_amount) + "', '1', '" + 
                        // timestamp + "', '" + 
                        // timestamp + "', '" + flavour_text + "', \"" + price + "\") " + 
                        // "ON DUPLICATE KEY UPDATE `name` = \"" + item_name + 
                        // "\", `verified` = '" + ( verified ? "1" : "0" ) + 
                        // "', `crafted` = '" + ( crafted ? "1" : "0" ) + 
                        // "', `enchanted` = '" + ( enchanted ? "1" : "0" ) + 
                        // "', `corrupted` = '" + ( corrupted ? "1" : "0" ) + 
                        // "', `x` = '" + std::to_string(x) + 
                        // "', `y` = '" + std::to_string(y) + 
                        // "', `inventoryId` = '" + inventory_id + 
                        // "', `accountName` = '" + account_name + 
                        // "', `stashId` = '" + stash_id + 
                        // "', `socketAmount` = '" + std::to_string(socket_amount) + 
                        // "', `linkAmount` = '" + std::to_string(link_amount) + 
                        // "', `available` = '1', `updatedTs` = '" + timestamp + 
                        // "', `price` = '" + price + "'" );
                        item_added++;
                    } catch ( sql::SQLException &e ) {
                        errors++;
                        print_sql_error( e );
                    }
                    end = std::chrono::steady_clock::now();
                    time_item += ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );

                    // Parse mods
                    begin = std::chrono::steady_clock::now();
                    int counter_mods = 0;
                    // Regex to extract numerical values
                    const std::regex re( "([0-9.]+)" );
                    // assert(item["explicitMods"].IsObject());
                    if ( item.HasMember( "explicitMods" )) {
                        const rapidjson::Value& mods = item["explicitMods"];
                        for ( rapidjson::SizeType k = 0; k < mods.Size(); k++ ) {
                            if ( !mods[k].IsNull()) {
                                counter_mods++;
                                assert( mods[k].IsString());
                                std::string mod = mods[k].GetString();
                                // Replace numerical values by '#' to normalize
                                std::string name = std::regex_replace( mod, re, "#" );
                                std::smatch sm;
                                std::vector<std::string> values = std::vector<std::string>();
                                values.assign( 4, "" );

                                std::string::const_iterator searchStart( mod.cbegin());
                                int index = 0;
                                /* Extract numerical values for row mod and store
                                   them into values vector */
                                while ( regex_search( searchStart, mod.cend(), sm, re )) {
                                    values[index] = sm[0];
                                    searchStart += sm.position() + sm.length();
                                    index++;
                                }
                                // Insert mods into database
                                try {
                                    mod_stmt->setString( 1, item_id );
                                    mod_stmt->setString( 2, name );
                                    mod_stmt->setString( 3, values[0]);
                                    mod_stmt->setString( 4, values[1]);
                                    mod_stmt->setString( 5, values[2]);
                                    mod_stmt->setString( 6, values[3]);
                                    mod_stmt->setString( 7, item_id + "_" + std::to_string(counter_mods));
                                    mod_stmt->setString( 8, name );
                                    mod_stmt->setString( 9, values[0]);
                                    mod_stmt->setString( 10, values[1]);
                                    mod_stmt->setString( 11, values[2]);
                                    mod_stmt->setString( 12, values[3]);
                                    mod_stmt->execute();
                                    // delete stmt;
                                    // stmt->execute( "INSERT INTO `Mods` (`itemId`, `modName`, `modValue1`, `modValue2`, `modValue3`, `modValue4`, `modType`, `modKey`) VALUES ('" +
                                    // item_id + "', \"" + name + "\", '" + values[0] + "', '" + values[1] + "', '" + values[2] + "', '" + values[3] + "', 'EXPLICIT', '" + 
                                    // ( item_id + "_" + std::to_string(counter_mods)) + "') ON DUPLICATE KEY UPDATE `modName` = \"" + name + "\", `modValue1` = '" + values[0] + "', `modValue2` = '" + values[1] + "', `modValue3` = '" + values[2] + "', `modValue4` = '" + values[3] + "', `modType` = 'EXPLICIT'" );
                                } catch ( sql::SQLException &e ) {
                                    print_sql_error( e );
                                }
                            }
                        }
                    }
                    counter_mods = 0;
                    if ( item.HasMember( "implicitMods" )) {
                        const rapidjson::Value& mods = item["implicitMods"];
                        for ( rapidjson::SizeType k = 0; k < mods.Size(); k++ ) {
                            if ( !mods[k].IsNull()) {
                                counter_mods++;
                                assert( mods[k].IsString());
                                std::string mod = mods[k].GetString();
                                // Replace numerical values by '#' to normalize
                                std::string name = std::regex_replace( mod, re, "#" );
                                std::smatch sm;
                                std::vector<std::string> values = std::vector<std::string>();
                                values.assign( 4, "" );

                                std::string::const_iterator searchStart( mod.cbegin());
                                int index = 0;
                                /* Extract numerical values for row mod and store
                                   them into values vector */
                                while ( regex_search( searchStart, mod.cend(), sm, re )) {
                                    values[index] = sm[0];
                                    searchStart += sm.position() + sm.length();
                                    index++;
                                }
                                // Insert mods into database
                                try {
                                    mod_stmt->setString( 1, item_id );
                                    mod_stmt->setString( 2, name );
                                    mod_stmt->setString( 3, values[0]);
                                    mod_stmt->setString( 4, values[1]);
                                    mod_stmt->setString( 5, values[2]);
                                    mod_stmt->setString( 6, values[3]);
                                    mod_stmt->setString( 7, item_id + "_" + std::to_string(counter_mods));
                                    mod_stmt->setString( 8, name );
                                    mod_stmt->setString( 9, values[0]);
                                    mod_stmt->setString( 10, values[1]);
                                    mod_stmt->setString( 11, values[2]);
                                    mod_stmt->setString( 12, values[3]);
                                    mod_stmt->execute();
                                    // delete stmt;
                                } catch ( sql::SQLException &e ) {
                                    print_sql_error( e );
                                }
                            }
                        }
                    }
                    counter_mods = 0;
                    if ( item.HasMember( "craftedMods" )) {
                        const rapidjson::Value& mods = item["craftedMods"];
                        for ( rapidjson::SizeType k = 0; k < mods.Size(); k++ ) {
                            if ( !mods[k].IsNull()) {
                                counter_mods++;
                                assert( mods[k].IsString());
                                std::string mod = mods[k].GetString();
                                // Replace numerical values by '#' to normalize
                                std::string name = std::regex_replace( mod, re, "#" );
                                std::smatch sm;
                                std::vector<std::string> values = std::vector<std::string>();
                                values.assign( 4, "" );

                                std::string::const_iterator searchStart( mod.cbegin());
                                int index = 0;
                                /* Extract numerical values for row mod and store
                                   them into values vector */
                                while ( regex_search( searchStart, mod.cend(), sm, re )) {
                                    values[index] = sm[0];
                                    searchStart += sm.position() + sm.length();
                                    index++;
                                }
                                // Insert mods into database
                                try {
                                    mod_stmt->setString( 1, item_id );
                                    mod_stmt->setString( 2, name );
                                    mod_stmt->setString( 3, values[0]);
                                    mod_stmt->setString( 4, values[1]);
                                    mod_stmt->setString( 5, values[2]);
                                    mod_stmt->setString( 6, values[3]);
                                    mod_stmt->setString( 7, item_id + "_" + std::to_string(counter_mods));
                                    mod_stmt->setString( 8, name );
                                    mod_stmt->setString( 9, values[0]);
                                    mod_stmt->setString( 10, values[1]);
                                    mod_stmt->setString( 11, values[2]);
                                    mod_stmt->setString( 12, values[3]);
                                    mod_stmt->execute();
                                    // delete stmt;
                                } catch ( sql::SQLException &e ) {
                                    print_sql_error( e );
                                }
                            }
                        }
                    }
                    counter_mods = 0;
                    if ( item.HasMember( "enchantMods" )) {
                        const rapidjson::Value& mods = item["enchantMods"];
                        for ( rapidjson::SizeType k = 0; k < mods.Size(); k++ ) {
                            if ( !mods[k].IsNull()) {
                                counter_mods++;
                                assert( mods[k].IsString());
                                std::string mod = mods[k].GetString();
                                // Replace numerical values by '#' to normalize
                                std::string name = std::regex_replace( mod, re, "#" );
                                std::smatch sm;
                                std::vector<std::string> values = std::vector<std::string>();
                                values.assign( 4, "" );

                                std::string::const_iterator searchStart( mod.cbegin());
                                int index = 0;
                                /* Extract numerical values for row mod and store
                                   them into values vector */
                                while ( regex_search( searchStart, mod.cend(), sm, re )) {
                                    values[index] = sm[0];
                                    searchStart += sm.position() + sm.length();
                                    index++;
                                }
                                // Insert mods into database
                                try {
                                    mod_stmt->setString( 1, item_id );
                                    mod_stmt->setString( 2, name );
                                    mod_stmt->setString( 3, values[0]);
                                    mod_stmt->setString( 4, values[1]);
                                    mod_stmt->setString( 5, values[2]);
                                    mod_stmt->setString( 6, values[3]);
                                    mod_stmt->setString( 7, item_id + "_" + std::to_string(counter_mods));
                                    mod_stmt->setString( 8, name );
                                    mod_stmt->setString( 9, values[0]);
                                    mod_stmt->setString( 10, values[1]);
                                    mod_stmt->setString( 11, values[2]);
                                    mod_stmt->setString( 12, values[3]);
                                    mod_stmt->execute();
                                    // delete stmt;
                                } catch ( sql::SQLException &e ) {
                                    print_sql_error( e );
                                }
                            }
                        }
                    }
                    end = std::chrono::steady_clock::now();
                    time_mods += ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );

                    int counter = 0;
                    begin = std::chrono::steady_clock::now();
                    // Parse sockets
                    for ( rapidjson::SizeType k = 0; k < sockets.Size(); k++ ) {
                        if ( !sockets[k].IsNull()) {
                                counter++;
                                assert( sockets[k].IsObject());
                                const rapidjson::Value& socket = sockets[k];
                                int         group = socket["group"].GetInt();
                                assert(socket["attr"].IsString());
                                std::string attr  = socket["attr"].GetString();

                                // Insert sockets into database
                                try {
                                    socket_stmt->setString( 1, item_id );
                                    socket_stmt->setInt(    2, group );
                                    socket_stmt->setString( 3, attr );
                                    socket_stmt->setString( 4, item_id + "_" + std::to_string(counter));
                                    socket_stmt->setInt(    5, group );
                                    socket_stmt->setString( 6, attr );
                                    socket_stmt->execute();
                                    // delete stmt;
                                    // stmt->execute( "INSERT INTO `Sockets` (`itemId`, `socketGroup`, `socketAttr`, `socketKey`) VALUES ('" +
                                    // item_id + "', '" + std::to_string(group) + "', '" + attr + "', '" +
                                    // ( item_id + "_" + std::to_string(counter)) + "') ON DUPLICATE KEY UPDATE `socketGroup` = '" + std::to_string(group) + "', `socketAttr` = '" + attr + "'" );
                                } catch ( sql::SQLException &e ) {
                                    print_sql_error( e );
                                }
                            }
                    }
                    end = std::chrono::steady_clock::now();
                    time_sockets += ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );

                    // Parse properties
                    begin = std::chrono::steady_clock::now();
                    counter = 0;
                    if ( item.HasMember( "properties" )) {
                        const rapidjson::Value& properties = item["properties"];
                        for ( rapidjson::SizeType k = 0; k < properties.Size(); k++ ) {
                            if ( !properties[k].IsNull()) {
                                counter++;
                                assert( properties[k].IsObject());
                                std::vector<std::string> insert_values = std::vector<std::string>();
                                insert_values.assign( 2, "" );
                                const rapidjson::Value& property = properties[k];
                                assert(property["name"].IsString());
                                std::string name = property["name"].GetString();
                                const rapidjson::Value& values = property["values"];
                                if ( values.Size() > 0 ) {
                                    const rapidjson::Value& values_inner = values[0];
                                    assert(values_inner[0].IsString());
                                    std::string value = values_inner[0].GetString();
                                    for ( rapidjson::SizeType l = 0; l < values_inner.Size(); l++ ) {
                                        if ( !values_inner[l].IsString()) {
                                            insert_values.push_back( std::to_string( values_inner[l].GetInt()));
                                        } else {
                                            insert_values.push_back( values_inner[l].GetString());
                                        }
                                    }
                                }

                                // Insert property into database
                                try {
                                    property_stmt->setString( 1, item_id );
                                    property_stmt->setString( 2, name );
                                    property_stmt->setString( 3, insert_values[0]);
                                    property_stmt->setString( 4, insert_values[1]);
                                    property_stmt->setString( 5, item_id + "_" + std::to_string(counter));
                                    property_stmt->setString( 6, name );
                                    property_stmt->setString( 7, insert_values[0]);
                                    property_stmt->setString( 8, insert_values[1]);
                                    property_stmt->execute();
                                    // delete stmt;
                                    // stmt->execute( "INSERT INTO `Properties` (`itemId`, `propertyName`, `propertyValue1`, `propertyValue2`, `propertyKey`) VALUES ('" +
                                    // item_id + "', \"" + name + "\", '" + insert_values[0] + "', '" + insert_values[1] + "', '" +
                                    // ( item_id + "_" + std::to_string(counter)) + "') ON DUPLICATE KEY UPDATE `propertyName` = \"" + name + "\", `propertyValue1` = '" + insert_values[0] + "', `propertyValue2` = '" + insert_values[1] + "'" );
                                } catch ( sql::SQLException &e ) {
                                    print_sql_error( e );
                                }
                            }
                        }
                    }
                    
                    // Parse additional properties
                    begin = std::chrono::steady_clock::now();
                    counter = 0;
                    if ( item.HasMember( "additionalProperties" )) {
                        const rapidjson::Value& add_properties = item["additionalProperties"];
                        for ( rapidjson::SizeType k = 0; k < add_properties.Size(); k++ ) {
                            if ( !add_properties[k].IsNull()) {
                                counter++;
                                assert( add_properties[k].IsObject());
                                std::vector<std::string> insert_values = std::vector<std::string>();
                                insert_values.assign( 2, "" );
                                const rapidjson::Value& property = add_properties[k];
                                assert(property["name"].IsString());
                                std::string name = property["name"].GetString();
                                const rapidjson::Value& values = property["values"];
                                if ( values.Size() > 0 ) {
                                    const rapidjson::Value& values_inner = values[0];
                                    assert(values_inner[0].IsString());
                                    std::string value = values_inner[0].GetString();
                                    for ( rapidjson::SizeType l = 0; l < values_inner.Size(); l++ ) {
                                        if ( !values_inner[l].IsString()) {
                                            insert_values.push_back( std::to_string( values_inner[l].GetInt()));
                                        } else {
                                            insert_values.push_back( values_inner[l].GetString());
                                        }
                                    }
                                }

                                // Insert property into database
                                try {
                                    property_stmt->setString( 1, item_id );
                                    property_stmt->setString( 2, name );
                                    property_stmt->setString( 3, insert_values[0]);
                                    property_stmt->setString( 4, insert_values[1]);
                                    property_stmt->setString( 5, item_id + "_" + std::to_string(counter));
                                    property_stmt->setString( 6, name );
                                    property_stmt->setString( 7, insert_values[0]);
                                    property_stmt->setString( 8, insert_values[1]);
                                    property_stmt->execute();
                                    // delete stmt;
                                    // stmt->execute( "INSERT INTO `Properties` (`itemId`, `propertyName`, `propertyValue1`, `propertyValue2`, `propertyKey`) VALUES ('" +
                                    // item_id + "', \"" + name + "\", '" + insert_values[0] + "', '" + insert_values[1] + "', '" +
                                    // ( item_id + "_" + std::to_string(counter)) + "') ON DUPLICATE KEY UPDATE `propertyName` = \"" + name + "\", `propertyValue1` = '" + insert_values[0] + "', `propertyValue2` = '" + insert_values[1] + "'" );
                                } catch ( sql::SQLException &e ) {
                                    print_sql_error( e );
                                }
                            }
                        }
                    }
                    end = std::chrono::steady_clock::now();
                    time_properties += ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );

                    // Parse requirements
                    begin = std::chrono::steady_clock::now();
                    counter = 0;
                    if ( item.HasMember( "requirements" )) {
                        const rapidjson::Value& requirements = item["requirements"];
                        for ( rapidjson::SizeType k = 0; k < requirements.Size(); k++ ) {
                            if ( !requirements[k].IsNull()) {
                                counter++;
                                assert( requirements[k].IsObject());
                                const rapidjson::Value& requirement = requirements[k];
                                assert(requirement["name"].IsString());
                                std::string name = requirement["name"].GetString();
                                const rapidjson::Value& values = requirement["values"];
                                const rapidjson::Value& values_inner = values[0];
                                assert(values_inner[0].IsString());
                                std::string value = values_inner[0].GetString();

                                // Insert requirements into database
                                try {
                                    requirement_stmt->setString( 1, item_id );
                                    requirement_stmt->setString( 2, name );
                                    requirement_stmt->setString( 3, value );
                                    requirement_stmt->setString( 4, item_id + "_" + std::to_string(counter));
                                    requirement_stmt->setString( 5, name );
                                    requirement_stmt->setString( 6, value );
                                    requirement_stmt->execute();
                                    // delete stmt;
                                    // stmt->execute( "INSERT INTO `Requirements` (`itemId`, `requirementName`, `requirementValue`, `requirementKey`) VALUES ('" +
                                    // item_id + "', \"" + name + "\", '" + value + "', '" +
                                    // ( item_id + "_" + std::to_string(counter)) + "') ON DUPLICATE KEY UPDATE `requirementName` = \"" + name + "\", `requirementValue` = '" + value + "'" );
                                } catch ( sql::SQLException &e ) {
                                    print_sql_error( e );
                                }
                            }
                        }
                    }
                    end = std::chrono::steady_clock::now();
                    time_requirements += ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );
                }
            }
        } else {
            // std::cout << std::endl << "Stash already exists" << std::endl;
        }
        // printf("%s, %s, %s, %s, %s, %s \n", account_name.c_str(), last_character_name.c_str(), id.c_str(), stash_name.c_str(), stash_type.c_str(), public_stash.c_str());
    }
    try {
        // stmt = con->prepareStatement( "COMMIT" );
        // stmt->execute();
        // delete stmt;
    } catch ( sql::SQLException &e ) {
        print_sql_error( e );
    }

    // delete stmt;
    delete account_stmt;
    delete stash_stmt;
    delete league_stmt;
    delete item_stmt;
    delete mod_stmt;
    delete socket_stmt;
    delete property_stmt;
    delete requirement_stmt;
    delete con;
}

void query( std::string str ) {
    try {
        sql::mysql::MySQL_Driver *driver;
        sql::Connection *con;
        sql::Statement *stmt;

        driver = sql::mysql::get_mysql_driver_instance();
        con    = driver->connect( DB_HOST + ":" + DB_PORT, DB_USER, DB_PASS );

        stmt = con->createStatement();
        stmt->execute( "USE POE"  );
        stmt->execute( str );

        delete stmt;
        delete con;
    } catch ( sql::SQLException &e ) {
        print_sql_error( e );
    }
}

void download_loop() {
    rapidjson::Document document;

    while ( !interrupt ) {
        // Download the next change id
        std::string path = download_JSON( next_change_id );

        // Read JSON file to extract next change id
        std::ifstream file( path.c_str() );
        std::stringstream sstr;
        sstr << file.rdbuf();
        document.Parse( sstr.str().c_str());
        // assert(document.IsObject());
        // assert(document["next_change_id"].IsNull());
        // If document is not valid, do not change the change id
        if ( !document.IsObject() || document["next_change_id"].IsNull()) {
            std::cout << stamp( __FUNCTION__ ) << "Change ID " << next_change_id << " empty" 
                      << std::endl;
            usleep( 1000 );
            continue;
        }
        const rapidjson::Value& change_id = document["next_change_id"];
        next_change_id = (char*) change_id.GetString();

        // Store the id in the DB
        try {
            sql::mysql::MySQL_Driver *driver;
            sql::Connection *con;
            sql::Statement *stmt;

            driver = sql::mysql::get_mysql_driver_instance();
            con    = driver->connect( DB_HOST + ":" + DB_PORT, DB_USER, DB_PASS );

            stmt = con->createStatement();
            stmt->execute( "USE POE"  );
            std::cout << stamp( __FUNCTION__ ) << "Adding chunk ID to DB" << std::endl;
            stmt->execute( "INSERT INTO `ChangeId` (`nextChangeId`) VALUES ('" + next_change_id + "')" );

            delete stmt;
            delete con;
        } catch ( sql::SQLException &e ) {
            print_sql_error( e );
        }
    }
}

void processing_loop() {
    while ( !interrupt ) {
        std::deque<std::string>::iterator it = downloaded_files.begin();
        while ( it != downloaded_files.end() && !interrupt ) {
            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
            item_added   = 0;
            item_updated = 0;
            item_removed = 0;
            errors       = 0;
            // Parse the JSON data
            parse_JSON( *it );
            // Set this change ID has processed and delete the data file
            query( "UPDATE `ChangeId` SET `processed` = '1' WHERE `nextChangeId` = '" + *it + "'" );
            std::string path = download_dir + "indexer_" + *it + ".json";
            // std::cout << std::endl << "Removing " << path << std::endl;
            std::remove( path.c_str());
            total_item_added    += item_added;
            total_item_updated  += item_updated;
            total_item_removed  += item_removed;
            total_errors        += errors;
            std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            int sum = item_added + item_updated + item_removed;
            float time_sec = ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );
            total_sum += sum;
            total_time += time_sec;
            float speed = sum / time_sec;
            float total_speed = total_sum / total_time;
            std::cout << stamp( __FUNCTION__ ) << "Entries total: " 
                      << sum << ", added: " 
                      << item_added << ", removed: " << item_removed << ", updated: " 
                      << item_updated << ", insert errors: " << errors
                      << " over " << time_sec << " sec at " << speed 
                      << " insert/s" << std::endl;
            std::cout << stamp( __FUNCTION__ ) << "Time profile: "
                      << "item: " << time_item << " sec (" << time_item / sum * 1000 << " ms), "
                      << "mods: " << time_mods << " sec (" << time_mods / sum * 1000 << " ms), "
                      << "properties: " << time_properties << " sec (" << time_properties / sum * 1000 << " ms), "
                      << "sockets: " << time_sockets << " sec (" << time_sockets / sum * 1000 << " ms), "
                      << "requirements: " << time_requirements << " sec (" << time_requirements / sum * 1000 << " ms)"
                      << std::endl;
            std::cout << stamp( __FUNCTION__ ) 
                      << "Total entries processed: " << total_sum 
                      << ", added: " << total_item_added << ", removed: " << total_item_removed << ", updated: " 
                      << total_item_updated << ", insert errors: " << total_errors
                      << " over " << total_time << " sec at " << total_speed 
                      << " insert/s" << std::endl;
            *it++;
        }
        if ( !interrupt ) {
            std::cout << stamp( __FUNCTION__ ) << "All files have been processed" 
                      << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds( 1000 ));
    }
}

/**
 * Handler for interrupt signal
 *
 * @param Signal s
 * @return Nothing
 */
void cleanup( int s ) {
    interrupt = true;
    std::cout << stamp( __FUNCTION__ ) << RED 
              << "Caught interrupt signal, exiting gracefully" 
              << RESET << std::endl;
}

int main() {

    std::thread download_thread; // JSON download thread
    std::thread processing_thread; // JSON processing thread

    // Catch interrupt signal
    struct sigaction sig_int_handler;

    sig_int_handler.sa_handler = cleanup;
    sigemptyset( &sig_int_handler.sa_mask );
    sig_int_handler.sa_flags = 0;
    sigaction( SIGINT, &sig_int_handler, NULL );

    // init next change id
    std::cout << stamp( __FUNCTION__ ) << "Checking last downloaded chunk" << std::endl;
    next_change_id = last_downloaded_chunk();
    if ( next_change_id.compare( "" ) != 0 ) {
        if ( next_change_id.compare( "-1" ) == 0 ) {
            std::cout << stamp( __FUNCTION__ ) << "New indexation: " 
                      << std::endl;
            download_thread = std::thread( download_loop );
        } else {
            std::cout << stamp( __FUNCTION__ ) << "Next change id: " 
                      << next_change_id << std::endl;
            std::cout << stamp( __FUNCTION__ ) 
                      << downloaded_files.size() << " files to be processed" 
                      << std::endl;
            download_thread = std::thread( download_loop );
        }
        processing_thread = std::thread( processing_loop );
    } else {
        std::cout << stamp( __FUNCTION__ ) 
                  << "There was an error fetching next change id" << std::endl;
    }

    download_thread.join();
    processing_thread.join();

    // std::vector<Item> items = get_stash_by_ID( "af89bb1d9d37995056a33521ec22d0d63a2d70e659a0cd5e449d00fa01dd34ea" );
    // std::cout << std::endl << "Got " << items.size() << " items" << std::endl;
    return 0;
}