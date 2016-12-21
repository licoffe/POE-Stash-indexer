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
#include <cmath>
#include <mutex>
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
#include "cfg_reader.h"
#include "main.h"

#define FUNCTION __FUNCTION__

struct Stash_differences {
    std::vector<Item> added;
    std::vector<Item> removed;
    std::vector<Item> kept;
};

struct Time {
    float amount;
    std::string unit;
};

struct Mod {
    std::string item_id;
    std::string name;
    std::string value1;
    std::string value2;
    std::string value3;
    std::string value4;
    std::string type;
    std::string mod_key;
};

struct Property {
    std::string item_id;
    std::string name;
    std::string value1;
    std::string value2;
    std::string property_key;
};

struct Requirement {
    std::string item_id;
    std::string name;
    std::string value;
    std::string requirement_key;
};

struct Socket {
    std::string item_id;
    int group;
    std::string attr;
    std::string socket_key;
};

const std::string URL          = "http://api.pathofexile.com/public-stash-tabs";
const std::string download_dir = "./data/";
std::string next_change_id;
std::deque<std::string> downloaded_files = std::deque<std::string>();
CFG_reader reader          = CFG_reader( "./config.cfg" );
const std::string DB_HOST  = reader.get( "DB_HOST" );
const std::string DB_PORT  = reader.get( "DB_PORT" );
const std::string DB_USER  = reader.get( "DB_USER" );
const std::string DB_PASS  = reader.get( "DB_PASS" );
const std::string DB_NAME  = reader.get( "DB_NAME" );
const int QUEUE_SIZE       = 10; // How many files should be downloaded ahead
bool interrupt             = false;
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
float time_other           = 0.0;
float time_loading_JSON    = 0.0;
sql::mysql::MySQL_Driver   *driver;
sql::Connection            *download_con;
sql::Connection            *processing_con;
std::mutex                 queue_mutex;

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

    if ( change_id.compare( "" ) == 0 ) {
        path = std::string( download_dir + "indexer_first.json" );
    } else {
        path = std::string( download_dir + "indexer_" + change_id + ".json" );
    }

    const char* outfilename = path.c_str();
    curl = curl_easy_init();
    if ( curl ) {
        // std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        fp = fopen( outfilename, "wb" );
        curl_easy_setopt( curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, "gzip");
        curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, write_data );
        curl_easy_setopt( curl, CURLOPT_WRITEDATA, fp );
        res = curl_easy_perform( curl );
        /* always cleanup */
        curl_easy_cleanup( curl );
        fclose( fp );
        // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        // std::cout << stamp( __FUNCTION__ ) << "Downloaded " << change_id 
                //   << " (" << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0
                //   << "sec )" << std::endl;
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
        sql::Statement *stmt;
        sql::ResultSet  *res;

        stmt = download_con->createStatement();
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
        sql::Statement *stmt;
        sql::ResultSet  *res;

        stmt = processing_con->createStatement();
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
    } catch ( sql::SQLException &e ) {
        print_sql_error( e );
        return results;
    }
    return results;
};

/**
 * Computes the amount of links of an item
 *
 * @param socket array
 * @return amount of links
 */
int get_links_amount( const rapidjson::Value& sockets ) {
    std::vector<int> amounts = std::vector<int>();
    int current_group = -1;
    // For each socket
    for ( rapidjson::SizeType k = 0; k < sockets.Size(); k++ ) {
        if ( !sockets[k].IsNull()) {
            assert( sockets[k].IsObject());
            const rapidjson::Value& socket = sockets[k];
            // If group is not referenced, create a new entry
            if ( current_group != socket["group"].GetInt() || current_group == -1 ) {
                amounts.push_back(0);
                current_group = socket["group"].GetInt();
            // Otherwise, increment
            } else {
                amounts[current_group]++;
            }
        }
    }
    // Return the entry with the maximum amount
    int amount = 0;
    for ( std::vector<int>::iterator it = amounts.begin() ; it != amounts.end() ; ++it ) {
        if ( amount < *it ) {
            amount = *it;
        }
    }
    return amount;
};

/**
 * Compare two stashes, returning a struct containing three vectors: 
 * added, removed and kept items.
 *
 * @param old and new stashes
 * @return Struct with stash differences
 */
Stash_differences compare_stashes( std::vector<Item> old_stash, 
                                   std::vector<Item> new_stash ) {
    std::vector<Item> added                = std::vector<Item>();
    std::vector<Item> removed              = std::vector<Item>();
    std::vector<Item> kept                 = std::vector<Item>();
    std::map<std::string, bool> discovered = std::map<std::string, bool>();

    // For each item in old stash
    for ( std::vector<Item>::iterator it_old = old_stash.begin() ; 
          it_old != old_stash.end() ; ++it_old ) {
        bool found = false;

        // For each item in the new stash
        for ( std::vector<Item>::iterator it_new = new_stash.begin() ; 
              it_new != new_stash.end() ; ++it_new ) {
            /* If there is an item with the same item id, 
               add it to the kept items */
            if ( it_new->item_id.compare( it_old->item_id ) == 0 ) {
                if ( !discovered[it_new->item_id]) {
                    discovered[it_new->item_id] = true;
                }
                found = true;
                kept.push_back( *it_new );
                break;
            }
        }
            
        // If the item was not found, add it to the removed items
        if ( !found ) {
            removed.push_back( *it_old );
        }
    }

    /* Each item which is not marked as discovered has been added 
      with the new stash */
    for ( std::vector<Item>::iterator it_new = new_stash.begin() ; 
          it_new != new_stash.end() ; ++it_new ) {
        if ( !discovered[it_new->item_id]) {
            added.push_back( *it_new );
        }
    }

    Stash_differences differences = { added, removed, kept };
    return differences;
};

/**
 * Converts a millisecond amount to a higer unit (min, hour, day...) if possible.
 *
 * @param millisecond amount to convert
 * @return time struct with the converted value and corresponding unit
 */
struct::Time format_time( float time_ms ) {
    std::string units[] = { 
        "ms", "sec", "min", "hour(s)", "day(s)", "week(s)", "month(s)", "year(s)" 
    };
    int counter = 0;
    if ( time_ms > 1000 ) {
        time_ms /= 1000.0; // seconds
        counter = 1;
        if ( time_ms > 60 ) {
            time_ms /= 60.0; // minutes
            counter = 2;
            if ( time_ms > 60 ) {
                time_ms /= 60.0; // hours
                counter = 3;
                if ( time_ms > 24 ) {
                    time_ms /= 24.0; // days
                    counter = 4;
                    if ( time_ms > 365 ) {
                        time_ms /= 365.0; // years
                        counter = 6;
                    } else if ( time_ms > 30 ) {
                        time_ms /= 30.0; // month
                        counter = 5;
                    } else if ( time_ms > 7 ) {
                        time_ms /= 7.0; // weeks
                        counter++;
                    }
                }
            }
        }
    }
    Time time = { time_ms, units[counter]};
    return time;
};

void threaded_insert( std::string query ) {
    try {
        sql::mysql::MySQL_Driver *new_driver = sql::mysql::get_mysql_driver_instance();
        new_driver->threadInit();
        sql::Connection *insert_con;
        insert_con = new_driver->connect( DB_HOST + ":" + DB_PORT, DB_USER, DB_PASS );
        insert_con->setSchema( DB_NAME );
        sql::Statement *stmt;
        stmt = insert_con->createStatement();
        stmt->execute( query );
        delete stmt;
        delete insert_con;
        new_driver->threadEnd();
    } catch ( sql::SQLException &e ) {
        print_sql_error( e );
        threaded_insert( query );
    }
}

/**
 * Insert stashes, items, leagues, properties, requirements, sockets and mods
 *
 * @param Path to JSON file
 * @return Nothing
 */
void parse_JSON( std::string path ) {
    rapidjson::Document document;
    time_mods         = 0.0;
    time_properties   = 0.0;
    time_requirements = 0.0;
    time_sockets      = 0.0;
    time_item         = 0.0;
    time_other        = 0.0;
    std::vector<Mod> parsed_mods                 = std::vector<Mod>();
    std::vector<Requirement> parsed_requirements = std::vector<Requirement>();
    std::vector<Property> parsed_properties      = std::vector<Property>();
    std::vector<Socket> parsed_sockets           = std::vector<Socket>();

    // Read all JSON file
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
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
    time_loading_JSON = ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );
    std::cout << stamp( __FUNCTION__ ) << "Loaded: " << path << " in " 
              << time_loading_JSON << " sec" << std::endl;
    sql::Statement           *stmt = processing_con->createStatement();
    sql::PreparedStatement   *account_stmt = processing_con->prepareStatement( "INSERT INTO `Accounts` (`accountName`, `lastCharacterName`, `lastSeen`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `lastSeen` = ?, `lastCharacterName` = ?" );
    sql::PreparedStatement   *stash_stmt = processing_con->prepareStatement( "INSERT INTO `Stashes` (`stashId`, `stashName`, `stashType`, `publicStash`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `stashName` = ?, `stashType` = ?, `publicStash` = ?" ); 
    sql::PreparedStatement   *league_stmt = processing_con->prepareStatement( "INSERT INTO `Leagues` (`leagueName`, `active`, `poeTradeId`) VALUES (?, '1', ?) ON DUPLICATE KEY UPDATE `leagueName` = `leagueName`" );
    sql::PreparedStatement   *item_stmt = processing_con->prepareStatement( "INSERT INTO `Items` (`w`, `h`, `ilvl`, `icon`, `league`, `itemId`, `name`, `typeLine`, `identified`, `verified`, `crafted`, `enchanted`, `corrupted`, `lockedToCharacter`, `frameType`, `x`, `y`, `inventoryId`, `accountName`, `stashId`, `socketAmount`, `linkAmount`, `available`, `addedTs`, `updatedTs`, `flavourText`, `price`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '1', ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `verified` = ?, `crafted` = ?, `enchanted` = ?, `corrupted` = ?, `x` = ?, `y` = ?, `inventoryId` = ?, `accountName` = ?, `stashId` = ?, `socketAmount` = ?, `linkAmount` = ?, `available` = '1', `updatedTs` = ?, `price` = ?" );
    sql::PreparedStatement   *mod_stmt = processing_con->prepareStatement( "INSERT INTO `Mods` (`itemId`, `modName`, `modValue1`, `modValue2`, `modValue3`, `modValue4`, `modType`, `modKey`) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `modName` = ?, `modValue1` = ?, `modValue2` = ?, `modValue3` = ?, `modValue4` = ?, `modType` = ?" );
    sql::PreparedStatement   *socket_stmt = processing_con->prepareStatement( "INSERT INTO `Sockets` (`itemId`, `socketGroup`, `socketAttr`, `socketKey`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `socketGroup` = ?, `socketAttr` = ?" );
    sql::PreparedStatement   *property_stmt = processing_con->prepareStatement( "INSERT INTO `Properties` (`itemId`, `propertyName`, `propertyValue1`, `propertyValue2`, `propertyKey`) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `propertyName` = ?, `propertyValue1` = ?, `propertyValue2` = ?" );
    sql::PreparedStatement   *requirement_stmt = processing_con->prepareStatement( 
        "INSERT INTO `Requirements` (`itemId`, `requirementName`, `requirementValue`, `requirementKey`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `requirementName` = ?, `requirementValue` = ?" );
    sql::PreparedStatement   *remove_item_stmt = processing_con->prepareStatement(
        "UPDATE `Items` SET `ilvl` = ?, `icon` = ?, `league` = ?, `name` = ?, `typeLine` = ?, `identified` = ?, `verified` = ?, `corrupted` = ?, `lockedToCharacter` = ?, `frameType` = ?, `x` = ?, `y` = ?, `inventoryId` = ?, `accountName` = ?, `stashId` = ?, `socketAmount` = ?, `linkAmount` = ?, `available` = 0, `updatedTs` = ? WHERE `itemId` = ?" );

    // try {
//         stmt->execute( "START TRANSACTION" );
//         // delete stmt;
//     } catch ( sql::SQLException &e ) {
//         print_sql_error( e );
//     }

    const rapidjson::Value& stashes = document["stashes"];
    // For each stash
    for ( rapidjson::SizeType i = 0; i < stashes.Size(); i++ ) {
        begin = std::chrono::steady_clock::now();
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
        long timestamp                  = get_current_timestamp().count();
        // std::cout << timestamp << std::endl;

        // If stash is updated, the account is online
        try {
            account_stmt->setString( 1, account_name );
            account_stmt->setString( 2, last_character_name );
            account_stmt->setUInt64( 3, timestamp );
            account_stmt->setUInt64( 4, timestamp );
            account_stmt->setString( 5, last_character_name );
            account_stmt->execute();
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
        } catch ( sql::SQLException &e ) {
            errors++;
            print_sql_error( e );
        }

        end = std::chrono::steady_clock::now();
        time_other += ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );

        // Get previously stored stash contents
        std::vector<Item> old_stash = get_stash_by_ID( stash_id );
        std::vector<Item> new_stash = std::vector<Item>();

        // For each item in the stash
        for ( rapidjson::SizeType j = 0; j < items.Size(); j++ ) {
            const rapidjson::Value& item = items[j];
            if ( item.IsObject()) {
                bool verified              = item["verified"].GetBool();
                int w                      = item["w"].GetInt();
                int h                      = item["h"].GetInt();
                int ilvl                   = item["ilvl"].GetInt();
                // No need to keep the root of the address -> save space
                std::string icon           = replace_string( 
                    item["icon"].GetString(), "http://web.poecdn.com/", "" );
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
                std::string flavour_text   = "";
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
                if ( item.HasMember( "flavourText" )) {
                    const rapidjson::Value& flavours = item["flavourText"];
                    for ( rapidjson::SizeType k = 0; k < flavours.Size(); k++ ) {
                        flavour_text += flavours[k].GetString();
                    }
                }
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
                
                const rapidjson::Value& sockets = item["sockets"];
                int socket_amount               = sockets.Size();
                int link_amount                 = get_links_amount( sockets );

                Item new_item = Item( w, h, ilvl, icon, league, item_id, 
                                      item_name, type_line, identified, verified, 
                                      corrupted, locked, frame_type, 
                                      x, y, inventory_id, account_name, stash_id, 
                                      socket_amount, link_amount, true, 
                                      timestamp, timestamp, 
                                      flavour_text, price, crafted, enchanted );
                new_stash.push_back( new_item );

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
                    item_stmt->setUInt64( 23, timestamp );
                    item_stmt->setUInt64( 24, timestamp );
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
                    item_stmt->setUInt64( 39, timestamp );
                    item_stmt->setString( 40, price );
                    item_stmt->execute();
                    item_added++;
                } catch ( sql::SQLException &e ) {
                    errors++;
                    print_sql_error( e );
                }
                end = std::chrono::steady_clock::now();
                time_item += ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );
                // try {
//                     stmt->execute( "COMMIT" );
//                     // delete stmt;
//                 } catch ( sql::SQLException &e ) {
//                     print_sql_error( e );
//                 }

                // Parse mods
                int counter_mods = 0;
                // Regex to extract numerical values
                const std::regex re( "([0-9.]+)" );
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

                            name = replace_string( name, "\n", ";" );
                            name = replace_string( name, "\r", ";" );
                            Mod parsed_mod = { 
                                "\"" + item_id + "\"", 
                                "\"" + name + "\"", 
                                "\"" + values[0] + "\"", "\"" + values[1] + "\"",
                                "\"" + values[2] + "\"", "\"" + values[3] + "\"", 
                                "\"EXPLICIT\"",
                                "\"" + item_id + std::to_string(counter_mods) + "\""
                            };
                            parsed_mods.push_back( parsed_mod );
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
                            name = replace_string( name, "\n", ";" );
                            name = replace_string( name, "\r", ";" );
                            Mod parsed_mod = { 
                                "\"" + item_id + "\"", 
                                "\"" + name + "\"", 
                                "\"" + values[0] + "\"", "\"" + values[1] + "\"",
                                "\"" + values[2] + "\"", "\"" + values[3] + "\"", 
                                "\"IMPLICIT\"",
                                "\"" + item_id + std::to_string(counter_mods) + "\""
                            };
                            parsed_mods.push_back( parsed_mod );
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
                            name = replace_string( name, "\n", ";" );
                            name = replace_string( name, "\r", ";" );
                            Mod parsed_mod = { 
                                "\"" + item_id + "\"", 
                                "\"" + name + "\"", 
                                "\"" + values[0] + "\"", "\"" + values[1] + "\"",
                                "\"" + values[2] + "\"", "\"" + values[3] + "\"", 
                                "\"CRAFTED\"",
                                "\"" + item_id + std::to_string(counter_mods) + "\""
                            };
                            parsed_mods.push_back( parsed_mod );
                            // Insert mods into database
                            // try {
                            //     mod_stmt->setString( 1, item_id );
                            //     mod_stmt->setString( 2, name );
                            //     mod_stmt->setString( 3, values[0]);
                            //     mod_stmt->setString( 4, values[1]);
                            //     mod_stmt->setString( 5, values[2]);
                            //     mod_stmt->setString( 6, values[3]);
                            //     mod_stmt->setString( 7, "CRAFTED" );
                            //     mod_stmt->setString( 8, item_id + "_" + std::to_string(counter_mods));
                            //     mod_stmt->setString( 9, name );
                            //     mod_stmt->setString( 10, values[0]);
                            //     mod_stmt->setString( 11, values[1]);
                            //     mod_stmt->setString( 12, values[2]);
                            //     mod_stmt->setString( 13, values[3]);
                            //     mod_stmt->setString( 14, "CRAFTED" );
                            //     mod_stmt->execute();
                            // } catch ( sql::SQLException &e ) {
                            //     print_sql_error( e );
                            // }
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
                            name = replace_string( name, "\n", ";" );
                            name = replace_string( name, "\r", ";" );
                            Mod parsed_mod = { 
                                "\"" + item_id + "\"", 
                                "\"" + name + "\"", 
                                "\"" + values[0] + "\"", "\"" + values[1] + "\"",
                                "\"" + values[2] + "\"", "\"" + values[3] + "\"", 
                                "\"ENCHANTED\"",
                                "\"" + item_id + std::to_string(counter_mods) + "\""
                            };
                            parsed_mods.push_back( parsed_mod );
                        }
                    }
                }

                int counter = 0;
                // Parse sockets
                for ( rapidjson::SizeType k = 0; k < sockets.Size(); k++ ) {
                    if ( !sockets[k].IsNull()) {
                        counter++;
                        assert( sockets[k].IsObject());
                        const rapidjson::Value& socket = sockets[k];
                        int         group = socket["group"].GetInt();
                        assert(socket["attr"].IsString());
                        std::string attr  = socket["attr"].GetString();

                        Socket parsed_socket = { 
                            "\"" + item_id + "\"", group, "\"" + attr + "\"",
                            "\"" + item_id + std::to_string(counter) + "\""
                        };
                        parsed_sockets.push_back( parsed_socket );
                    }
                }

                // Parse properties
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
                                        insert_values[l] = std::to_string( values_inner[l].GetInt());
                                    } else {
                                        insert_values[l] = values_inner[l].GetString();
                                    }
                                }
                            }

                            Property parsed_property = { 
                                "\"" + item_id + "\"", "\"" + name + "\"", 
                                "\"" + insert_values[0] + "\"",
                                "\"" + insert_values[1] + "\"",
                                "\"" + item_id + std::to_string(counter) + "\""
                            };
                            parsed_properties.push_back( parsed_property );
                        }
                    }
                }
                
                // Parse additional properties
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
                            Property parsed_property = { 
                                "\"" + item_id + "\"", "\"" + name + "\"", 
                                "\"" + insert_values[0] + "\"",
                                "\"" + insert_values[1] + "\"",
                                "\"" + item_id + std::to_string(counter) + "\""
                            };
                            parsed_properties.push_back( parsed_property );
                        }
                    }
                }

                // Parse requirements
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

                            Requirement parsed_requirement = { 
                                "\"" + item_id + "\"", "\"" + name + "\"", 
                                "\"" + value + "\"",
                                "\"" + item_id + std::to_string(counter) + "\""
                            };
                            parsed_requirements.push_back( parsed_requirement );
                        }
                    }
                }
            }
        }
        if ( old_stash.size() > 0 ) {
            Stash_differences differences = compare_stashes( old_stash, new_stash );
            for( auto const& value: differences.removed ) {
                remove_item_stmt->setInt(    1, value.ilvl );
                remove_item_stmt->setString( 2, value.icon );
                remove_item_stmt->setString( 3, value.league );
                remove_item_stmt->setString( 4, value.name );
                remove_item_stmt->setString( 5, value.type_line );
                remove_item_stmt->setInt(    6, value.identified );
                remove_item_stmt->setInt(    7, value.verified );
                remove_item_stmt->setInt(    8, value.corrupted );
                remove_item_stmt->setInt(    9, value.locked_to_character );
                remove_item_stmt->setInt(    10, value.frame_type );
                remove_item_stmt->setInt(    11, value.x );
                remove_item_stmt->setInt(    12, value.y );
                remove_item_stmt->setString( 13, value.inventory_id );
                remove_item_stmt->setString( 14, value.account_name );
                remove_item_stmt->setString( 15, value.stash_id );
                remove_item_stmt->setInt(    16, value.socket_amount );
                remove_item_stmt->setInt(    17, value.link_amount );
                remove_item_stmt->setUInt64( 18, timestamp );
                remove_item_stmt->setString( 19, value.item_id );
                remove_item_stmt->execute();
                item_removed++;
            }
            item_added   -= differences.kept.size();
            item_updated += differences.kept.size();
        }
    }
    try {
        stmt->execute( "SET autocommit=0;" );
        stmt->execute( "SET unique_checks=0;" );
        stmt->execute( "SET foreign_key_checks=0;" );
    } catch ( sql::SQLException &e ) {
        print_sql_error( e );
    }
    // Insert mods into database
    begin = std::chrono::steady_clock::now();
    std::ofstream mod_file;
    mod_file.open( "mods.txt" );
    for ( std::vector<Mod>::iterator it = parsed_mods.begin() ; 
          it != parsed_mods.end(); ++it ) {
        mod_file << it->item_id << "," << it->name << "," << it->value1 << "," << it->value2 << "," << it->value3 << "," << it->value4 << "," << it->type << "," << it->mod_key << std::endl;
    }
    mod_file.close();
    std::thread t_mods( threaded_insert, "LOAD DATA LOW_PRIORITY LOCAL INFILE '/Users/thibautjacob/Documents/Projects/POE-price/indexer-ng/mods.txt' REPLACE INTO TABLE `Mods` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' LINES TERMINATED BY '\n'" );
    t_mods.detach();
    // try {
//             stmt->execute( "LOAD DATA CONCURRENT LOCAL INFILE '/Users/thibautjacob/Documents/Projects/POE-price/indexer-ng/mods.txt' REPLACE INTO TABLE `Mods` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' LINES TERMINATED BY '\n'" );
//     } catch ( sql::SQLException &e ) {
//         std::cout << std::endl << "# ERR: SQLException in " << __FILE__;
//     std::cout << std::endl << "(" << __FUNCTION__ << ") on line "
//             << __LINE__ << std::endl;
//     std::cout << std::endl << "# ERR: " << e.what();
//     std::cout << std::endl << " (MySQL error code: " << e.getErrorCode();
//     std::cout << std::endl << ", SQLState: " << e.getSQLState() << " )" << std::endl;
//     }
    end = std::chrono::steady_clock::now();
    time_mods += ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );
    
    // Insert requirements into database
    begin = std::chrono::steady_clock::now();
    std::ofstream requirement_file;
    requirement_file.open( "requirements.txt" );
    for ( std::vector<Requirement>::iterator it = parsed_requirements.begin() ; 
          it != parsed_requirements.end(); ++it ) {
        requirement_file << it->item_id << "," << it->name << "," << it->value << "," << it->requirement_key << std::endl;
    }
    requirement_file.close();
    std::thread t_requirements( threaded_insert, "LOAD DATA CONCURRENT LOCAL INFILE '/Users/thibautjacob/Documents/Projects/POE-price/indexer-ng/requirements.txt' REPLACE INTO TABLE `Requirements` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' LINES TERMINATED BY '\n'" );
    t_requirements.detach();
    // try {
//         stmt->execute( "LOAD DATA CONCURRENT LOCAL INFILE '/Users/thibautjacob/Documents/Projects/POE-price/indexer-ng/requirements.txt' REPLACE INTO TABLE `Requirements` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' LINES TERMINATED BY '\n'" );
//     } catch ( sql::SQLException &e ) {
//         std::cout << std::endl << "# ERR: SQLException in " << __FILE__;
//     std::cout << std::endl << "(" << __FUNCTION__ << ") on line "
//             << __LINE__ << std::endl;
//     std::cout << std::endl << "# ERR: " << e.what();
//     std::cout << std::endl << " (MySQL error code: " << e.getErrorCode();
//     std::cout << std::endl << ", SQLState: " << e.getSQLState() << " )" << std::endl;
//     }
    end = std::chrono::steady_clock::now();
    time_requirements += ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );
    
    // Insert properties into database
    begin = std::chrono::steady_clock::now();
    std::ofstream property_file;
    property_file.open( "properties.txt" );
    for ( std::vector<Property>::iterator it = parsed_properties.begin() ; 
          it != parsed_properties.end(); ++it ) {
        property_file << it->item_id << "," << it->name << "," << it->value1 << "," << it->value2 << "," << it->property_key << std::endl;
    }
    property_file.close();
    std::thread t_properties( threaded_insert, "LOAD DATA CONCURRENT LOCAL INFILE '/Users/thibautjacob/Documents/Projects/POE-price/indexer-ng/properties.txt' REPLACE INTO TABLE `Properties` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' LINES TERMINATED BY '\n'" );
    t_properties.detach();
    // try {
//         stmt->execute( "LOAD DATA CONCURRENT LOCAL INFILE '/Users/thibautjacob/Documents/Projects/POE-price/indexer-ng/properties.txt' REPLACE INTO TABLE `Properties` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' LINES TERMINATED BY '\n'" );
//     } catch ( sql::SQLException &e ) {
//         std::cout << std::endl << "# ERR: SQLException in " << __FILE__;
//     std::cout << std::endl << "(" << __FUNCTION__ << ") on line "
//             << __LINE__ << std::endl;
//     std::cout << std::endl << "# ERR: " << e.what();
//     std::cout << std::endl << " (MySQL error code: " << e.getErrorCode();
//     std::cout << std::endl << ", SQLState: " << e.getSQLState() << " )" << std::endl;
//     }
    end = std::chrono::steady_clock::now();
    time_properties += ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );
    
    // Insert sockets into database
    begin = std::chrono::steady_clock::now();
    std::ofstream socket_file;
    socket_file.open( "sockets.txt" );
    for ( std::vector<Socket>::iterator it = parsed_sockets.begin() ; 
          it != parsed_sockets.end(); ++it ) {
          socket_file << it->item_id << "," << it->group << "," << it->attr << "," << it->socket_key << std::endl;
    }
    socket_file.close();
    std::thread t_sockets( threaded_insert, "LOAD DATA CONCURRENT LOCAL INFILE '/Users/thibautjacob/Documents/Projects/POE-price/indexer-ng/sockets.txt' REPLACE INTO TABLE `Sockets` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' LINES TERMINATED BY '\n'" );
    t_sockets.detach();
    // try {
//         stmt->execute( "LOAD DATA CONCURRENT LOCAL INFILE '/Users/thibautjacob/Documents/Projects/POE-price/indexer-ng/sockets.txt' REPLACE INTO TABLE `Sockets` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' LINES TERMINATED BY '\n'" );
//     } catch ( sql::SQLException &e ) {
//         std::cout << std::endl << "# ERR: SQLException in " << __FILE__;
//     std::cout << std::endl << "(" << __FUNCTION__ << ") on line "
//             << __LINE__ << std::endl;
//     std::cout << std::endl << "# ERR: " << e.what();
//     std::cout << std::endl << " (MySQL error code: " << e.getErrorCode();
//     std::cout << std::endl << ", SQLState: " << e.getSQLState() << " )" << std::endl;
//     }
    end = std::chrono::steady_clock::now();
    time_sockets += ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );
    // t_mods.join();
//     t_properties.join();
//     t_requirements.join();
//     t_sockets.join();
    try {
        stmt->execute( "COMMIT" );
        // delete stmt;
    } catch ( sql::SQLException &e ) {
        print_sql_error( e );
    }
    std::cout << "mods: " << parsed_mods.size() << ", properties: " 
              << parsed_properties.size() << ", requirements: " 
              << parsed_requirements.size() << ", sockets: " << parsed_sockets.size() << std::endl;

    delete stmt;
    delete account_stmt;
    delete stash_stmt;
    delete league_stmt;
    delete item_stmt;
    delete mod_stmt;
    delete socket_stmt;
    delete property_stmt;
    delete requirement_stmt;
    delete remove_item_stmt;
}

float round ( float value, int precision ) {
    int power = std::pow( 10, precision );
    return std::round( value * power ) / power;
}

void query( std::string str ) {
    try {
        sql::Statement *stmt;

        stmt = processing_con->createStatement();
        stmt->execute( str );

        delete stmt;
    } catch ( sql::SQLException &e ) {
        print_sql_error( e );
    }
}

void download_loop() {
    rapidjson::Document document;

    while ( !interrupt ) {
        if ( downloaded_files.size() < QUEUE_SIZE ) {
            // Download the next change id
            std::string path = download_JSON( next_change_id );
            // Add file to the queue
            std::lock_guard<std::mutex> lock(queue_mutex);
            downloaded_files.push_back( next_change_id );

            // Read JSON file to extract next change id
            std::ifstream file( path.c_str() );
            std::stringstream sstr;
            sstr << file.rdbuf();
            document.Parse( sstr.str().c_str());
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
                sql::Statement *stmt;

                stmt = download_con->createStatement();
                // std::cout << stamp( __FUNCTION__ ) << "Adding chunk ID to DB" << std::endl;
                stmt->execute( "INSERT INTO `ChangeId` (`nextChangeId`) VALUES ('" + next_change_id + "')" );

                delete stmt;
            } catch ( sql::SQLException &e ) {
                print_sql_error( e );
            }
        } else {
            usleep( 5000 );
        }
    }
}

void processing_loop() {
    while ( !interrupt ) {
        std::deque<std::string>::iterator it = downloaded_files.begin();
        while ( it != downloaded_files.end() && !interrupt ) {
            item_added   = 0;
            item_updated = 0;
            item_removed = 0;
            errors       = 0;
            // Parse the JSON data
            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
            parse_JSON( download_dir + "indexer_" + *it + ".json" );
            std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            // Set this change ID has processed and delete the data file
            query( "UPDATE `ChangeId` SET `processed` = '1' WHERE `nextChangeId` = '" + *it + "'" );
            std::string path = download_dir + "indexer_" + *it + ".json";
            // std::cout << std::endl << "Removing " << path << std::endl;
            std::remove( path.c_str());
            std::lock_guard<std::mutex> lock(queue_mutex);
            *it++;
            downloaded_files.pop_front();
            total_item_added    += item_added;
            total_item_updated  += item_updated;
            total_item_removed  += item_removed;
            total_errors        += errors;
            int sum = item_added + item_updated + item_removed;
            float time_sec = ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );
            total_sum          += sum;
            total_time         += time_sec;
            float speed         = std::floor( sum / time_sec );
            float total_speed   = std::floor( total_sum / total_time );
            float remaning_time = 
                time_sec - ( time_loading_JSON + time_item + time_mods + 
                time_properties + time_sockets + time_requirements + time_other );
            Time total_time_conv = format_time( total_time * 1000.0 );
            Time time_sec_conv   = format_time( time_sec * 1000.0 );
            std::cout << stamp( __FUNCTION__ ) << "Entries total: " 
                      << sum << ", added: " << GREEN
                      << item_added << RESET << ", removed: " << RED << item_removed 
                      << RESET << ", updated: " << BLUE
                      << item_updated << RESET << ", insert errors: " << errors
                      << " over " << round( time_sec_conv.amount, 2 ) << " " 
                      << time_sec_conv.unit << " at " << MAGENTA << speed 
                      << RESET << " insert/s" << std::endl;
            std::cout << stamp( __FUNCTION__ ) << "Time profile: "
                      << "JSON: " << round( time_loading_JSON, 2 ) << " sec, "
                      << "items: " << round( time_item, 2 ) << " sec (" 
                      << std::ceil( time_item * 100 / time_sec ) << " %), "
                      << "mods: " << round( time_mods, 2 ) << " sec (" 
                      << std::ceil( time_mods * 100 / time_sec ) << " %), "
                      << "props: " << round( time_properties, 2 ) << " sec (" 
                      << std::ceil( time_properties * 100 / time_sec ) << " %), "
                      << "socks: " << round( time_sockets, 2 ) << " sec (" 
                      << std::ceil( time_sockets * 100 / time_sec ) << " %), "
                      << "req: " << round( time_requirements, 2 ) << " sec (" 
                      << std::ceil( time_requirements * 100 / time_sec ) << " %) "
                      << "others: " << round( time_other, 2 ) << " sec ("
                      << std::ceil( time_other * 100 / time_sec ) << " %), "
                      << "remain: " << round( remaning_time, 2 ) << " sec ("
                      << std::ceil( remaning_time * 100 / time_sec ) << " %)"
                      << std::endl;
            std::cout << stamp( __FUNCTION__ ) 
                      << "Total entries processed: " << total_sum 
                      << ", added: " << GREEN << total_item_added << RESET 
                      << ", removed: " << RED << total_item_removed << RESET 
                      << ", updated: " << BLUE << total_item_updated << RESET
                      << ", insert errors: " << total_errors
                      << " over " << round( total_time_conv.amount, 2 ) << " " 
                      << total_time_conv.unit << " at " << MAGENTA << total_speed 
                      << RESET << " insert/s" << std::endl;
            std::cout << stamp( __FUNCTION__ ) 
                      << downloaded_files.size() << " files to be processed" 
                      << std::endl;
            // std::string files = "";
            // for ( std::deque<std::string>::iterator it_q = downloaded_files.begin() ; 
            //       it_q != downloaded_files.end() ; it_q++ ) {
            //     files += ", " + *it_q;
            // }
            //  std::cout << stamp( __FUNCTION__ ) 
            //           << "Files: " << files 
            //           << std::endl;
        }
        if ( !interrupt ) {
            std::cout << stamp( __FUNCTION__ ) << "Waiting for files to process" 
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

/**
 * Run a benchmark on a set of files
 * 
 * @param Nothing
 * @return Nothing
 */
 
void bench( std::string path ) {
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    parse_JSON( path );
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    
    total_item_added    += item_added;
    total_item_updated  += item_updated;
    total_item_removed  += item_removed;
    total_errors        += errors;
    int sum = item_added + item_updated + item_removed;
    float time_sec = ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );
    total_sum          += sum;
    total_time         += time_sec;
    float speed         = std::floor( sum / time_sec );
    float total_speed   = std::floor( total_sum / total_time );
    float remaning_time = 
        time_sec - ( time_loading_JSON + time_item + time_mods + 
        time_properties + time_sockets + time_requirements + time_other );
    Time total_time_conv = format_time( total_time * 1000.0 );
    Time time_sec_conv   = format_time( time_sec * 1000.0 );
    
    std::cout << stamp( __FUNCTION__ ) << "Entries total: " 
              << sum << ", added: " << GREEN
              << item_added << RESET << ", removed: " << RED << item_removed 
              << RESET << ", updated: " << BLUE
              << item_updated << RESET << ", insert errors: " << errors
              << " over " << round( time_sec_conv.amount, 2 ) << " " 
              << time_sec_conv.unit << " at " << MAGENTA << speed 
              << RESET << " insert/s" << std::endl;
    std::cout << stamp( __FUNCTION__ ) << "Time profile: "
              << "JSON: " << round( time_loading_JSON, 2 ) << " sec, "
              << "items: " << round( time_item, 2 ) << " sec (" 
              << std::ceil( time_item * 100 / time_sec ) << " %), "
              << "mods: " << round( time_mods, 2 ) << " sec (" 
              << std::ceil( time_mods * 100 / time_sec ) << " %), "
              << "props: " << round( time_properties, 2 ) << " sec (" 
              << std::ceil( time_properties * 100 / time_sec ) << " %), "
              << "socks: " << round( time_sockets, 2 ) << " sec (" 
              << std::ceil( time_sockets * 100 / time_sec ) << " %), "
              << "req: " << round( time_requirements, 2 ) << " sec (" 
              << std::ceil( time_requirements * 100 / time_sec ) << " %) "
              << "others: " << round( time_other, 2 ) << " sec ("
              << std::ceil( time_other * 100 / time_sec ) << " %), "
              << "remain: " << round( remaning_time, 2 ) << " sec ("
              << std::ceil( remaning_time * 100 / time_sec ) << " %)"
              << std::endl;
    std::cout << stamp( __FUNCTION__ ) 
              << "Total entries processed: " << total_sum 
              << ", added: " << GREEN << total_item_added << RESET 
              << ", removed: " << RED << total_item_removed << RESET 
              << ", updated: " << BLUE << total_item_updated << RESET
              << ", insert errors: " << total_errors
              << " over " << round( total_time_conv.amount, 2 ) << " " 
              << total_time_conv.unit << " at " << MAGENTA << total_speed 
              << RESET << " insert/s" << std::endl;
}
 
void benchmark() {
    std::vector<std::string> files = {
        "./bench/indexer_31302710-33646811-31252392-36315804-33954900.json",
        "./bench/indexer_31403451-33753224-31353577-36423092-34054380.json",
        "./bench/indexer_31404167-33753864-31354065-36423481-34054947.json",
        "./bench/indexer_31404653-33754420-31354610-36424060-34055582.json",
        "./bench/indexer_31405237-33755108-31355149-36424484-34055976.json",
        "./bench/indexer_31405893-33755907-31355736-36425049-34056622.json",
        "./bench/indexer_31406319-33756638-31356176-36425781-34057270.json",
        "./bench/indexer_31407025-33757249-31356667-36426425-34057903.json",
        "./bench/indexer_31407500-33757952-31357532-36426997-34058597.json"
    };
    for ( std::vector<std::string>::iterator it = files.begin() ; it != files.end() ; it++ ) {
        item_added   = 0;
        item_updated = 0;
        item_removed = 0;
        errors       = 0;      
        bench( *it );
    }
}

int main( int argc, char* argv[]) {

    std::thread download_thread; // JSON download thread
    std::thread processing_thread; // JSON processing thread

    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    // Connect to DB
    driver = sql::mysql::get_mysql_driver_instance();
    download_con = driver->connect( DB_HOST + ":" + DB_PORT, DB_USER, DB_PASS );
    download_con->setSchema( DB_NAME );
    processing_con = driver->connect( DB_HOST + ":" + DB_PORT, DB_USER, DB_PASS );
    processing_con->setSchema( DB_NAME );
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    float time_DB = ( std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0 );
    std::cout << stamp( __FUNCTION__ ) << "Connected to DB in " 
              << time_DB << " sec" << std::endl;
              
    // benchmark();
//     return 0;

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

    delete download_con;
    delete processing_con;

    return 0;
}