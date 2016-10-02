/**
 * Logger class
 * Provides timestamp to logs and possibility to log to file
 *
 * TODO: 
 * - Fix hard-coded path to log file in fs.access and fs.appendFile
 */
var fs = require( "fs" );
var colors = {
    "reset":      "\x1b[0m",
    "bright":     "\x1b[1m",
    "dim":        "\x1b[2m",
    "underscore": "\x1b[4m",
    "blink":      "\x1b[5m",
    "reverse":    "\x1b[7m",
    "hidden":     "\x1b[8m",
    "black":      "\x1b[30m",
    "red":        "\x1b[31m",
    "green":      "\x1b[32m",
    "yellow":     "\x1b[33m",
    "blue":       "\x1b[34m",
    "magenta":    "\x1b[35m",
    "cyan":       "\x1b[36m" };

function Logger() {
    this.use_file      = false;
    this.use_timestamp = false;
    
    // Getter & setters
    Logger.prototype.get_use_file = function() {
        return this.use_file;
    }
    
    Logger.prototype.get_file_path = function() {
        return this.file_path;
    }
    
    Logger.prototype.get_use_timestamp = function() {
        return this.use_timestamp;
    }
    
    // Set wether we should log to file or not
    Logger.prototype.set_use_file = function( bool ) {
        this.use_file = bool;
    }
    
    // Set file path
    Logger.prototype.set_file_path = function( path ) {
        this.file_path = path;
    }
    
    // Set wether we should use timestamp
    Logger.prototype.set_use_timestamp = function( bool ) {
        this.use_timestamp = bool;
    }
    
    /**
     * log
     * Log input message to console or to file
     * @params Message
     * @return None
     */
    Logger.prototype.log = function( message, dispatcher, error_code, fileOnly ) {
        // If we have an error code
        if ( error_code === "w" ) { // warning code
            message = colors.yellow + message + colors.reset;
        } else if ( error_code === "e" ) { // error_code
            message = colors.red + message + colors.reset;
        }
        // If disptacher is defined
        if ( dispatcher !== "" && dispatcher !== undefined ) {
            message = "[" + colors.green + dispatcher + colors.reset + "] " + message;
        }
        if ( this.use_timestamp ) {
            var date = new Date();
            message = colors.yellow + date.getDate() + "-" + (date.getMonth() + 1) + "-" + 
                      date.getFullYear() + " " + date.getHours() + ":" + 
                      date.getMinutes() + ":" + date.getSeconds() + colors.red + " > " + colors.reset + message;
        }
        // Log to file
        if ( this.use_file ) {
            if ( this.file_path === "" || this.file_path === undefined ) {
                console.log( "No file to log to specified" );
                return;
            }
            // Check file access: should be writable and visible
            try {
                fs.accessSync( this.file_path, fs.F_OK );
                fs.appendFileSync( this.file_path, message + "\n", "utf8" );
            } catch ( e ) {
                fs.closeSync(fs.openSync( this.file_path, 'w' ));
            }
        }
        // If not file only, write to console
        if ( !fileOnly ) {
            console.log( message );
        }
    }
};

module.exports = Logger;