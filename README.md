## What is included ?

This tool is composed of both an indexer for the official Path of Exile Stash API and a client.

## Indexer
![alt](./indexer.png)

#### Description
The indexer polls the official stash API at a given interval to retrieve JSON formatted stash contents. This content is then inserted into a Mongo database for future polling.

#### Installation
Required node packages can be installed through `npm install`.

#### How to run?
The indexer can be started with `node ./indexer.js` or `nodejs ./indexer.js` depending on your system.

##### CLI arguments
`-d` will output additional log informations into a _log.txt_ file. Only use to debug as it creates unnecessary IO traffic.

## Client
![alt](./client.png)

The client features:
- Market exchange rate polling through [poe.trade](http://poe.trade)
- Lookup of prices for a given item name, account name or mods
- A price distribution plot to have a better idea of the current market price for a specific item

#### Installation
Required node packages can be installed through `npm install`. On Ubuntu it mayb also be required to create an alias of the nodejs command to node using the following `ln /usr/bin/nodejs /usr/bin/node`.

#### How to run?
The indexer can be started with `npm start`.

### Requirements
MongoDB, NodeJS, Mac or Linux.