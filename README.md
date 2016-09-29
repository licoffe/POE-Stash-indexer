### What is included ?

This tool is composed of both an indexer for the official Path of Exile Stash API and a client.

### Indexer
![alt](./indexer.png)

The indexer polls the official stash API at a given interval to retrieve JSON formatted stash contents. This content is then inserted into a Mongo database for future polling.

The indexer can be started with `./indexer.js` after `chmod a+x indexer.js` has been issued or just with `node ./indexer.js`.

### Client
![alt](./client.png)

The client features:
- Market exchange rate polling through poe.trade
- Lookup of prices for a given item name, account name or mods
- A price distribution plot to have a better idea of the current market price for a specific item

### Requirements
MongoDB, NodeJS, Mac or Linux.