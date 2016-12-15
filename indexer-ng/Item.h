#ifndef _ITEM_H_
#define _ITEM_H_

#include <string>
#include <vector>
#include "Socket.h"
#include "Property.h"
#include "Requirement.h"
#include "Mod.h"

class Item {
    public:
        Item( int width, 
              int height, 
              int ilvl, 
              std::string icon, 
              std::string league, 
              std::string item_id, 
              std::string name, 
              std::string type_line, 
              bool identified, 
              bool verified, 
              bool corrupted, 
              bool locked_to_character, 
              int frame_type, 
              int x, 
              int y, 
              std::string inventory_id, 
              std::string account_name, 
              std::string stash_id, 
              int socket_amount, 
              int link_amount, 
              bool available, 
              int added_timestamp, 
              int updated_timestamp, 
              std::string flavour_text, 
              std::string price, 
              bool crafted, 
              bool enchanted );
        int                      width;
        int                      height;
        int                      ilvl;
        std::string              icon;
        std::string              league;
        std::string              item_id;
        std::string              name;
        std::string              type_line;
        bool                     identified;
        bool                     verified;
        bool                     corrupted;
        bool                     locked_to_character;
        int                      frame_type;
        int                      x;
        int                      y;
        std::string              inventory_id;
        std::string              account_name;
        std::string              stash_id;
        int                      socket_amount;
        int                      link_amount;
        bool                     available;
        int                      added_timestamp;
        int                      updated_timestamp;
        std::string              flavour_text;
        std::string              price;
        bool                     crafted;
        bool                     enchanted;
        std::vector<Socket>      sockets;
        std::vector<Property>    properties;
        std::vector<Property>    additional_properties;
        std::vector<Requirement> requirements;
        std::vector<Mod>         mods;
};

#endif /* _ITEM_H_ */