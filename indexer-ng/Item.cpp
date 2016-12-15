#include "Item.h"

Item::Item( int width, int height, int ilvl, std::string icon, std::string league, 
            std::string item_id, std::string name, std::string type_line, 
            bool identified, bool verified, bool corrupted, bool locked_to_character, 
            int frame_type, int x, int y, std::string inventory_id, 
            std::string account_name, std::string stash_id, int socket_amount, 
            int link_amount, bool available, int added_timestamp, 
            int updated_timestamp, std::string flavour_text, std::string price, 
            bool crafted, bool enchanted ) {
    this->width                 = width;
    this->height                = height;
    this->ilvl                  = ilvl;
    this->icon                  = icon;
    this->league                = league; 
    this->item_id               = item_id;
    this->name                  = name;
    this->type_line             = type_line; 
    this->identified            = identified;
    this->verified              = verified;
    this->corrupted             = corrupted; 
    this->locked_to_character   = locked_to_character; 
    this->frame_type            = frame_type;
    this->x                     = x;
    this->y                     = y;
    this->inventory_id          = inventory_id; 
    this->account_name          = account_name;
    this->stash_id              = stash_id;
    this->socket_amount         = socket_amount; 
    this->link_amount           = link_amount;
    this->available             = available;
    this->added_timestamp       = added_timestamp; 
    this->updated_timestamp     = updated_timestamp;
    this->flavour_text          = flavour_text;
    this->price                 = price;
    this->crafted               = crafted;
    this->enchanted             = enchanted;
    this->sockets               = std::vector<Socket>();
    this->properties            = std::vector<Property>();
    this->additional_properties = std::vector<Property>();
    this->requirements          = std::vector<Requirement>();
    this->mods                  = std::vector<Mod>();
};