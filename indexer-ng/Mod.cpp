#include "Mod.h"

Mod::Mod( std::string name, float value1, float value2, float value3, 
          float value4, Mod::Mod_type mod_type ) {
    this->name     = name;
    this->value1   = value1;
    this->value2   = value2;
    this->value3   = value3;
    this->value4   = value4;
    this->mod_type = mod_type;
};