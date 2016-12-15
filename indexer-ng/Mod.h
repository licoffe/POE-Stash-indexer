#ifndef _MOD_H_
#define _MOD_H_

#include <string>

class Mod {
    public:
        enum Mod_type {
            EXPLICIT, IMPLICIT, CRAFTED, ENCHANTED
        };
        Mod( std::string name, float value1, float value2, float value3, float value4, Mod::Mod_type mod_type );
        
        std::string name;
        float value1;
        float value2;
        float value3;
        float value4;
        Mod::Mod_type mod_type;
};

#endif /* _MOD_H_ */