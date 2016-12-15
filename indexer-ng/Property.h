#ifndef _PROPERTY_H_
#define _PROPERTY_H_

#include <string>

class Property {
    public:
        Property( std::string name, float value1, float value2 );
        
        std::string name;
        float value1;
        float value2;
};

#endif /* _PROPERTY_H_ */