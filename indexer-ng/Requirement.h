#ifndef _REQUIREMENT_H_
#define _REQUIREMENT_H_

#include <string>

class Requirement {
    public:
        Requirement( std::string name, float value );
        
        std::string name;
        float value;
};

#endif /* _REQUIREMENT_H_ */