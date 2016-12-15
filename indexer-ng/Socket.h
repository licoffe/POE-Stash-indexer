#ifndef _SOCKET_H_
#define _SOCKET_H_

#include <string>

class Socket {
    public:
        Socket( int group, char attr );
        
        int  group;
        char attr;
};

#endif /* _SOCKET_H_ */