#include <windows.h>
#include "socket_iocp.h"

//��������
void main() {
	Launch(15555);

	while (1) {
		Sleep(10);
	}
}