/*
自己编写游戏服务器之iocp
1.初始化，
2.创建完成端口
3.根据系统中CPU核心的数量建立对应的Worker线程
4.创建一个用于监听的Socket，绑定到完成端口上，然后开始在指定的端口上监听连接请求
5.在这个监听Socket上投递AcceptEx请求
*/

#include <winsock2.h>
#include <windows.h>
#include <process.h>
#include <stdio.h>
#include <mswsock.h>

#include "socket_iocp.h"

#pragma comment(lib, "ws2_32.lib")

#define		FAIL						-1
#define		SUCCESS						0

#define		N_SOCKETS_PER_LISTENER		4
//#define		NOTIFICATION_KEY			((ULONG_PTR)-1)

// 缓冲区长度 (1024*8)
// 之所以为什么设置8K，也是一个江湖上的经验值
// 如果确实客户端发来的每组数据都比较少，那么就设置得小一些，省内存
#define		Max_Buffer_Size				8192

const GUID AcceptEX = WSAID_ACCEPTEX;
const GUID GetAcceptExSockAddrs = WSAID_GETACCEPTEXSOCKADDRS;
const GUID ConnectEX = WSAID_CONNECTEX;

int							Port;
HANDLE						IOCPHandle = NULL;
SOCKET						Listener = NULL;

LPFN_ACCEPTEX				lpfn_AcceptEx;
LPFN_GETACCEPTEXSOCKADDRS	lpfn_GetAcceptExSockAddrs;
LPFN_CONNECTEX				lpfn_ConnectEx;

typedef enum IOOperType
{
	AcceptOperate,				//accept事件到达，有新连接请求
	ReceiveOperate,				//数据接收事件
	SendOperate,				//数据发送事件
	CloseOperate,				//关闭事件
}IOOPERTYPE, * PIOOPERTYPE;

typedef struct
{
	OVERLAPPED      Overlapped;
	SOCKET          Socket;
	WSABUF          WsaBuffer;
	CHAR            Buffer[Max_Buffer_Size];
	IOOPERTYPE      OperType;

} PreIOContext, * LPPreIOContext;

int		InitSocket();
void	HandleIOCP(LPPreIOContext  ioContext);
int		SetSocketOpts(SOCKET* sock);
int		GetLPNFHandle();
int		ListenPort();

DWORD WINAPI WorkThread(LPVOID lp_iocp);

int Launch(int port)
{
	int				i;
	DWORD			thread_id;
	SYSTEM_INFO     sys_info;
	HANDLE* threads = NULL;

	Port = port;

	GetSystemInfo(&sys_info);
	printf("System memery page size: %d \n", sys_info.dwPageSize);
	printf("System cpus: %d \n", sys_info.dwNumberOfProcessors);

	int thread_num = sys_info.dwNumberOfProcessors * 2;
	threads = (HANDLE*)malloc(sizeof(HANDLE) * thread_num);

	if (InitSocket() == FAIL) {
		printf("InitSocket failed.\n");
		return -1;
	}

	if ((IOCPHandle = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0)) == NULL) {
		printf("CreateIoCompletionPort Failed，err: %d\n", GetLastError());
		return -1;
	}

	for (i = 0; i < thread_num; i++)
	{
		if ((threads[i] = CreateThread(NULL, 0, WorkThread, IOCPHandle, 0, &thread_id)) == NULL)
		{
			printf("CreateThread() failed. error: %d\n", GetLastError());
			CloseHandle(IOCPHandle);
			WSACleanup();
			return -1;
		}
	}

	if ((Listener = WSASocketW(AF_INET, SOCK_STREAM, 0, NULL, 0, WSA_FLAG_OVERLAPPED)) == INVALID_SOCKET)
	{
		printf("WSASocket() failed. error: %d\n", WSAGetLastError());
		WSACleanup();
		return -1;
	}

	if (SetSocketOpts(Listener) == FAIL)
	{
		printf("SetSocketOpts failed.\n");
		return -1;
	}

	if (INVALID_HANDLE_VALUE == CreateIoCompletionPort((HANDLE)Listener, IOCPHandle, (ULONG_PTR)AcceptOperate, 0))
	{
		printf("CreateIoCompletionPort(listener) failed.\n");
		closesocket(Listener);
		WSACleanup();
		CloseHandle(IOCPHandle);
		return -1;
	}

	if (ListenPort() == FAIL)
	{
		printf("ListenPort failed.\n");
		return -1;
	}

	if (GetLPNFHandle() == FAIL)
	{
		printf("GetLPNFHandle failed.\n");
		return -1;
	}

	for (i = 0; i < N_SOCKETS_PER_LISTENER; i++)
	{
		PostAcceptEX();
	}
}

int InitSocket()
{
	WSADATA         wsa_data;

	if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
		printf("WSAStartup() failed.\n");
		return FAIL;
	}

	if (LOBYTE(wsa_data.wVersion) != 2 || HIBYTE(wsa_data.wVersion) != 2)
	{
		printf("Require Windows Socket Version 2.2 Error!\n");
		WSACleanup();
		return FAIL;
	}

	return SUCCESS;
}


DWORD WINAPI WorkThread(LPVOID lp_iocp)
{
	DWORD				err_no;
	DWORD				bytes = 0;
	HANDLE				iocp = (HANDLE)lp_iocp;
	PULONG_PTR          lp_completion_key = NULL;
	OVERLAPPED			overlapped;
	LPPreIOContext		ioContext;

	while (TRUE)
	{
		if (0 == GetQueuedCompletionStatus(iocp, &bytes, (PULONG_PTR)&lp_completion_key, (LPOVERLAPPED*)&ioContext, INFINITE))
		{
			err_no = GetLastError();

			if (err_no)
			{

				if (WAIT_TIMEOUT == err_no) continue;

				if (ERROR_NETNAME_DELETED == err_no || ERROR_OPERATION_ABORTED == err_no)
				{
					printf("The socket was closed. error: %d\n", err_no);
					GlobalFree(ioContext);

					continue;
				}

				printf("GetQueuedCompletionStatus() failed. error: %d\n", err_no);
				GlobalFree(ioContext);

				return err_no;
			}
		}

		if (NULL == ioContext)
		{
			printf("GetQueuedCompletionStatus() returned no operation");
			continue;
		}

		printf("IO_Context: %p \n", ioContext);
		printf("Bytes transferred: %d \n", bytes);
		printf("IO_Context->Action： %d\n", ioContext->OperType);


		if (0 == bytes && 0 != ioContext->OperType)
		{
			printf("No bytes transferred for the action.");
			GlobalFree(ioContext);
			continue;
		}

		HandleIOCP(ioContext);
	}

}

void HandleIOCP(PreIOContext* ioContext)
{
	switch (ioContext->OperType)
	{
	case AcceptOperate:
		DoAccept(ioContext);
		break;

	case ReceiveOperate:
		DoRecv(ioContext);
		break;

	case SendOperate:
		DoSend(ioContext);
		break;

	default:
		printf("ERROR: No action match! \n");
		break;
	}
}

int SetSocketOpts(SOCKET* sock)
{
	int opt_val = 1;
	int opt_len = sizeof(int);

	LINGER			linger;

	if (-1 == setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (const void*)&opt_val, opt_len))
	{
		printf("setsockopt(SO_KEEPALIVE) failed.\n");
		closesocket(Listener);
		WSACleanup();
		CloseHandle(IOCPHandle);
		return FAIL;
	}

	// closesocket: return immediately and send RST
	linger.l_onoff = 1;
	linger.l_linger = 0;
	if (-1 == setsockopt(sock, SOL_SOCKET, SO_LINGER, (char*)&linger, sizeof(linger)))
	{
		printf("setsockopt(SO_LINGER) failed.\n");
		closesocket(Listener);
		WSACleanup();
		CloseHandle(IOCPHandle);
		return FAIL;
	}

	// Windows only support SO_REUSEADDR
	if (-1 == setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (const void*)&opt_val, opt_len))
	{
		printf("setsockopt(SO_REUSEADDR) failed.\n");
		closesocket(Listener);
		WSACleanup();
		CloseHandle(IOCPHandle);
		return FAIL;
	}

	return SUCCESS;
}

int	ListenPort()
{
	SOCKADDR_IN	inet_addr;
	inet_addr.sin_family = AF_INET;
	inet_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	inet_addr.sin_port = htons(Port);
	if (SOCKET_ERROR == bind(Listener, (PSOCKADDR)&inet_addr, sizeof(inet_addr)))
	{
		printf("bind() failed.\n");
		closesocket(Listener);
		WSACleanup();
		CloseHandle(IOCPHandle);
		return FAIL;
	}


	if (SOCKET_ERROR == listen(Listener, SOMAXCONN))
	{
		printf("listen() failed.\n");
		closesocket(Listener);
		WSACleanup();
		CloseHandle(IOCPHandle);
		return FAIL;
	}

	return SUCCESS;
}

int GetLPNFHandle()
{
	SOCKET			sock;
	DWORD           dwBytes;

	sock = WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_IP, NULL, 0, WSA_FLAG_OVERLAPPED);
	if (sock == INVALID_SOCKET) {
		printf("socket() failed.\n");
		return FAIL;
	}

	if (-1 == WSAIoctl(sock, SIO_GET_EXTENSION_FUNCTION_POINTER, &AcceptEX, sizeof(GUID),
		&lpfn_AcceptEx, sizeof(LPFN_ACCEPTEX),
		&dwBytes, NULL, NULL))
	{
		printf("WSAIoctl(LPFN_ACCEPTEX) failed.\n");
		return FAIL;
	}

	if (-1 == WSAIoctl(sock, SIO_GET_EXTENSION_FUNCTION_POINTER, &GetAcceptExSockAddrs, sizeof(GUID),
		&lpfn_GetAcceptExSockAddrs, sizeof(LPFN_GETACCEPTEXSOCKADDRS),
		&dwBytes, NULL, NULL))
	{
		printf("WSAIoctl(LPFN_GETACCEPTEXSOCKADDRS) failed.\n");
		return FAIL;
	}

	if (-1 == WSAIoctl(sock, SIO_GET_EXTENSION_FUNCTION_POINTER, &ConnectEX, sizeof(GUID),
		&lpfn_ConnectEx, sizeof(LPFN_CONNECTEX),
		&dwBytes, NULL, NULL))
	{
		printf("WSAIoctl(LPFN_CONNECTEX) failed.\n");
		return FAIL;
	}

	if (-1 == closesocket(sock)) {
		printf("closesocket() failed.\n");
		return FAIL;
	}

	return SUCCESS;
}

int PostAcceptEX()
{
	DWORD	bytes = 0;
	LPPreIOContext	ioContext = NULL;

	if ((ioContext = (LPPreIOContext)GlobalAlloc(GPTR, sizeof(PreIOContext))) == NULL)
	{
		printf("GlobalAlloc() failed. error: %d\n", GetLastError());
		return FALSE;
	}

	ZeroMemory(&(ioContext->Overlapped), sizeof(OVERLAPPED));
	ioContext->WsaBuffer.len = Max_Buffer_Size;
	ioContext->WsaBuffer.buf = ioContext->Buffer;
	ioContext->OperType = AcceptOperate;

	ioContext->Socket = WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
	if (INVALID_SOCKET == ioContext->Socket)
	{
		printf("WSASocketW() failed.\n");
		return FALSE;
	}


	if (0 == lpfn_AcceptEx(
		Listener,
		ioContext->Socket,
		ioContext->WsaBuffer.buf,
		ioContext->WsaBuffer.len - (sizeof(SOCKADDR_IN) + 16) * 2,
		sizeof(SOCKADDR_IN) + 16,
		sizeof(SOCKADDR_IN) + 16,
		&bytes,
		&(ioContext->Overlapped)
	)) {
		if (WSA_IO_PENDING != WSAGetLastError())
		{
			printf("LPFN_ACCEPTEX() failed. last error: %d\n", WSAGetLastError());
			return FAIL;
		}
	}

	printf("post_accept_ex. listner: %I64d, ioContext: %p \n", Listener, ioContext);

	return SUCCESS;
}


int DoAccept(PreIOContext* ioContext)
{
	SOCKADDR_IN* local_sock_addr = NULL;
	SOCKADDR_IN* remote_sock_addr = NULL;
	int addr_len = sizeof(SOCKADDR_IN);

	if (FALSE == setsockopt(ioContext->Socket, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, (char*)&Listener, sizeof(SOCKET)))
	{
		printf("setsockopt(SO_UPDATE_ACCEPT_CONTEXT) failed. error: %d\n", WSAGetLastError());
	}


	lpfn_GetAcceptExSockAddrs(
		ioContext->WsaBuffer.buf,
		ioContext->WsaBuffer.len - ((addr_len + 16) * 2),
		addr_len + 16,
		addr_len + 16,
		(SOCKADDR**)&local_sock_addr, &addr_len,
		(SOCKADDR**)&remote_sock_addr, &addr_len
	);

	printf("客户端 %s:%d 信息：%s.\n", inet_ntoa(remote_sock_addr->sin_addr), ntohs(remote_sock_addr->sin_port), ioContext->WsaBuffer.buf);


	if (NULL == CreateIoCompletionPort((HANDLE)ioContext->Socket, IOCPHandle, 0, 0))
	{
		printf("CreateIoCompletionPort() failed. error: %d\n", GetLastError());
		return FAIL;
	}

	if (FAIL == PostRecv(ioContext)) 
	{
		printf("DoAccept PostRecvt() failed. \n");
		return FAIL;
	}

	return PostAcceptEX();
}


int PostRecv(PreIOContext* ioContext)
{
	printf("post_recv. ioContext: %p \n", ioContext);

	DWORD flags = 0;
	DWORD bytes = 0;
	DWORD errNumber;
	int ret;

	ZeroMemory(&(ioContext->Overlapped), sizeof(OVERLAPPED));
	ioContext->WsaBuffer.len = Max_Buffer_Size;
	ioContext->WsaBuffer.buf = ioContext->Buffer;
	ioContext->OperType = ReceiveOperate;

	ret = WSARecv(ioContext->Socket, &(ioContext->WsaBuffer), 1, &bytes, &flags, &(ioContext->Overlapped), NULL);

	errNumber = WSAGetLastError();
	if (-1 == ret && WSA_IO_PENDING != errNumber)
	{
		if (errNumber == WSAEWOULDBLOCK) printf("WSARecv() not ready");

		printf("WSARecv() faild. client socket: %I64d, error: %d\n", ioContext->Socket, errNumber);

		return FAIL;
	}

	return SUCCESS;
}


int DoRecv(PreIOContext* ioContext)
{
	printf("do_recv. ioContext: %p \n", ioContext);
	printf("do_recv: recv data：\n %s \n", ioContext->WsaBuffer.buf);

	ZeroMemory(&(ioContext->Overlapped), sizeof(OVERLAPPED));
	ioContext->WsaBuffer.len = Max_Buffer_Size;
	ioContext->WsaBuffer.buf = ioContext->Buffer;
	ioContext->OperType = ReceiveOperate;

	return PostRecv(ioContext);
}


int PostSend(PreIOContext* ioContext)
{
	printf("post_send. ioContext: %p \n", ioContext);

	return 0;
}


int DoSend(PreIOContext* ioContext)
{
	printf("do_send. ioContext: %p \n", ioContext);

	shutdown(ioContext->Socket, SD_BOTH);
	GlobalFree(ioContext);

	return 0;
}