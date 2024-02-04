from socketserver import ThreadingMixIn
from xmlrpc.server import DocXMLRPCServer, DocXMLRPCRequestHandler


class RPCThreading(ThreadingMixIn, DocXMLRPCServer):
    daemon_threads = True


def threaded(port):
    return RPCThreading(('0.0.0.0', port), DocXMLRPCRequestHandler)
