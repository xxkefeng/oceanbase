#!/usr/bin/env python2.6

''' diamond mock '''

# author: junyue 
# date  : 2012-12-05

import sys,urllib 
from os import curdir,sep
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer


class DiamondHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            get=True
            key='mytest'
            value=''            
            kw=self.path.split("&")
            for c in kw:
                 k = c.split("=")
                 if k[0] == '/method' and k[1] == 'set':
                    get=False
                 elif k[0] == 'key':
                    key=k[1]
                 elif k[0] == 'value':
                    value=k[1]
            if get == True:
                f=open(curdir+sep+key)
                self.send_response(200)
                self.send_header('Content-type','text/html')
                self.end_headers()
                self.wfile.write(f.read())
                f.close()
            else:
                f=open(curdir+sep+key, "w")
                self.send_response(200)
                self.send_header('Content-type','text/html')
                self.end_headers()
                content = urllib.unquote(value)
                f.write(content)
                f.close()
                self.wfile.write('ok')
                
        except IOError:
            self.send_error(404, 'File Not Found: %s' % self.path)
            
if __name__=='__main__':

    try:
        server = HTTPServer(('',9000),DiamondHandler)
        server.serve_forever()
    except KeyboardInterrupt:
        server.socket.close()