#!/usr/bin/env python3
import http.server
import socketserver
import os
import sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8889
WEB_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "web"))

class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def do_GET(self):
        if self.path in ("/", "/dashboard", "/dashboard/"):
            self.path = "/index.html"
        elif self.path.startswith("/dashboard/"):
            self.path = self.path[len("/dashboard"):]
        return super().do_GET()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        super().end_headers()

class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

if __name__ == "__main__":
    with ThreadingHTTPServer(("0.0.0.0", PORT), DashboardHandler) as httpd:
        print(f"CDA Web Dashboard serving at http://0.0.0.0:{PORT}/ and http://0.0.0.0:{PORT}/dashboard/")
        httpd.serve_forever()
