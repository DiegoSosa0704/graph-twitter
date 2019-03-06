#!/usr/bin/env bash
source ~/graph-twitter/env/bin/activate
0 12 * * * python3 send_request.py
deactivate