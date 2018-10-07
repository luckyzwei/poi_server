#!/bin/bash

gunicorn -c config.py compare_server:app