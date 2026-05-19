#!/bin/bash
gunicorn --bind 0.0.0.0:8000 --chdir /home/site/wwwroot/migration_utility app:app --timeout 600
