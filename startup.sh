#!/bin/bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
gunicorn mia:app --bind=0.0.0.0 --timeout 300