# Gunicorn configuration for Render
workers = 4
threads = 2
timeout = 120
bind = "0.0.0.0:10000"
worker_class = "gthread"
accesslog = "-"  # Log to stdout
errorlog = "-"   # Log to stdout
