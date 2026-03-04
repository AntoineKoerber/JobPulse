PID_FILE := .server.pid
LOG_FILE := .server.log

start:
	@if [ -f $(PID_FILE) ] && kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		echo "Server already running (PID $$(cat $(PID_FILE)))"; \
	else \
		source venv/bin/activate && \
		nohup uvicorn src.main:app --reload > $(LOG_FILE) 2>&1 & \
		echo $$! > $(PID_FILE); \
		echo "Server started (PID $$(cat $(PID_FILE))) — logs in $(LOG_FILE)"; \
	fi

stop:
	@if [ -f $(PID_FILE) ] && kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		kill $$(cat $(PID_FILE)) && rm $(PID_FILE); \
		echo "Server stopped"; \
	else \
		echo "Server not running"; \
		rm -f $(PID_FILE); \
	fi

logs:
	@tail -f $(LOG_FILE)

status:
	@if [ -f $(PID_FILE) ] && kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		echo "Running (PID $$(cat $(PID_FILE)))"; \
	else \
		echo "Not running"; \
	fi

restart: stop start

.PHONY: start stop logs status restart
