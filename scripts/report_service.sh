#!/bin/bash

# Define variables
APP_DIR=~/git/rh_gizmos/
LOG_DIR=~/logs/rh_gizmos
PYTHONPATH=$APP_DIR/src
VENV_DIR=~/git/rh_absa/.venv
LOG_FILE=$LOG_DIR/gizmos_service.log
APP_MODULE="reporting/app_act_sector_report.py"  # Dash app file

# Start function
start() {
    # Check for any existing Dash app processes and kill them
    PIDS=$(pgrep -f "python.*$APP_MODULE")
    if [ -n "$PIDS" ]; then
        echo "Stopping existing Dash app processes..."
        kill -9 $PIDS
    fi

    echo "Starting Dash app..."
    export PYTHONPATH=$PYTHONPATH
    cd $APP_DIR/src
    . $VENV_DIR/bin/activate
    mkdir -p $LOG_DIR
    nohup python $APP_MODULE > $LOG_FILE 2>&1 &
    echo "Dash app started with PID $!"
}

# Stop function
stop() {
    echo "Stopping Dash app..."
    PIDS=$(pgrep -f "python.*$APP_MODULE")
    if [ -n "$PIDS" ]; then
        kill -9 $PIDS
        echo "Dash app stopped."
    else
        echo "No Dash app processes found."
    fi
}

# Status function
status() {
    PIDS=$(pgrep -f "python.*$APP_MODULE")
    if [ -n "$PIDS" ]; then
        echo "Dash app is running with PIDs: $PIDS"
    else
        echo "Dash app is not running."
    fi
}

# Help function
help() {
    echo "Usage: $0 {start|stop|status|restart}"
}

# Handle command line arguments
case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)
        status
        ;;
    restart)
        stop
        start
        ;;
    *)
        help
        ;;
esac
