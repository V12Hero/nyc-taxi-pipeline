#!/bin/bash

PYTHON_PATH="/usr/bin/python3"
SCRIPT_PATH="/path/to/your/project/process_data.py"

# Define the cron job schedule
CRON_SCHEDULE="0 0 1 * *"

# Define the full cron job command
CRON_JOB="$CRON_SCHEDULE $PYTHON_PATH $SCRIPT_PATH"

# Check if the cron job already exists
(crontab -l | grep -F "$CRON_JOB") && echo "Cron job already exists." || (crontab -l; echo "$CRON_JOB") | crontab -

echo "Current crontab:"
crontab -l

if crontab -l | grep -F "$CRON_JOB"; then
    echo "Cron job added successfully."
else
    echo "Failed to add cron job."
fi
