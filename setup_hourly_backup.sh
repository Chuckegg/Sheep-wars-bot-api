#!/bin/bash
# Setup script for hourly backups on Linux
# This will configure cron to run the backup script every hour

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_PATH=$(which python3)

echo "========================================"
echo "Hourly Backup Setup for stats.xlsx"
echo "========================================"
echo ""
echo "Script directory: $SCRIPT_DIR"
echo "Python path: $PYTHON_PATH"
echo ""

# Check if backup_hourly.py exists
if [ ! -f "$SCRIPT_DIR/backup_hourly.py" ]; then
    echo "[ERROR] backup_hourly.py not found in $SCRIPT_DIR"
    exit 1
fi

# Make the Python script executable
chmod +x "$SCRIPT_DIR/backup_hourly.py"
echo "[OK] Made backup_hourly.py executable"

# Create the cron entry
CRON_ENTRY="0 * * * * cd $SCRIPT_DIR && $PYTHON_PATH $SCRIPT_DIR/backup_hourly.py >> $SCRIPT_DIR/backup_hourly.log 2>&1"

echo ""
echo "Cron entry to add:"
echo "----------------------------------------"
echo "$CRON_ENTRY"
echo "----------------------------------------"
echo ""

# Check if cron entry already exists
if crontab -l 2>/dev/null | grep -q "backup_hourly.py"; then
    echo "[INFO] Cron entry for backup_hourly.py already exists"
    echo ""
    echo "Current crontab entries for backup:"
    crontab -l | grep backup_hourly.py
else
    echo "[SETUP] Adding cron entry..."
    
    # Add to crontab
    (crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -
    
    if [ $? -eq 0 ]; then
        echo "[SUCCESS] Cron entry added successfully!"
        echo ""
        echo "The backup will now run automatically every hour at :00 minutes"
        echo "Logs will be written to: $SCRIPT_DIR/backup_hourly.log"
    else
        echo "[ERROR] Failed to add cron entry"
        echo ""
        echo "You can manually add it with:"
        echo "  crontab -e"
        echo ""
        echo "Then add this line:"
        echo "  $CRON_ENTRY"
        exit 1
    fi
fi

echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "To verify the cron job:"
echo "  crontab -l"
echo ""
echo "To view backup logs:"
echo "  tail -f $SCRIPT_DIR/backup_hourly.log"
echo ""
echo "To manually run the backup:"
echo "  cd $SCRIPT_DIR && python3 backup_hourly.py"
echo ""
echo "To remove the cron job:"
echo "  crontab -e"
echo "  (then delete the line with backup_hourly.py)"
echo ""
