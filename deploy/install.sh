#!/bin/bash
# Run as root on CT104 to install/update Safety Monitor
set -e

APP_DIR=/opt/safety-monitor
REPO_URL=git@github.com:rpiasentin/safety-monitor.git

echo "=== Safety Monitor Install/Update ==="

# Dependencies
apt-get update -qq
apt-get install -y -qq git python3-pip

# App user
id safetymon &>/dev/null || useradd -r -s /usr/sbin/nologin -m -d $APP_DIR safetymon

# Directories
mkdir -p $APP_DIR/{app,logs,data}

# Clone or pull
if [ -d "$APP_DIR/app/.git" ]; then
  echo "Updating repo..."
  git -C $APP_DIR/app pull
else
  echo "Cloning repo..."
  git clone $REPO_URL $APP_DIR/app
fi

# Python deps
pip3 install --break-system-packages -q -r $APP_DIR/app/requirements.txt

# .env
if [ ! -f "$APP_DIR/.env" ]; then
  cp $APP_DIR/app/.env.example $APP_DIR/.env
  echo ""
  echo ">>> IMPORTANT: Edit $APP_DIR/.env with your credentials <<<"
  echo ""
fi

# Systemd
cp $APP_DIR/app/deploy/safety-monitor.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable safety-monitor
systemctl restart safety-monitor

# Permissions
chown -R safetymon:safetymon $APP_DIR
chmod 600 $APP_DIR/.env

echo ""
echo "=== Done. Dashboard at http://$(hostname -I | awk '{print $1}'):8000 ==="
systemctl status safety-monitor --no-pager
