#!/bin/bash
cat <<'EOF_SERVICE' > /etc/systemd/system/drop-caches.service
[Unit]
Description=Drop Linux page cache

[Service]
Type=oneshot
ExecStart=/bin/sh -c 'echo 3 > /proc/sys/vm/drop_caches'
EOF_SERVICE

cat <<'EOF_TIMER' > /etc/systemd/system/drop-caches.timer
[Unit]
Description=Run drop-caches every 10 seconds

[Timer]
OnBootSec=10
OnUnitActiveSec=10
AccuracySec=1

[Install]
WantedBy=timers.target
EOF_TIMER

systemctl daemon-reexec
systemctl daemon-reload
systemctl enable --now drop-caches.timer