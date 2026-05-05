# cronwatch

Lightweight daemon that monitors cron job execution times and alerts on drift or failure.

---

## Installation

```bash
pip install cronwatch
```

Or install from source:

```bash
git clone https://github.com/youruser/cronwatch.git && cd cronwatch && pip install .
```

---

## Usage

Define your monitored jobs in a config file (`cronwatch.yaml`):

```yaml
jobs:
  daily-backup:
    schedule: "0 2 * * *"
    tolerance: 300        # seconds of acceptable drift
    alert: slack

  hourly-sync:
    schedule: "0 * * * *"
    tolerance: 60
    alert: email

alerts:
  slack:
    webhook: "https://hooks.slack.com/services/..."
  email:
    to: "ops@example.com"
```

Start the daemon:

```bash
cronwatch start --config cronwatch.yaml
```

Notify cronwatch when a job runs (add to your cron command):

```bash
0 2 * * * /usr/bin/backup.sh && cronwatch ping daily-backup
```

Check status of all monitored jobs:

```bash
cronwatch status
```

---

## How It Works

cronwatch tracks the last execution time of each registered job. If a job exceeds its expected schedule window by more than the configured tolerance, or fails to run entirely, an alert is dispatched through the configured channel.

---

## License

MIT © 2024 cronwatch contributors