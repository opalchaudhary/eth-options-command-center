# DigitalOcean Droplet Deployment

This guide deploys the ETH Options Command Center on a single Ubuntu 24.04 Droplet with FastAPI, APScheduler, Streamlit, systemd services, Supabase, and public Delta Exchange APIs.

The deployment uses public Delta market-data endpoints by default. Add `DELTA_API_KEY` or `DELTA_API_SECRET` only if you intentionally need private API features elsewhere.

## 1. Create The Droplet

Use Ubuntu 24.04 LTS. SSH into the server as a sudo-capable user.

```bash
sudo apt update
sudo apt install -y python3-pip python3-venv git
```

## 2. Clone The Repo

The examples below use `/opt/eth-options-command-center` and a Linux user named `deploy`. If your repo path or user is different, update both systemd service files before installing them.

```bash
sudo mkdir -p /opt/eth-options-command-center
sudo chown -R deploy:deploy /opt/eth-options-command-center
git clone YOUR_GITHUB_REPO_URL /opt/eth-options-command-center
cd /opt/eth-options-command-center
```

## 3. Create The Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -r backend/requirements.txt
```

## 4. Configure Environment

Create `/opt/eth-options-command-center/.env`:

```bash
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
USE_FASTAPI_BACKEND=true
FASTAPI_BACKEND_URL=http://127.0.0.1:8000
BACKEND_SCHEDULER_ENABLED=true
BACKEND_JOB_TIMEOUT_SECONDS=50
```

Leave `DELTA_API_KEY` and `DELTA_API_SECRET` unset for public Delta API mode. `.env` and `backend/.env` are excluded from git.

## 5. Prepare Runtime Folders And Scripts

```bash
mkdir -p logs
chmod +x deployment/start_fastapi.sh
chmod +x deployment/start_streamlit.sh
```

## 6. Install systemd Services

Edit `deployment/eth-fastapi.service` and `deployment/eth-streamlit.service` if your Linux user or project path differs from:

```text
User=deploy
Group=deploy
WorkingDirectory=/opt/eth-options-command-center
```

Install and start the services:

```bash
sudo cp deployment/eth-fastapi.service /etc/systemd/system/eth-fastapi.service
sudo cp deployment/eth-streamlit.service /etc/systemd/system/eth-streamlit.service
sudo systemctl daemon-reload
sudo systemctl enable eth-fastapi
sudo systemctl enable eth-streamlit
sudo systemctl start eth-fastapi
sudo systemctl start eth-streamlit
```

## 7. Verify Services

```bash
systemctl status eth-fastapi
systemctl status eth-streamlit
journalctl -u eth-fastapi -f
journalctl -u eth-streamlit -f
```

Health checks:

```text
http://SERVER_IP:8000/health
http://SERVER_IP:8000/system/status
http://SERVER_IP:8501
```

Runtime logs are written to `logs/` and journald. Backend Python logs rotate through `logs/fastapi.log`.

## 8. Configure Firewall

For direct testing:

```bash
sudo ufw allow OpenSSH
sudo ufw allow 80
sudo ufw allow 443
sudo ufw allow 8501
sudo ufw enable
```

After Nginx is active and tested, you can remove public `8501` exposure and keep Streamlit bound behind the reverse proxy.

## 9. Future Nginx Reverse Proxy

The included `deployment/nginx_example.conf` proxies:

- Streamlit at `/` to `localhost:8501`
- FastAPI at `/api` to `localhost:8000`
- WebSocket upgrade headers for Streamlit
- Long proxy timeouts compatible with Streamlit sessions

Install Nginx when ready:

```bash
sudo apt install -y nginx
sudo cp deployment/nginx_example.conf /etc/nginx/sites-available/eth-options-command-center
sudo ln -s /etc/nginx/sites-available/eth-options-command-center /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

With this example config, FastAPI `/health` is available through Nginx as `/api/health`.

## 10. Reboot Persistence

Confirm both services survive reboot:

```bash
sudo reboot
```

After reconnecting:

```bash
systemctl status eth-fastapi
systemctl status eth-streamlit
```

## Deployment Checklist

- Ubuntu 24.04 Droplet created.
- `python3-pip`, `python3-venv`, and `git` installed.
- Repo cloned to `/opt/eth-options-command-center` or service files updated for your path.
- Virtual environment created at `venv/`.
- Root and backend requirements installed.
- `.env` configured with Supabase values.
- `BACKEND_SCHEDULER_ENABLED=true`.
- `BACKEND_JOB_TIMEOUT_SECONDS=50`.
- Delta private keys omitted for public API mode.
- `logs/` directory exists.
- `chmod +x deployment/start_fastapi.sh` completed.
- `chmod +x deployment/start_streamlit.sh` completed.
- systemd services copied, enabled, and started.
- `systemctl status eth-fastapi` is healthy.
- `systemctl status eth-streamlit` is healthy.
- `/health`, `/system/status`, and Streamlit URLs load.
- Firewall allows OpenSSH, 80, 443, and temporary 8501 access.
- Nginx config reviewed before production reverse proxy use.
