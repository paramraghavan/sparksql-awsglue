# JupyterLab Remote Access On Home Network

This guide shows how to run JupyterLab on one computer, called **PC1**, and open it from another computer, called **PC2**, on the same home network.

Use **JupyterLab** as the default. It includes notebooks, a file browser, terminals, text editors, and multiple tabs. Use classic **Jupyter Notebook** only if JupyterLab does not work or if you want a lighter, older interface.

Use this only on a trusted home network. Do not expose Jupyter to the public internet or set up router port forwarding for port `8888`.

## 1. Mental Model

```text
PC1 = computer running Python, PySpark, and JupyterLab
PC2 = another computer on the same Wi-Fi/home network
```

JupyterLab runs on PC1:

```text
http://0.0.0.0:8888
```

PC2 opens Jupyter by using PC1's home-network IP address:

```text
http://PC1_IP_ADDRESS:8888/lab
```

Example:

```text
http://192.168.1.25:8888/lab
```

## 2. Install JupyterLab On PC1

Activate the Spark lab virtual environment on PC1.

Mac:

```bash
cd ~/spark-glue-local-lab
source .venv/bin/activate
```

Windows PowerShell:

```powershell
cd C:\spark-glue-local-lab
.\.venv\Scripts\Activate.ps1
```

Install JupyterLab and an IPython kernel:

```bash
python -m pip install --upgrade jupyterlab ipykernel
```

Verify:

```bash
jupyter lab --version
```

Optional: install classic Jupyter Notebook only if JupyterLab does not work or you prefer the older lightweight interface:

```bash
python -m pip install --upgrade notebook
jupyter notebook --version
```

## 3. Set A JupyterLab Password On PC1

Set the password on PC1 before starting the server.

Recommended command:

```bash
jupyter server password
```

If that command is not available, use:

```bash
jupyter notebook password
```

You will be prompted to enter and confirm a password.

Use a real password, not something like `0`, `1234`, or `password`. Anyone on the same home network who can reach PC1 may be able to try the login page.

## 4. Start JupyterLab On PC1

Run this on PC1:

```bash
cd ~/spark-glue-local-lab
source .venv/bin/activate

jupyter lab --ip=0.0.0.0 --port=8888 --no-browser
```

Windows PowerShell version:

```powershell
cd C:\spark-glue-local-lab
.\.venv\Scripts\Activate.ps1

jupyter lab --ip=0.0.0.0 --port=8888 --no-browser
```

What the options mean:

| Option | Meaning |
|---|---|
| `--ip=0.0.0.0` | Listen on all network interfaces, not only localhost |
| `--port=8888` | Use port 8888 |
| `--no-browser` | Do not open a browser on PC1 automatically |

## 5. Optional: Use Classic Jupyter Notebook Instead

Use this only if JupyterLab does not work or you want the older lightweight interface.

Start classic Notebook on PC1:

```bash
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser
```

From PC2, open:

```text
http://PC1_IP_ADDRESS:8888/tree
```

Example:

```text
http://192.168.1.25:8888/tree
```

## 6. Find PC1's IP Address

On Mac PC1:

```bash
ipconfig getifaddr en0
```

Example output:

```text
192.168.1.25
```

If that returns nothing, try:

```bash
ifconfig | grep "inet "
```

On Windows PC1:

```powershell
ipconfig
```

Look for the Wi-Fi or Ethernet `IPv4 Address`, for example:

```text
192.168.1.25
```

## 7. Open JupyterLab From PC2

On PC2, open a browser and go to:

```text
http://PC1_IP_ADDRESS:8888/lab
```

Example:

```text
http://192.168.1.25:8888/lab
```

For classic Notebook, if you chose the optional Notebook path:

```text
http://192.168.1.25:8888/tree
```

JupyterLab should ask for the password you set on PC1.

If Jupyter prints a token URL in the PC1 terminal, you can ignore the token and open `/lab` from PC2. The password login should work after you set the Jupyter password.

## 8. Allow Firewall Access If Needed

If PC2 cannot connect, PC1 may be blocking the connection.

Mac:

- Open **System Settings**.
- Go to **Network** or **Firewall** settings.
- Allow incoming connections for Python, Terminal, or the Jupyter process if prompted.

Windows:

- Open **Windows Defender Firewall**.
- Allow Python through the firewall on private networks.
- Make sure you are on a private/home network profile, not public.

You can also test from PC2:

```bash
ping PC1_IP_ADDRESS
```

Example:

```bash
ping 192.168.1.25
```

## 9. Stop JupyterLab

On PC1, press:

```text
Ctrl+C
```

Jupyter may ask for confirmation. Press `y` and Enter.

## 10. Safety Checklist

- Use this only on your trusted home network.
- Keep the Jupyter password enabled.
- Do not disable token/password authentication.
- Do not use router port forwarding.
- Do not expose port `8888` to the internet.
- Stop Jupyter when you are done.

## 11. Common Problems

`PC2 cannot open the page`

- Confirm Jupyter is still running on PC1.
- Confirm PC1 and PC2 are on the same Wi-Fi/home network.
- Confirm the PC1 IP address is correct.
- Check firewall settings on PC1.

`Password does not work`

- Stop Jupyter.
- Run `jupyter server password` again on PC1.
- Start Jupyter again with `--ip=0.0.0.0 --port=8888 --no-browser`.

`Port 8888 is already in use`

Use another port:

```bash
jupyter lab --ip=0.0.0.0 --port=8890 --no-browser
```

Then open from PC2:

```text
http://PC1_IP_ADDRESS:8890/lab
```
