# Why You're Getting DLP Alerts: Complete Guide

This is a classic **data egress / sensitive data exposure** scenario. Here's a comprehensive breakdown of likely causes
and how to resolve each.

---

## 🔍 Root Causes

### 1. Jupyter Notebook Output Contains Sensitive Data

The most common trigger. When `plot_scurves` renders a graph, the **underlying data used to build the plot is embedded
in the notebook cell output** (as JSON/HTML in the `.ipynb` file). If the S3 data contains PII, PHI, financial records,
or other sensitive fields, the notebook auto-saves that output to disk on the EMR master node.

**Example — How this happens:**

Imagine your S3 file looks like this:

```
customer_id | ssn         | credit_score | default_flag
C001        | 123-45-6789 | 620          | 1
C002        | 987-65-4321 | 580          | 0
```

You load it and plot:

```python
import pandas as pd

df = pd.read_csv("s3://my-bucket/customers.csv")
plot_scurves(df)  # Uses credit_score and default_flag
```

Even though the **chart looks like a simple curve**, the `.ipynb` file now contains this in its output cell:

```json
{
  "output_type": "display_data",
  "data": {
    "application/json": {
      "x": [
        580,
        620,
        ...
      ],
      "customer_id": [
        "C001",
        "C002",
        ...
      ],
      "ssn": [
        "123-45-6789",
        "987-65-4321",
        ...
      ]
      ←
      SSN
      sitting
      here
      in
      plain
      text
    }
  }
}
```

The DLP tool scans this `.ipynb` file (which auto-saves every few minutes) and finds SSNs matching the pattern
`\d{3}-\d{2}-\d{4}` → **Alert fires**.

If the master node syncs or the user downloads/shares the `.ipynb` file, DLP scans it and fires.

---

### 2. S3 Data Being Read Contains Flagged Content

DLP tools can monitor **S3 access patterns and network traffic** at the corporate level. Simply reading a sensitive S3
bucket in bulk can trigger an alert, even before you do anything with the data.

**Example — Scenario A: CloudTrail-based DLP**

Your company's DLP has a rule:
> *"Alert when any user reads more than 10,000 records from S3 buckets tagged `data-classification: PII`"*

Your user runs:

```python
# Reads 2 million rows from a PII-tagged S3 bucket
df = spark.read.parquet("s3://prod-customer-data/full_dataset/")
```

CloudTrail logs this as a `GetObject` call on a sensitive bucket → **DLP alert fires** even before the plot is drawn.

**Example — Scenario B: Network DLP**

The corporate proxy monitors outbound traffic. When Spark reads from S3:

```
EMR Node → S3 VPC Endpoint → Data flows to EMR memory
```

If S3 traffic is **not routed through a VPC endpoint** and goes over the public internet instead, the DLP proxy
intercepts it and scans the payload → **Alert fires**.

DLP tools often monitor:

- **Network traffic** — data flowing from S3 → EMR → browser could be inspected
- **S3 API calls** — some DLP tools integrate with CloudTrail/S3 access logs and flag bulk reads of sensitive buckets

---

### 3. Browser Rendering = Data Leaving the "Safe Zone"

This is the **most subtle cause**. Jupyter runs a kernel on the EMR server, but the **output is sent to the browser over
a WebSocket connection**. This means sensitive data physically travels from the server to the user's laptop.

**Data Flow:**

```
S3 Bucket (PHI data)
    ↓  [Spark reads 500k patient records]
EMR Master Node Memory
    ↓  [plot_scurves processes data, Jupyter prepares output]
WebSocket (ws://emr-master:8888)
    ↓  [Data travels over the network to browser]
Chrome on User's Laptop  ← DLP agent installed here sees the data
```

The DLP agent sitting on the user's **laptop/browser** inspects incoming WebSocket traffic and sees:

```
patient_id: P1023, diagnosis_code: Z87.891, dob: 1978-05-14
patient_id: P1024, diagnosis_code: Z87.891, dob: 1965-11-02
...
```

Even though the user only sees a smooth S-curve graph, the **raw data payload** behind it crossed the network boundary
from server to laptop → **DLP alert fires**.

The plot data travels:

```
S3 → EMR (PySpark/Pandas) → Jupyter kernel → WebSocket → User's Browser
```

---

### 4. Notebook Being Saved/Exported or Sent

A `.ipynb` file is just a JSON document. When a user **downloads, emails, or shares** it, they are unknowingly
transferring all the embedded cell outputs — including sensitive data — outside the secure environment.

**Example — User downloads notebook with output:**

The notebook cell output (invisibly) contains:

```json
"outputs": [
{
"data": {
"text/html": [
"<table><tr><td>John Smith</td><td>SSN: 123-45-6789</td><td>Score: 580</td></tr>..."
]
}
}
]
```

User then:

- **Emails** `analysis_notebook.ipynb` to a colleague → DLP scans the email attachment → Alert fires
- **Uploads** it to a shared Google Drive → DLP scans the upload → Alert fires
- **Pastes** the plot into a Teams/Slack message → DLP inspects the clipboard content → Alert fires

---

### 5. EMR Master Node is Internet-Exposed

If the EMR master node has a **public IP** and port `8888` is open in the Security Group, the Jupyter server is
reachable from the internet. DLP tools flag this as a potential exfiltration path.

**Example — Risky Setup:**

```
EMR Security Group (Bad Configuration):
┌─────────────────────────────────┐
│  Inbound Rules:                 │
│  Port 22   → 0.0.0.0/0  ← SSH  │
│  Port 8888 → 0.0.0.0/0  ← ⚠️  │  Jupyter exposed to entire internet
└─────────────────────────────────┘
```

The user accesses Jupyter via:

```
http://54.23.101.45:8888/?token=abc123   ← Public IP, no encryption
```

Data flows:

```
S3 sensitive data → EMR → Public Internet → User's Browser
```

No encryption, no VPN, no access control beyond a token → **DLP flags this as a high-risk exfiltration channel**.

---

## ✅ How to Resolve It

### Step 1 — Identify the Exact DLP Trigger

Work with your Cyber Security team to answer:

- What **policy** fired? (PII? PHI? Credit card patterns? IP?)
- What **data channel** was flagged? (Email? Web upload? File transfer? Network traffic?)
- What **content** was detected? (Actual cell output? S3 object content?)

This narrows down the fix significantly.

---

### Step 2 — Sanitize the Data Before Plotting

Only pass the **minimum non-sensitive columns** needed to draw the curve.

```python
import pandas as pd
import hashlib

df = pd.read_csv("s3://my-bucket/customers.csv")

# ❌ BAD — passes entire dataframe including PII to the plot
plot_scurves(df)

# ✅ GOOD — strip all PII, keep only what the curve needs
plot_df = df[['credit_score', 'default_flag']].copy()
plot_scurves(plot_df)

# ✅ EVEN BETTER — if you must keep an ID, hash it
df['customer_id'] = df['customer_id'].apply(
    lambda x: hashlib.sha256(x.encode()).hexdigest()[:8]
)
plot_df = df[['customer_id', 'credit_score', 'default_flag']]
plot_scurves(plot_df)
```

Now the notebook output only contains `[620, 580, ...]` — no SSNs, no names → **DLP has nothing to flag**.

**Alternative approach — Mask PII before plotting:**

```python
# Mask PII before plotting
df['customer_id'] = df['customer_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())

# Or drop sensitive columns entirely — only keep what's needed for the curve
plot_df = df[['score', 'label']]  # No PII columns
plot_scurves(plot_df)
```

---

### Step 3 — Clear Notebook Output Before Saving/Sharing

Strip all outputs from the notebook before it leaves the EMR environment. Raw output (including plot data) is stored in
the `.ipynb` file.

**From the terminal on EMR:**

```bash
# Clear outputs from notebook file before downloading it
jupyter nbconvert --clear-output --inplace /home/hadoop/my_analysis.ipynb

# Verify the outputs are gone
cat /home/hadoop/my_analysis.ipynb | python3 -c "
import sys, json
nb = json.load(sys.stdin)
outputs = [cell.get('outputs', []) for cell in nb['cells']]
print('Total output cells:', sum(len(o) for o in outputs))
# Should print: Total output cells: 0
"
```

**From Jupyter UI:**

- **Kernel → Restart & Clear Output**
- Then **File → Save**

Now the `.ipynb` file is just code — no embedded data → **safe to email or download**.

---

### Step 4 — Keep Jupyter Traffic Internal with SSH Tunnel

Instead of opening port 8888 to the internet, use an encrypted **SSH tunnel** so traffic never leaves the private
network.

**How to set it up:**

```bash
# On the user's local laptop — create an SSH tunnel
# This maps localhost:8888 on your laptop → port 8888 on the EMR master node
ssh -i ~/keys/my-emr-key.pem \
    -L 8888:localhost:8888 \
    -N \                        # Don't execute a command, just tunnel
    hadoop@ec2-54-23-101-45.compute-1.amazonaws.com
```

Now the user opens their browser to:

```
http://localhost:8888   ← Looks local, but is actually tunneled securely to EMR
```

**Recommended alternative — Use AWS Systems Manager Session Manager:**

```bash
aws ssm start-session --target i-0123456789abcdef0 \
    --document-name AWS-StartPortForwardingSession \
    --parameters "localPortNumber=8888,portNumber=8888,host=localhost"
```

**What changes in the data flow:**

```
BEFORE (Risky):
S3 → EMR → Public Internet (unencrypted) → Browser
                  ↑ DLP sees this

AFTER (Safe):
S3 → EMR → SSH Encrypted Tunnel (localhost) → Browser
                  ↑ DLP cannot inspect encrypted SSH traffic
```

---

### Step 5 — Work with Aggregated/Anonymized S3 Data

Create a **separate anonymized dataset in S3** specifically for visualization — never read raw PII data for plotting
purposes.

```python
# ❌ BAD — reading raw production data with PII
raw_df = spark.read.parquet("s3://prod-data/customers/raw/")
plot_scurves(raw_df)  # PII flows all the way to the browser


# ✅ GOOD — pre-process and store anonymized data first (run this as a separate job)
def create_plot_ready_dataset():
    raw_df = spark.read.parquet("s3://prod-data/customers/raw/")

    plot_df = raw_df.select(
        F.col("credit_score"),  # Not sensitive
        F.col("default_flag"),  # Not sensitive
        F.col("segment")  # Not sensitive
        # Drop: name, ssn, dob, address, account_number
    )

    # Save anonymized version to a separate bucket
    plot_df.write.parquet("s3://analytics-safe/customers/plot_ready/")


# In the notebook — only read the safe dataset
safe_df = spark.read.parquet("s3://analytics-safe/customers/plot_ready/")
plot_scurves(safe_df)  # No PII ever enters the notebook
```

**Advanced — Use S3 Object Lambda to auto-redact on read:**

```
User reads from S3 → Lambda intercepts → Strips PII fields → Returns clean data
```

This way even if someone reads the wrong bucket, they only get redacted data.

If the S3 bucket contains raw sensitive data, also consider:

- Reading a **pre-aggregated or anonymized version** for visualization purposes
- Using **S3 Object Lambda** to redact fields on read
- Storing plot-ready (non-sensitive) datasets in a separate S3 path

---

### Step 6 — Lock Down the EMR Security Group

Change the Security Group so **only internal/VPN traffic** can reach Jupyter.

```
❌ BEFORE (Dangerous):
Inbound Rule: TCP Port 8888 | Source: 0.0.0.0/0 (entire internet)

✅ AFTER (Secure):
Inbound Rule: TCP Port 8888 | Source: 10.0.0.0/8 (internal VPC CIDR only)
   OR
Inbound Rule: TCP Port 8888 | Source: <Corporate VPN IP range>
   OR
Remove port 8888 entirely — use SSH tunnel instead (Step 4)
```

**Terraform example — restrict Jupyter to VPC internal traffic only:**

```hcl
resource "aws_security_group_rule" "jupyter_internal_only" {
  type        = "ingress"
  from_port   = 8888
  to_port     = 8888
  protocol    = "tcp"
  cidr_blocks = ["10.0.0.0/8"]   # VPC internal only — NOT 0.0.0.0/0
  security_group_id = aws_security_group.emr_master.id
}
```

Ensure the EMR master node **Security Group** blocks inbound 8888 from `0.0.0.0/0`. Use a **VPN or bastion host** for
access.

---

### Step 7 — Request a DLP Exemption or Whitelist (if legitimate)

If the data access is **fully authorized** and the workflow is secure:

- Work with Cyber Security to **whitelist the EMR master node IP** or the specific S3 bucket from DLP scanning
- Document the data flow and get it approved as a **sanctioned exception**

**What to prepare for the exemption request:**

```
📋 DLP Exemption Request — Example Documentation

1. Business Justification:
   "Data Scientist [Name] needs to read customer credit data from S3 bucket
   'prod-risk-analytics' to build model validation curves for the Risk team."

2. Data Classification:
   Bucket: s3://prod-risk-analytics/model-validation/
   Classification: PII - Tier 2
   Fields accessed: credit_score, default_flag (no direct identifiers)

3. Security Controls in Place:
   ✅ SSH tunnel used — no public port exposure
   ✅ Data anonymized before plotting (PII columns dropped)
   ✅ Notebook outputs cleared before any sharing
   ✅ EMR runs in private subnet with no public IP
   ✅ S3 access via VPC endpoint (no public internet routing)

4. Requested Exemption:
   Whitelist EMR Master Node IP: 10.0.1.45
   Whitelist S3 bucket: prod-risk-analytics (read-only)
   Duration: 90 days, renewable
```

---

## 📋 Quick Summary Table

| Root Cause                                         | Primary Fix                                        | Secondary Steps                         |
|----------------------------------------------------|----------------------------------------------------|-----------------------------------------|
| Sensitive data in plot/cell output                 | Mask/aggregate data before plotting (Step 2)       | Clear outputs before sharing (Step 3)   |
| `.ipynb` file contains embedded sensitive output   | Clear outputs before saving/sharing (Step 3)       | Use data sanitization (Step 2)          |
| Browser receiving raw sensitive data via WebSocket | Use SSH tunnel, keep traffic internal (Step 4)     | Lock down Security Group (Step 6)       |
| EMR master node publicly accessible                | Lock down Security Group, use VPN/SSM (Step 6)     | Use SSH tunnel (Step 4)                 |
| Reading raw PII/PHI S3 files directly              | Use pre-anonymized datasets for viz (Step 5)       | Create separate anonymized S3 paths     |
| Legitimate workflow being over-flagged             | Request DLP whitelist with Cyber Security (Step 7) | Implement all preventive controls first |

---

## 🗺️ Decision Flow to Diagnose Your Specific Case

```
DLP Alert Received
        ↓
Ask Cyber Security: "What channel triggered it?"
        ↓
   ┌────┴─────────────────────────────────────┐
Email/File    Network Traffic    S3 Access Log    Endpoint Agent
   ↓               ↓                  ↓               ↓
Step 3          Step 4            Step 5           Step 2
Clear outputs  SSH Tunnel      Use anon dataset   Mask data
```

---

## 🎯 Most Likely Culprits

The most likely culprits are **#1 (Notebook Output) or #3 (Browser Rendering)**:

- **Sensitive data flowing from S3 through the notebook to the browser**
- The combination of **Step 2 + Step 3 + Step 4** resolves the vast majority of cases without needing any exemption

**Start here:**

1. Sanitize the data before plotting (Step 2)
2. Clear notebook outputs (Step 3)
3. Work with Cyber Security to identify the exact policy that fired
4. Implement SSH tunnel for secure access (Step 4)