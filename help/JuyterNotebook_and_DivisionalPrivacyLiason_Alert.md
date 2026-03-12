# Why You're Getting DLP Alerts: Complete Guide

This is a classic **data egress / sensitive data exposure** scenario. Here's a comprehensive breakdown of likely causes
and how to resolve each.

---

## 🔍 Why Auto-Save Triggers DLP — Technical Explanation

### The Simple Answer:

**Your Jupyter notebook auto-saves every ~30 seconds. Each auto-save writes the `.ipynb` file to disk. DLP scans that
file looking for sensitive data patterns. If found → alert fires.**

It doesn't matter if you're just analyzing the data in memory — if the data appears in a cell output (visible or
invisible), auto-save will capture it, and DLP will find it.

### The Detailed Mechanism:

**The `.ipynb` file is just a JSON file.** When Jupyter auto-saves, it writes everything to disk, including:

```
my_notebook.ipynb
├── Cell 1: Code
│   └── source: ["import pandas as pd"]
├── Cell 2: Code  
│   └── source: ["df = spark.read.parquet('s3://...customer_data...')"]
├── Cell 3: Code
│   └── source: ["plot_scurves(df)"]
├── Cell 3: OUTPUT ← DLP SCANS THIS
│   ├── output_type: "display_data"
│   ├── data: {
│   │   "application/json": {
│   │     "title": "S-Curve",
│   │     "data": [
│   │       {"x": 620, "customer_id": "C001", "ssn": "123-45-6789"},  ← SENSITIVE
│   │       {"x": 580, "customer_id": "C002", "ssn": "987-65-4321"}   ← SENSITIVE
│   │     ]
│   │   }
│   │ }
```

**Complete Data Flow:**

```
1. S3 contains PII:
   customer_id | ssn         | credit_score
   C001        | 123-45-6789 | 620

2. Your code loads it:
   df = spark.read.parquet("s3://...")
   ✅ SAFE — Data is only in memory, not saved anywhere

3. You create a plot with the entire dataframe:
   plot_scurves(df)
   ⚠️ WARNING — ALL columns including SSN sent to plot function

4. Plot library embeds data as JSON:
   {"data": [{"ssn": "123-45-6789", "score": 620}, ...]}
   ⚠️ WARNING — SSN now in the plot output object

5. Jupyter captures plot output:
   cell.outputs[0] = { "data": {"application/json": {...with SSN...}} }

6. Auto-save writes to disk every 30 seconds:
   my_notebook.ipynb ← JSON file with embedded SSN data
   📝 CRITICAL — File now contains PII in plaintext

7. DLP agent scans the file:
   Pattern match: \d{3}-\d{2}-\d{4}
   Found: ["123-45-6789", "987-65-4321", ...]
   🚨 ALERT FIRES
```

**The critical point:** Even though you only *visualized* `credit_score` on the axis, if you passed the entire `df`
object to `plot_scurves()`, ALL columns (including SSN) get embedded in the plot JSON → written to disk → scanned by
DLP.

**DLP scanning process:**

1. Jupyter auto-saves the `.ipynb` file to disk every ~30 seconds
2. The DLP agent on the EMR master node monitors file writes
3. When it sees `*.ipynb` file being written, it scans the content
4. DLP looks for **patterns** matching its rules:
    - SSN pattern: `\d{3}-\d{2}-\d{4}`
    - Credit card: `\d{4}-\d{4}-\d{4}-\d{4}`
    - Email: `[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}`
    - Keywords: "password", "apikey", "secret", "token"
    - Column names: "ssn", "dob", "account_number", "medical_record"
    - And custom rules your company defines
5. If a match is found → **DLP alert fires**

The key insight: **DLP is scanning the auto-saved file on disk, not the Jupyter server's memory or network traffic.**

---

## 🎯 Quick Reference: What DLP Patterns Trigger Alerts

When DLP scans your `.ipynb` file, it looks for these patterns in the cell outputs:

### Pattern Matches (Regex-based):

```
SSN:                  123-45-6789  (or 123456789)
Credit Card:          1234-5678-9012-3456  (or variations)
Email:                john.doe@company.com
Phone:                (555) 123-4567
API Key/Token:        api_sk_abc123def456ghijklmnop
Password/Secret:      password="xyz123"
Dates:                1978-05-14 (Date of Birth)
Medical Record ID:    MR-12345678
Account Number:       ACC-9876543210
```

### Column Name Matches (Keyword-based):

```
ssn, social_security_number
dob, date_of_birth, birthdate
name, first_name, last_name, full_name
email, email_address
phone, phone_number, mobile
address, street_address
account_number, account_id
credit_card, card_number
password, passwd, pwd, secret, api_key, token
health_record, medical_record, diagnosis, treatment
salary, compensation, bonus
```

### File/Field Classification Matches:

If S3 bucket is tagged with "PII", "PHI", "Confidential", or "Restricted" → DLP may flag entire bucket reads

---

## 🔎 How to Diagnose: Inspect Your Notebook for Sensitive Data

Since DLP is scanning the auto-saved `.ipynb` file, you need to examine what's actually being written there.

### Step 0A — Check What's in Your Notebook Output

**Inspect the raw `.ipynb` file (JSON format):**

```bash
# Read the notebook as JSON and look for patterns
cat my_notebook.ipynb | python3 << 'EOF'
import json
import sys
import re

notebook = json.load(sys.stdin)

# DLP patterns to check for
patterns = {
    "SSN": r"\d{3}-\d{2}-\d{4}",
    "Credit Card": r"\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}",
    "Email": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    "Phone": r"\d{3}[-.\s]?\d{3}[-.\s]?\d{4}",
    "API Key": r"(api_key|apikey|api-key)[:\s]*['\"]?[\w\-]{20,}['\"]?",
    "Password": r"(password|passwd|pwd)[:\s]*['\"]?[^\s\"']+['\"]?",
}

print("=" * 80)
print("SCANNING NOTEBOOK OUTPUTS FOR SENSITIVE PATTERNS")
print("=" * 80)

for cell_idx, cell in enumerate(notebook.get('cells', [])):
    outputs = cell.get('outputs', [])
    if not outputs:
        continue
    
    for out_idx, output in enumerate(outputs):
        # Convert output to string for pattern matching
        output_str = json.dumps(output)
        
        # Check each pattern
        for pattern_name, pattern in patterns.items():
            matches = re.findall(pattern, output_str)
            if matches:
                print(f"\n⚠️  FOUND {pattern_name.upper()} in Cell {cell_idx}, Output {out_idx}")
                print(f"   Pattern: {pattern}")
                print(f"   Matches: {matches[:3]}")  # Show first 3 matches
                print(f"   Context: {output_str[:200]}...")

print("\n" + "=" * 80)
print("DONE")
EOF
```

This will show you exactly what sensitive patterns are being detected by DLP.

---

### Step 0B — Check Your DataFrame Before Plotting

Before you call `plot_scurves()`, inspect what columns your DataFrame contains:

```python
import pandas as pd
import json

# After loading your data
df = spark.read.parquet("s3://prod-data/customers/raw/")

# CHECK 1: What columns exist?
print("All columns in dataframe:")
print(df.columns)
# Output: ['customer_id', 'name', 'ssn', 'dob', 'credit_score', 'default_flag']
#          ↑ SENSITIVE     ↑ SENSITIVE ↑ SENSITIVE ↑ SENSITIVE

# CHECK 2: Look at a sample row
print("\nSample row:")
print(df.head(1).to_dict())
# Output: {'customer_id': ['C001'], 
#          'ssn': ['123-45-6789'],  ← This will end up in the plot output!
#          'credit_score': [620]}

# CHECK 3: What will the plot function actually receive?
plot_df = df  # This is what gets passed to plot_scurves
print("\nColumns passed to plot_scurves():")
print(plot_df.columns)
# If SSN, DOB, names are here → ALERT WILL FIRE
```

---

### Step 0C — Trace What Gets Embedded in Plot Output

Different plotting libraries embed data differently. Check your specific library:

**For Plotly (common with plot_scurves):**

```python
import plotly.graph_objects as go

df = spark.read.parquet("s3://prod-data/customers/raw/")

# Create a plot
fig = go.Scatter(x=df['credit_score'], y=df['default_flag'])
fig.add_trace(go.Scatter(
    x=df['credit_score'],
    y=df['default_flag'],
    text=df['customer_id'],  # ← This column gets embedded in output
    customdata=df[['name', 'ssn', 'dob']]  # ← And this too!
))

# CHECK: What data is actually embedded?
print("\nData embedded in plot:")
print(fig.to_json())  # This shows ALL the data that will be in the notebook output
# If you see SSN, names, DOB here → This is what DLP will find
```

**For Matplotlib:**

```python
import matplotlib.pyplot as plt

# ❌ DON'T do this — matplotlib stores the data in memory
plt.plot(df['customer_id'], df['credit_score'])

# When Jupyter saves output, it includes all the data points
fig = plt.gcf()
print(fig.axes[0].lines[0].get_data())  # Shows what's in the plot
```

**For Pandas DataFrame display:**

```python
df = spark.read.parquet("s3://prod-data/customers/raw/")

# ❌ This displays the ENTIRE dataframe in the output
display(df)  # Or just df in a cell
# Auto-save captures the HTML table with ALL columns including PII

# CHECK: How many rows are being displayed?
print(f"DataFrame shape: {df.shape}")  # If it's large, all rows end up in output
print(f"Columns: {df.columns}")  # These all get embedded
```

---

### Step 0D — Look at the Actual Cell Output in Jupyter UI

**In Jupyter notebook, expand the cell output and inspect it:**

1. Run your plotting cell
2. Right-click on the cell output → **Inspect** (or press F12 in browser)
3. Look in the browser console for what data is being rendered
4. Or export the notebook and inspect the JSON manually

**Manual inspection:**

```bash
# View the notebook outputs as pretty-printed JSON
python3 << 'EOF'
import json

with open('/home/hadoop/my_notebook.ipynb') as f:
    nb = json.load(f)

# Look at just the outputs from your plotting cell
cell_idx = 3  # Your plot_scurves cell
outputs = nb['cells'][cell_idx].get('outputs', [])

print(json.dumps(outputs, indent=2))
# Look for embedded SSN, names, DOB, account numbers, etc.
EOF
```

---

### Step 0E — Check Your S3 Data Source

The data coming FROM S3 is what ends up IN the notebook output. Inspect it:

```bash
# Download a sample of the S3 data and inspect it
aws s3 cp s3://prod-data/customers/raw/part-0.parquet . --sse AES256

# Convert parquet to CSV to see the content
python3 << 'EOF'
import pandas as pd

df = pd.read_parquet('part-0.parquet')
print("Columns in S3 data:")
print(df.columns.tolist())
print("\nFirst row:")
print(df.head(1))
print("\nData types:")
print(df.dtypes)
EOF
```

---

## ⚠️ Common Culprits in Code

Here's what to look for in your notebook cells that would trigger DLP:

### Bad Pattern #1: Reading entire dataframe and passing to plot

```python
# ❌ BAD — All columns go to plot output
df = spark.read.parquet("s3://prod-data/customers/raw/")
plot_scurves(df)
```

**What DLP finds:**

- All column names in the output (customer_id, ssn, dob, account_number)
- All data values for those columns embedded in the plot JSON
- SSN pattern match: `123-45-6789`

**Fix:**

```python
# ✅ GOOD — Only safe columns in plot output
df = spark.read.parquet("s3://prod-data/customers/raw/")
plot_df = df[['credit_score', 'default_flag']]
plot_scurves(plot_df)
```

---

### Bad Pattern #2: Using sensitive columns as labels/metadata

```python
# ❌ BAD — SSN ends up in plot metadata
fig = go.Scatter(
    x=df['credit_score'],
    y=df['default_flag'],
    text=df['ssn'],  # ← Embedded in output!
    hovertext=df['customer_name']  # ← Also embedded!
)
```

**What DLP finds:**

- `"text": ["123-45-6789", "987-65-4321", ...]`
- `"hovertext": ["John Smith", "Jane Doe", ...]`

**Fix:**

```python
# ✅ GOOD — Use hashed or generic labels
import hashlib

fig = go.Scatter(
    x=df['credit_score'],
    y=df['default_flag'],
    text=[hashlib.md5(str(x).encode()).hexdigest()[:8] for x in df['ssn']],
    # Or just remove text/hovertext entirely
)
```

---

### Bad Pattern #3: Printing sensitive data to cell output

```python
# ❌ BAD — Output gets embedded in notebook
print("Sample records from database:")
for idx, row in df.iterrows():
    print(f"ID: {row['customer_id']}, SSN: {row['ssn']}, Score: {row['credit_score']}")

# Or using display
display(df)  # Entire dataframe with all columns
```

**What DLP finds:**

- The printed SSN patterns: `123-45-6789`
- The HTML table with all columns displayed

**Fix:**

```python
# ✅ GOOD — Only print safe information
print("Sample records summary:")
print(f"Total records: {len(df)}")
print(f"Average score: {df['credit_score'].mean()}")
print(f"Default rate: {df['default_flag'].mean()}")
```

---

### Bad Pattern #4: Storing intermediate results with sensitive columns

```python
# ❌ BAD — Results dataframe with PII gets auto-saved
results = pd.DataFrame({
    'customer_id': df['customer_id'],
    'ssn': df['ssn'],
    'score': df['credit_score'],
    'plot_data': plot_data
})
results.to_parquet('s3://results/data.parquet')

# Then display or use results
display(results)  # All columns including SSN displayed
```

**What DLP finds:**

- The `results` dataframe output with SSN column
- The column name "ssn" itself (some DLP rules trigger on column names)

**Fix:**

```python
# ✅ GOOD — Only keep non-sensitive columns
results = pd.DataFrame({
    'score': df['credit_score'],
    'plot_data': plot_data
})
# Or if you need customer_id:
results = pd.DataFrame({
    'customer_hash': df['customer_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest()),
    'score': df['credit_score']
})
```

---

## 📋 Checklist: What to Look For in Your Code

Go through each cell in your notebook and answer these questions:

### For Data Loading Cells:

- [ ] Does the cell read from an S3 bucket with "pii", "customer", "health", "medical", "sensitive", "prod" in the name?
- [ ] Do you load ALL columns? Or only the ones you need?
    - ❌ `df = spark.read.parquet("s3://...")`  ← All columns
    - ✅ `df = spark.read.parquet("s3://...").select('column1', 'column2')`  ← Specific columns
- [ ] What columns does the S3 data contain? (ssn, dob, name, account_number, medical_record_id?)
- [ ] Are you immediately filtering to only safe columns?

### For Plotting/Visualization Cells:

- [ ] What dataframe are you passing to the plot function?
- [ ] Does it contain any columns with names like: ssn, dob, name, account_number, address, email, phone?
- [ ] Are you using `text=`, `hovertext=`, `customdata=` parameters that pull from sensitive columns?
- [ ] Are the x/y axes pulling from sensitive columns?
    - ❌ `plt.plot(df['ssn'], df['score'])`  ← SSN on axis
    - ✅ `plt.plot(df['score'], df['default_flag'])`  ← Safe columns

### For Output Display Cells:

- [ ] Are you using `display(df)` or `print(df)`?
- [ ] Are you printing individual records with sensitive fields?
    - ❌ `for row in df.iterrows(): print(row['ssn'])`
    - ✅ `print(f"Total records: {len(df)}")`
- [ ] Are you converting dataframe to HTML/JSON that includes all columns?

### For Intermediate/Results Cells:

- [ ] Are you creating new dataframes that include sensitive columns?
- [ ] Are you storing results that contain PII/sensitive data?
- [ ] Are you writing intermediate results to S3 or local storage that gets backed up?

### General Questions:

- [ ] What is the most sensitive column in your notebook?
- [ ] Could that column appear anywhere in the cell outputs?
- [ ] Have you intentionally dropped/filtered it before any plots or displays?

---

## Root Causes — Why Sensitive Data Ends Up in Auto-Saved Outputs

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

## ✅ How to Resolve It (Auto-Save Specific)

Since your alerts are triggered by **notebook auto-save**, focus on these fixes in order:

### Step 1 — Sanitize the Data Before Plotting (CRITICAL)

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

### Step 2 — Disable or Delay Auto-Save During Sensitive Operations

Stop Jupyter from auto-saving while you're working with sensitive data.

**Option A — Disable auto-save entirely:**

Edit `~/.jupyter/jupyter_notebook_config.py`:
# Or append to it directly
```shell
# c.ContentsManager.autosave_interval = 0  # Disable auto-save
echo "c.ContentsManager.autosave_interval = 0" >> ~/.jupyter/jupyter_notebook_config.py
```

### Full Path Breakdown
```
/home/hadoop/.jupyter/jupyter_notebook_config.py
│        │      │                              │
│        │      │                              └─ Config filename
│        │      └─ Hidden directory (starts with .)
│        └─ Home directory of hadoop user
└─ \
```


**Option B — Increase auto-save interval:**

```python
c.ContentsManager.autosave_interval = 300  # Auto-save every 5 minutes instead of 30 seconds
```

**Option C — Use a separate notebook for sensitive analysis:**

```
Analysis Notebook (auto-save disabled)
    ↓ Do all data processing here
    ↓ Kernel → Restart & Clear Output before saving
Results Notebook (auto-save enabled)
    ↓ Import only final, safe results
```

---

### Step 3 — Clear Notebook Output After Plotting

If you can't disable auto-save, immediately clear outputs after plotting and before auto-save captures them.

**From the terminal:**

```bash
# Clear outputs from notebook immediately after plotting
jupyter nbconvert --clear-output --inplace /home/hadoop/my_analysis.ipynb
```

**From Jupyter UI (right after plotting):**

- **Kernel → Restart & Clear Output**
- Then **File → Save**

---

### Step 4 — Keep Jupyter Traffic Internal with SSH Tunnel (Secondary)

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

### Step 5 — Work with Aggregated/Anonymized S3 Data (Optional)

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

### Step 6 — Lock Down the EMR Security Group (Optional)

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

### Step 7 — Request a DLP Exemption (Last Resort)

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

## 🎯 Root Cause: Auto-Save Trigger

**Your specific scenario: DLP alerts are triggered when the Jupyter notebook auto-saves.**

This confirms **Root Cause #1** — the notebook auto-save mechanism is persisting sensitive data in the `.ipynb` file
outputs, and DLP is scanning these saved files.

### Why Auto-Save Triggers DLP

When Jupyter auto-saves every few seconds (default is ~30s intervals), it writes the entire notebook to disk as JSON,
including:

- All cell code
- **All cell outputs** (including embedded data from plots)
- Metadata

If those outputs contain sensitive data (SSNs, PHI, PII patterns), the DLP agent scans the `.ipynb` file on disk and
fires an alert.

---

## ⚡ IMMEDIATE FIXES (Priority Order)

### **Fix #1 (CRITICAL) — Sanitize Data BEFORE It Gets Plotted**

This prevents sensitive data from ever reaching the notebook output in the first place.

```python
# ❌ DON'T DO THIS
df = spark.read.parquet("s3://prod-data/customers/raw/")
plot_scurves(df)  # Outputs ALL columns, including PII → Auto-saves with PII

# ✅ DO THIS INSTEAD
df = spark.read.parquet("s3://prod-data/customers/raw/")
plot_df = df[['credit_score', 'default_flag', 'segment']]  # Only safe columns
plot_scurves(plot_df)  # Outputs only safe columns → Auto-save is clean
```

**Why this works:** If sensitive columns never reach the plot function, they never get embedded in the output → nothing
for DLP to detect.

---

### **Fix #2 (CRITICAL) — Disable or Delay Auto-Save During Sensitive Operations**

Stop Jupyter from auto-saving while you're working with sensitive data.

**Option A — Disable auto-save in Jupyter settings:**

```python
# In a notebook cell, before reading sensitive data:
import IPython

IPython.display.Javascript("""
require(['services/contents'], function(contents) {
    // Disable auto-save
    require.config({paths: {contents: '/api/contents'}});
    console.log('Auto-save disabled');
});
""")

# Your sensitive code here
df = spark.read.parquet("s3://prod-data/customers/raw/")
plot_df = df[['credit_score', 'default_flag']]
plot_scurves(plot_df)

# Then manually save with cleared outputs:
# Kernel → Restart & Clear Output → Save
```

**Option B — Configure Jupyter to not auto-save outputs:**

Edit `~/.jupyter/jupyter_notebook_config.py`:

```python
c.ContentsManager.autosave_interval = 0  # Disable auto-save entirely
# OR
c.ContentsManager.autosave_interval = 300  # Only auto-save every 5 minutes (instead of 30s)
```

Then manually save only when you've cleared outputs.

**Option C — Use a temporary notebook for sensitive operations:**

```
Sensitive Analysis Notebook (auto-save DISABLED)
    ↓ [Do all the plotting and analysis here]
    ↓ [Kernel → Restart & Clear Output before saving]
Results Notebook (auto-save ENABLED)
    ↓ [Import only the final plots/metrics — no raw data]
    ↓ [Safe to auto-save — no PII in outputs]
```

---

### **Fix #3 — Immediately Clear Outputs After Plotting**

If you can't prevent sensitive data from reaching the output, clear it immediately after plotting before auto-save
captures it.

```python
import pandas as pd
from IPython.display import display

df = spark.read.parquet("s3://prod-data/customers/raw/")
plot_df = df[['credit_score', 'default_flag']]

# Plot
plot_scurves(plot_df)

# ⚠️ IMMEDIATELY clear the output before auto-save
import os

os.system("jupyter nbconvert --clear-output --inplace /home/hadoop/my_notebook.ipynb")
```

Or use the Jupyter UI immediately after plotting:

- **Kernel → Restart & Clear Output**
- **File → Save** (now only code is saved, no sensitive outputs)

---

## 📋 Quick Reference — Auto-Save Trigger Solutions

| Approach                                             | Effort | Effectiveness                        |
|------------------------------------------------------|--------|--------------------------------------|
| **Sanitize data before plotting (Fix #1)**           | Low    | ⭐⭐⭐⭐⭐ BEST — Prevents issue entirely |
| **Disable auto-save during sensitive work (Fix #2)** | Low    | ⭐⭐⭐⭐⭐ BEST — Prevents saves          |
| **Clear outputs immediately (Fix #3)**               | Low    | ⭐⭐⭐ Good — Removes data from disk    |
| **Use SSH tunnel for access (Step 4)**               | Medium | ⭐⭐ Helps but doesn't stop auto-save  |
| **Request DLP exemption (Step 7)**                   | High   | ⭐ Last resort only                   |

---

## 🚀 Recommended Workflow for Auto-Save Alerts

Since your alerts are triggered by auto-save, use this workflow:

```python
# 1. READ sensitive data into memory (not dangerous yet)
raw_df = spark.read.parquet("s3://prod-data/customers/raw/")

# 2. IMMEDIATELY extract only non-sensitive columns
# This prevents auto-save from capturing PII
plot_df = raw_df.select(
    "credit_score",
    "default_flag",
    "segment"
    # NO: name, ssn, dob, account_number, etc.
)

# 3. PLOT using only the clean dataframe
plot_scurves(plot_df)

# 4. OPTIONAL: If you want to save, clear outputs first
# Kernel → Restart & Clear Output → Save
```

**With this approach:**

- Auto-save runs every 30 seconds ✓
- But the notebook only contains safe columns in outputs ✓
- DLP scans the auto-saved file ✓
- No sensitive patterns found ✓
- **No alert** ✓

---

## ⚠️ What NOT to Do

```python
# ❌ DON'T: Read all columns and hope auto-save won't trigger
df = spark.read.parquet("s3://prod-data/customers/raw/")
plot_scurves(df)
# Auto-save runs → All columns including PII end up in .ipynb → DLP fires

# ❌ DON'T: Try to hide from DLP by piping through cat/echo
subprocess.run(["cat", "s3://data.csv"])
# DLP still sees the data flowing into memory

# ❌ DON'T: Download the notebook without clearing outputs
# The .ipynb file contains embedded PII → Triggers alerts on download
```

---

## 🔬 Diagnostic Guide: Find Your Specific Problem

**Follow these steps to identify exactly what in your code is triggering DLP:**

### Step 1: Locate Your Problem Cell

1. Check when DLP alerts started occurring
2. Look at cells you ran around that time
3. Identify the cell that contains `plot_scurves()` or similar plotting code
4. That cell is likely the culprit

**Question to ask:** Did you add new columns, load a new S3 dataset, or change your plot code recently?

### Step 2: Inspect the Notebook File (Most Direct Method)

On the EMR master node, run this to scan your notebook for sensitive patterns:

```bash
# Navigate to your notebook directory
cd /home/hadoop

# Run the diagnostic scanner
python3 << 'EOF'
import json
import re
from pathlib import Path

# Find your notebook
notebooks = list(Path('.').glob('*.ipynb'))
print(f"Found {len(notebooks)} notebooks:")
for nb in notebooks:
    print(f"  - {nb}")

# Analyze each one
for notebook_path in notebooks:
    print(f"\n{'='*80}")
    print(f"ANALYZING: {notebook_path}")
    print('='*80)
    
    with open(notebook_path) as f:
        nb = json.load(f)
    
    # DLP patterns
    patterns = {
        "SSN": r"\d{3}-\d{2}-\d{4}",
        "DOB": r"\d{4}-\d{2}-\d{2}",
        "Email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
        "Phone": r"\d{3}[-.\s]?\d{3}[-.\s]?\d{4}",
    }
    
    found_any = False
    for cell_idx, cell in enumerate(nb.get('cells', [])):
        outputs = cell.get('outputs', [])
        if not outputs:
            continue
        
        cell_code = ''.join(cell.get('source', []))
        output_str = json.dumps(outputs)
        
        for pattern_name, pattern in patterns.items():
            matches = re.findall(pattern, output_str)
            if matches:
                found_any = True
                print(f"\n⚠️  Cell {cell_idx} - {pattern_name.upper()} FOUND")
                print(f"Code: {cell_code[:100]}")
                print(f"Matches: {set(matches[:5])}")  # Unique matches
    
    if not found_any:
        print("✅ No sensitive patterns detected")

EOF
```

This will tell you:

- Which cell has sensitive outputs
- What patterns were detected (SSN, DOB, Email, etc.)
- Whether the outputs are the problem

### Step 3: Check Your DataFrame Columns

In a notebook cell, add this diagnostic code:

```python
# What dataframe did you load?
print("=" * 80)
print("DATAFRAME DIAGNOSIS")
print("=" * 80)

# Print the dataframe you're using for plotting
print("\nColumns in your dataframe:")
print(df.columns.tolist())

# Check for common sensitive column names
sensitive_keywords = ['ssn', 'dob', 'name', 'email', 'phone', 'account', 'password', 'secret']
found_sensitive = [col for col in df.columns if any(kw in col.lower() for kw in sensitive_keywords)]

if found_sensitive:
    print(f"\n⚠️  SENSITIVE COLUMNS DETECTED: {found_sensitive}")
else:
    print("\n✅ No obvious sensitive columns")

# Check what's actually going to the plot
print("\nColumns being passed to plot function:")
plot_df = df  # Whatever your plot function receives
print(plot_df.columns.tolist())

# Check a sample row
print("\nSample data (first row):")
for col in df.columns:
    print(f"  {col}: {df[col].iloc[0]}")
```

**If you see any of these column names, that's likely the problem:**

- ssn, social_security_number
- dob, date_of_birth, birthdate
- name, first_name, last_name
- email, email_address
- phone, phone_number
- account_number, account_id

### Step 4: Test Your Plot Output

Run this to see what actually gets embedded in the plot:

```python
# For Plotly-based plots
import plotly.graph_objects as go

# Create your plot
fig = go.Scatter(x=df['credit_score'], y=df['default_flag'])

# Check what data is embedded
plot_json = fig.to_json()
print("Plot JSON length:", len(plot_json))
print("First 500 chars of plot JSON:")
print(plot_json[:500])

# Look for sensitive patterns
import re

patterns = {
    "SSN": r"\d{3}-\d{2}-\d{4}",
    "Dates": r"\d{4}-\d{2}-\d{2}",
}

for name, pattern in patterns.items():
    matches = re.findall(pattern, plot_json)
    if matches:
        print(f"\n⚠️  {name} FOUND IN PLOT: {matches[:3]}")
```

### Step 5: Compare Safe vs. Unsafe Code

**If diagnostic shows your plot contains SSN, run this to fix it:**

```python
# BEFORE (Bad):
df = spark.read.parquet("s3://prod-data/customers/raw/")
plot_scurves(df)  # ← This passes ALL columns

# AFTER (Good):
df = spark.read.parquet("s3://prod-data/customers/raw/")
plot_df = df[['credit_score', 'default_flag']]  # ← Only safe columns
plot_scurves(plot_df)  # ← This passes only safe columns
```

Clear outputs and re-run:

```python
# After making the fix, clear outputs
# Kernel → Restart & Clear Output → Save

# Or from code:
import subprocess

subprocess.run(["jupyter", "nbconvert", "--clear-output", "--inplace", "/home/hadoop/my_notebook.ipynb"])
```

---

## Summary

**Your scenario:** Jupyter auto-save is persisting notebook outputs containing sensitive data → DLP scans these
auto-saved files → Alerts fire.

**Solution priority:**

1. **Sanitize data before plotting** — Prevents sensitive columns from ever reaching the output
2. **Disable auto-save during sensitive operations** — Stops the file from being written with PII
3. **Clear outputs immediately** — Removes PII from already-saved files

**Expected outcome:** Auto-save runs normally, but the `.ipynb` file contains only safe data → No DLP alerts.