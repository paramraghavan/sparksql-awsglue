# Master Data Management for Identity and Access Management (IAM)

## What an IAM/MDM Data Engineer Does

An IAM/MDM Data Engineer builds and maintains the platform that creates a **single, trusted source of truth** for employee identities and access rights. Rather than focusing on transactional data (orders, invoices, loans), the role centers on ensuring that enterprise identity data is accurate, consistent, and available across all systems.

A typical job description might mention responsibilities such as:

> Designing and implementing scalable, cloud-native identity architectures across AWS, building data pipelines from identity ingestion to provisioning, and exposing identity data through APIs for access management, compliance, and security.

In practice, this means designing AWS-based identity platforms using services such as Amazon S3, Glue, Lambda, RDS, API Gateway, Kafka, EventBridge, and AWS IAM.

### IAM Has 3 Interconnected Components

Before we dive deeper, understand that **IAM consists of 3 layers:**

```
LAYER 1: MASTER DATA (Static Definitions - S3/RDS)
├─ Who: Employee Master, Contractor Master
├─ What: Role Master, Permission Master, System Master
├─ How: Access Rights Master, Approval Authority Master
├─ Rules: SOD Rules, Compliance Rules, Security Policies
└─ Audit: System Access History Master

LAYER 2: REQUEST WORKFLOW (Dynamic Process - ServiceNow)
├─ Employee requests access (submits ticket)
├─ System queries Layer 1 (master data)
├─ Manager approves (workflow)
├─ Admin provisions (Glue jobs, Lambda)
├─ Update Layer 1 (add access grant)
└─ Distribute credentials (Keychain)

LAYER 3: IDENTITY SYSTEMS (Enforcement - Live)
├─ Okta (SSO for cloud apps)
├─ Active Directory (on-prem auth)
├─ AWS IAM (cloud access control)
├─ Keychain (password vault)
└─ All use Layer 1 (master data) to authenticate/authorize
```

**What we're building:** Layer 1 (Master Data) + Pipeline to feed Layer 2 & 3

**ServiceNow & Keychain:** Part of IAM, but NOT master data. They're processes and systems that USE master data.

---

### Typical Data Flow

``` text
LAYER 1: MASTER DATA INGESTION & STORAGE

SAP HR      Active Dir      AWS IAM      Okta      ServiceNow
 |             |               |           |            |
 +-------------+---------------+-----------+------------+
               |
          Data Ingestion
       (Glue / Lambda / API)
               |
          Data Quality Rules
               |
      Matching & Deduplication
               |
   MDM Golden Identity Record
   (Employee, Role, Permission, Access Rights, etc.)
               |
    +----------+-----------+------+
    |          |           |      |
 APIs        Kafka      Lambda   Batch
    |          |           |      |
                     ↓
LAYER 2: REQUEST WORKFLOW (ServiceNow)

Employee requests access → ServiceNow queries master data
         ↓
Manager approves → Updates master data (Access Rights Master)
         ↓
Admin provisions → Lambda/Glue creates access
         ↓
Distribute credentials → Keychain sends passwords
         ↓
LAYER 3: IDENTITY SYSTEMS (Enforcement)

Okta, AD, AWS IAM use master data to authenticate/authorize
```

A typical IAM pipeline might:

1. Ingest employee data from HR system (SAP).
2. Validate data quality (email format, required fields).
3. Standardize employee names and email addresses.
4. Match employees across multiple systems to find duplicates.
5. Create or update the golden identity record.
6. Provision access in AWS IAM, Okta, Active Directory automatically.
7. Publish identity changes to downstream systems via APIs and Kafka.

Example (PySpark):

``` python
# Read from SAP HR
hr_data = spark.read.csv("s3://landing/employees")

# Clean and standardize
clean = (
    hr_data
      .withColumn("email", lower(col("email")))
      .dropDuplicates(["employee_id"])
      .filter(col("employee_id").isNotNull())
)

# Write golden record
clean.write.mode("overwrite").saveAsTable("identity_master")
```

------------------------------------------------------------------------

# What is Identity and Access Management (IAM)?

Organizations often have multiple systems storing different versions of the same employee.

Example:

| System | Employee Name | Department | Email | Status |
|--------|---------------|-----------|--------|--------|
| SAP HR | John Martinez | Underwriting | john.martinez@company.com | Active |
| Active Directory | jmartinez | Underwriting | jmartinez@company.com | Enabled |
| AWS IAM | john.martinez | Underwriting | N/A | Active |
| Okta | john_martinez | Operations | john.m@company.com | ACTIVE |
| ServiceNow | Martinez, John | Underwriting | jmartinez@company.com | Active |

Without IAM/MDM, every system maintains its own version, resulting in:
- Inconsistent access levels across systems
- Slow onboarding (5-7 days manual work)
- Incomplete deprovisioning (security risk)
- No audit trail for compliance
- Manual errors and conflicts

The IAM/MDM platform creates a **Golden Identity Record**.

| Attribute | Value |
|-----------|-------|
| Global Employee ID | E-2024-98765 |
| Legal Name | John Michael Martinez |
| Email | john.martinez@company.com |
| Department | Underwriting |
| Manager | Sarah Chen |
| MFA Required | Yes |
| Security Clearance | Level 3 |
| Last Provisioned | 2024-07-15 |
| Status | Active |

Every system (HR, Active Directory, AWS, Okta, ServiceNow) references this single authoritative record. When John's department changes, the update happens once in the golden record, and all systems automatically reflect the change.

------------------------------------------------------------------------

# Master Data vs Processes vs Systems (Important Distinction)

**Master Data** (what we're building):
- Static definitions stored in database
- Employee Master, Role Master, Permission Master, etc.
- Updated daily/weekly via Glue pipelines
- Managed by: Data Engineers

**Request Workflow** (ServiceNow):
- Dynamic business process for requesting access
- Queries master data to make decisions
- Updates master data when access is approved
- Managed by: IT Operations

**Identity Systems** (Okta, AD, AWS, Keychain):
- Authentication and credential management
- Use master data to enforce access
- Managed by: System Administrators

**All 3 are part of IAM, but only Master Data is what we're building with MDM.**

Example workflow:
```
John requests S3 access in ServiceNow
        ↓
ServiceNow queries Master Data
- Is John's role allowed S3 access? (Role Master)
- Who approves? (Approval Authority Master)
- Any conflicts? (SOD Rules Master)
        ↓
Manager approves in ServiceNow
        ↓
System updates Master Data
- Add new Access Rights record for John
- Update System Access History
        ↓
Keychain stores and distributes credentials
        ↓
AWS IAM checks Master Data
- Is John supposed to have S3 access? (Access Rights Master)
- Has it expired? (Access Rights Master)
- Grant/Deny access
```

------------------------------------------------------------------------

# How Systems Consume Identity Data

Downstream systems don't manage identity master data themselves. Instead, they consume it using common integration patterns.

## 1. Real-Time APIs

Applications query the identity platform directly for current employee information.

Example:

``` http
GET /api/identities/john.martinez@company.com
```

Response:

``` json
{
  "global_emp_id": "E-2024-98765",
  "name": "John Martinez",
  "email": "john.martinez@company.com",
  "department": "Underwriting",
  "manager": "Sarah Chen",
  "mfa_required": true,
  "access_systems": ["AWS", "Okta", "ServiceNow"],
  "status": "Active"
}
```

This ensures every application sees the current, authoritative identity data.

------------------------------------------------------------------------

## 2. Automated Provisioning

When a new employee is hired, the IAM system automatically creates accounts across all systems.

``` text
New Hire Event (HR)
         |
    Golden Record Created
         |
    Access Policies Applied
         |
    ┌────┬────┬────┬──────┐
    ↓    ↓    ↓    ↓      ↓
   AWS  Active  Okta ServiceNow  Email
   IAM  Dir            Account
```

Example: When Alice is hired as an underwriter:
- AWS IAM account created with `alice.smith@company.com`
- Active Directory account enabled
- Okta user provisioned to Salesforce
- ServiceNow account created
- Credentials sent to employee
- All in ~30 minutes (automated)

------------------------------------------------------------------------

## 3. Event Streaming

Whenever identity changes, the system publishes events through Kafka for real-time notification.

``` text
Identity Change Event
(Department Update)
         |
    Kafka Topic: identity-events
      /     |      \
   AWS   Okta   Compliance
  IAM    SSO     System
   ↓      ↓        ↓
Update Update    Log &
Access Groups   Alert
```

Each subscriber updates automatically without polling.

------------------------------------------------------------------------

## 4. Batch Exports

Legacy systems that cannot consume APIs receive nightly CSV exports.

``` text
Golden Identity Record
         |
    Export CSV
         |
    S3 / FTP
         |
  Legacy ERP Import
```

------------------------------------------------------------------------

# Typical Identity Data Managed by IAM/MDM

**Note:** The following master data tables include:
- **CORE IAM** (14 tables) - Strictly identity & access management
- **Supporting Systems** (4 tables) - Related but broader scope (HR, IT Ops, Finance)

**Recommendation:** Start with Core IAM tables, add supporting tables based on business needs.

---

## CORE IAM MASTER TABLES (Start Here)

## 1. Employee Master ⭐ CORE IAM

- Employee ID (globally unique)
- Name (legal, preferred)
- Email (primary corporate email)
- Phone and Mobile
- Department
- Job Title and Level
- Manager (reporting relationship)
- Cost Center
- Hire Date and Termination Date
- Employment Status (Active, OnLeave, Terminated)
- Location
- Background Check Status
- Security Clearance Level

Used by HR Systems, AWS, Okta, Active Directory, ServiceNow, Compliance.

------------------------------------------------------------------------

## 2. Contractor/Vendor/Third-Party User Master ⭐ CORE IAM

- Contractor ID
- Full Legal Name
- Organization/Company
- Contract Start and End Date
- Email (corporate or external)
- Department/Area Assigned
- Sponsoring Manager
- Type (Contractor, Vendor, Partner, Consultant)
- Access Approval Status
- Systems Authorized to Access
- NDA Status
- Insurance/Compliance Requirements

Used by HR, Vendor Management, Security, Compliance.

------------------------------------------------------------------------

## 3. System/Application Master ⭐ CORE IAM

- System ID (unique identifier)
- System Name (AWS, Salesforce, ServiceNow, etc.)
- System Type (Cloud, On-Premise, Hybrid)
- Environment (Production, Staging, Dev)
- Owner/Admin
- Integration Method (API, LDAP, SCIM, JDBC)
- Criticality Level (Critical, High, Medium, Low)
- Data Classification (PII, Confidential, Internal, Public)
- Authentication Method (SAML, OAuth, AD, API Key)
- Compliance Requirements (SOX, HIPAA, GLBA, etc.)

Used by Security, Infrastructure, Compliance, Access Management.

------------------------------------------------------------------------

## 4. Role and Permission Master ⭐ CORE IAM

- Role ID (standard role identifier)
- Role Name (Underwriter, Manager, Admin, Analyst)
- Role Description
- Associated System Permissions (per system)
- Approval Required (Yes/No)
- Approval Authority (Who can approve)
- Access Expiry Duration (90 days, annual, etc.)
- Compliance Requirements
- Conflicting Roles (Segregation of Duties)
- Cost Center Eligible
- Department Eligible

Used by Security, Access Management, HR, Compliance.

------------------------------------------------------------------------

## 5. Group Master ⭐ CORE IAM

- Group ID
- Group Name (Department-Users, Finance-Team, AWS-Admins)
- Group Type (Security, Distribution, Team)
- Owner
- System Where Group Exists (AD, Okta, AWS, etc.)
- Members (employee IDs)
- Approval Required
- Purpose/Description
- Creation Date
- Expiry Date (if temporary)
- Compliance Classification

Used by Security, Active Directory, Okta, Email Systems, Compliance.

------------------------------------------------------------------------

---

## SUPPORTING MDM/OPERATIONAL MASTER TABLES (Add as Needed)

## 6. Department/Cost Center/Location Master ⚠️ SUPPORTING MDM

- Department ID
- Department Name
- Cost Center Code
- Cost Center Name
- Department Head
- Location (city, country, region)
- Business Unit
- Reporting Structure (parent department)
- Employee Count
- Compliance Zone (GDPR, CCPA, etc.)
- Budget Allocation

Used by Finance, HR, Reporting, Compliance.

------------------------------------------------------------------------

## 7. Device Master ⚠️ SUPPORTING OPS

- Device ID / Asset Tag
- Device Type (Laptop, Mobile, Desktop, Monitor)
- Device Model
- Serial Number
- Owner (employee ID)
- Purchase Date
- End-of-Life Date
- Status (In-Use, Stolen, Decommissioned)
- Assigned Location
- MDM Status (Enrolled, Non-Compliant, Blocked)
- Encryption Status
- Security Updates Status

Used by IT Asset Management, Security, Compliance, Mobile Device Management.

------------------------------------------------------------------------

## 8. Access Right Master

- Access ID (unique identifier)
- Employee ID
- System Name
- Resource/Permission (S3 Bucket, Database, Application)
- Access Level (Read-Only, Read-Write, Admin)
- Approval Authority
- Approval Date
- Expiry Date
- Business Justification
- Segregation of Duties Check (Pass/Fail)
- Compliance Classification
- Last Review Date

Used by Security, Compliance, Audit, Access Management.

------------------------------------------------------------------------

## 9. Security Policy Master

- Policy ID
- Policy Name (MFA Policy, Password Policy, SOD Policy)
- Policy Type (Access, Authentication, Compliance)
- Affected Systems
- Policy Rules (e.g., "MFA Required for Admin Access")
- Affected Roles/Departments
- Enforcement Status (Active, Inactive)
- Compliance Requirements
- Creation Date
- Review Frequency
- Owner/Author

Used by Security, Compliance, IT Operations.

------------------------------------------------------------------------

## 10. Approval Authority Master

- Authority ID
- Employee ID (person who can approve)
- Authority Level (1-5, or by dollar amount)
- Approver Title
- Can Approve (New Access, Renewal, Termination)
- Systems/Resources They Can Approve
- Approval Limit (number of approvals per day/month)
- Delegate Authority (can assign to another person)
- Authority Expiry Date

Used by Security, Compliance, Access Management.

------------------------------------------------------------------------

## 11. Entitlement Master

- Entitlement ID
- Entitlement Name (Cloud-Developer-Access, Loan-Processor, etc.)
- Description
- Associated Roles
- Associated Systems
- Associated Permissions
- Compliance Classification
- Owner
- Review Frequency

Used by Security, Access Management, Compliance.

------------------------------------------------------------------------

## 12. Audit/Compliance Rule Master

- Rule ID
- Rule Name (SOD Rule, Access Review Rule, etc.)
- Rule Type (Segregation of Duties, Expiry, Compliance)
- Rule Logic (IF this role THEN cannot have that role)
- Affected Roles/Systems
- Violation Severity (Critical, High, Medium)
- Auto-Remediate (Yes/No)
- Enforcement Status
- Compliance Standard (SOX, HIPAA, GLBA)
- Owner

Used by Compliance, Security, Audit, Risk Management.

------------------------------------------------------------------------

## 13. Organizational Unit (OU) Master

- OU ID
- OU Name (Finance, Engineering, Sales, etc.)
- OU Type (Department, Team, Function)
- Parent OU
- Manager
- Location
- Compliance Zone
- Active Directory OU Path
- Default Roles for New Users
- Access Policies

Used by Active Directory, HR, Security.

------------------------------------------------------------------------

## 14. External Access Master

- External Access ID
- Partner/Vendor Organization
- Employee ID (sponsoring manager)
- Access Type (Temporary, Permanent, Emergency)
- Systems Granted Access
- Data Access Allowed (Yes/No, what data)
- NDA/Contract Reference
- Start Date and End Date
- Approval Status
- Last Activity Date
- Compliance Checklist

Used by Security, Compliance, Risk Management.

------------------------------------------------------------------------

## 15. Emergency Access Master

- Emergency Account ID
- Account Type (Break-Glass, Super-Admin)
- Purpose
- Associated Systems
- Credentials (encrypted, audited)
- Who Can Access (list of people)
- Access Log (all uses logged)
- Approval Required (Yes/No)
- Annual Certification Required (Yes/No)
- Last Used Date

Used by Security, IT Operations, Incident Response.

------------------------------------------------------------------------

## 16. Training & Certification Master

- Record ID
- Employee ID
- Training Type (Security Awareness, Data Privacy, Compliance)
- Training Name
- Completion Date
- Expiry Date
- Certification Status
- Score/Grade
- Required for Role (Yes/No)
- Compliance Link

Used by Compliance, HR, Training Teams.

------------------------------------------------------------------------

## 17. Segregation of Duties (SOD) Rules Master

- Rule ID
- Conflicting Role 1
- Conflicting Role 2
- Systems Affected
- Conflict Type (Cannot perform both)
- Business Justification
- Compliance Standard (SOX, Internal Policy)
- Owner
- Enforcement Level (Prevent, Alert, Review)

Used by Compliance, Security, Access Management, Audit.

------------------------------------------------------------------------

## 18. System Access History Master

- History ID
- Employee ID
- System
- Access Granted Date
- Access Revoked Date
- Permission Level
- Approver
- Reason for Grant
- Reason for Revocation
- Status (Active, Revoked, Expired)
- Compliance Review Status

Used by Audit, Compliance, Security, Forensics.

------------------------------------------------------------------------

# How All Master Data Tables Work Together

``` text
┌─────────────────────────────────────────────────────────────────┐
│              EMPLOYEE/IDENTITY MASTER (Core)                    │
├─────────────────────────────────────────────────────────────────┤
│ Employee ID, Name, Email, Department, Manager, Status           │
│                         │                                        │
├─────────────────────────┼────────────────────────────────────────┤
│                         ↓                                        │
│  ┌──────────────────────────────────────────────────────┐       │
│  │ Who is this person? What are they allowed to do?    │       │
│  │                                                      │       │
│  │ Department Master ─→ Role & Permission Master       │       │
│  │         ↓                    ↓                       │       │
│  │ Org Structure      What systems?                    │       │
│  │         ↓                    ↓                       │       │
│  │ Location Master     System/Application Master       │       │
│  │         ↓                    ↓                       │       │
│  │ Cost Center        Access Rights Master             │       │
│  └──────────────────────────────────────────────────────┘       │
│                         │                                        │
├─────────────────────────┼────────────────────────────────────────┤
│                         ↓                                        │
│  ┌──────────────────────────────────────────────────────┐       │
│  │ How to provision? What to check?                    │       │
│  │                                                      │       │
│  │ Group Master ──────→ Security Policy Master         │       │
│  │ Entitlement Master ─→ Approval Authority Master     │       │
│  │ Device Master ──────→ SOD Rules Master              │       │
│  │ Audit Rules Master ─→ Compliance Rules Master       │       │
│  └──────────────────────────────────────────────────────┘       │
│                         │                                        │
├─────────────────────────┼────────────────────────────────────────┤
│                         ↓                                        │
│  ┌──────────────────────────────────────────────────────┐       │
│  │ Special Cases & Compliance                         │       │
│  │                                                      │       │
│  │ Contractor Master ──→ External Access Master        │       │
│  │ Emergency Access Master                             │       │
│  │ Training & Certification Master                     │       │
│  │ System Access History Master                        │       │
│  └──────────────────────────────────────────────────────┘       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Example: When John Martinez requests access to AWS S3 customer data**

1. **Identify**: Employee Master → John is in Underwriting department
2. **Determine Role**: Role & Permission Master → Underwriter role allows S3 access
3. **Check System**: System/Application Master → S3 requires AWS IAM account
4. **Find Approver**: Approval Authority Master → Department manager Sarah can approve
5. **Check Conflicts**: SOD Rules Master → Underwriter can read, but not approve loans
6. **Check Compliance**: Audit Rules Master → GLBA requires quarterly review
7. **Create Access**: Access Rights Master → Create entry (approved, 90-day expiry)
8. **Track History**: System Access History Master → Log the grant for audit
9. **Provision**: AWS IAM → Create access (triggered by access rights update)
10. **Alert**: Email → Notify John of access, set reminder for expiry

------------------------------------------------------------------------

# Master Data Summary Table

| Master Table | Purpose | Key Relationship |
|--------------|---------|-------------------|
| Employee Master | WHO (identity) | Core - all others reference this |
| Contractor Master | WHO (external) | Alternate to Employee Master |
| Department Master | WHERE (org structure) | Organizes employees |
| Role Master | WHAT (permissions) | Defines what can be accessed |
| System Master | WHERE (applications) | What systems exist |
| Access Rights | GRANT (permission) | Ties employee to system role |
| Group Master | GROUP (teams) | Collections of employees |
| Approval Authority | WHO APPROVES | Who can approve access |
| Device Master | WHAT DEVICE | Hardware assigned |
| Security Policy | HOW (rules) | Rules for access |
| SOD Rules | PREVENT (conflicts) | Segregation of duties |
| Entitlement Master | BUNDLE (grouped rights) | Pre-packaged access |
| Audit Rules | COMPLIANCE (checks) | Regulatory requirements |
| External Access | EXTERNAL PARTY | Non-employees |
| Emergency Access | BREAK-GLASS | Emergency accounts |
| Training Master | PREREQUISITE | Required before access |
| Access History | AUDIT TRAIL | Who had what, when |

------------------------------------------------------------------------

# What IAM/MDM Does NOT Store

IAM/MDM is **not** designed for:

- Loan applications (transactional)
- Customer transactions (transactional)
- Orders and invoices (transactional)
- Audit logs of system activities (operational)
- Sensor or application events (operational)

Those remain in operational systems. IAM/MDM manages the relatively stable definitions of employees, departments, and access rights.

------------------------------------------------------------------------

# Running Use Case: Global Financial Services Company

To better understand IAM/MDM concepts, let's use a **Global Financial Services Company** as a running example.

This company has:
- 50,000+ employees across 100+ locations
- Multiple business units (Lending, Risk, Operations, Compliance)
- Highly regulated (must comply with SOX, GLBA, Dodd-Frank)
- Multiple legacy and cloud systems
- 100+ applications requiring access management

**Current Problem (Before IAM/MDM):**
- New employee onboarding: 5-7 days (manual work)
- Access decisions scattered across emails and tickets
- Deprovisioning incomplete when employees leave (security risk)
- Can't quickly answer "Who has access to what?"
- Compliance violations in audits due to incomplete audit trail

**With IAM/MDM Platform:**
- Onboarding: 30 minutes (automated)
- Access decisions centralized and documented
- Automatic deprovisioning within 1 hour of termination
- Real-time dashboard showing all access
- Complete audit trail for compliance

### Example Workflow: New Loan Underwriter

1. **HR hires Sarah Chen as Senior Underwriting Analyst**
   - Event: "NewHire" published to identity system

2. **Golden Identity Record Created**
   - Employee ID: E-2024-99999
   - Department: Underwriting
   - Job Level: IC-5
   - Required Systems: AWS, Salesforce, ServiceNow, Okta

3. **Access Policies Applied**
   - Loan approval authority: Up to $500K
   - Segregation of Duty: Cannot both create and approve loans
   - MFA Required: Yes
   - Data Access: Customer credit files, loan portfolio

4. **Automatic Provisioning**
   - AWS: Create IAM user `sarah.chen`, assign `Underwriter` role
   - Okta: Create SSO account, provision Salesforce
   - ServiceNow: Create ITSM account, assign Underwriting queue
   - Active Directory: Enable account, add groups
   - Email: Send credentials

5. **Audit Trail Created**
   - Who: Sarah Chen
   - What: Created and provisioned to 4 systems
   - When: 2024-07-15 09:30:00
   - Why: New hire - Underwriting Analyst
   - Who approved: HR Manager + Compliance

6. **Monitoring & Compliance**
   - Dashboard shows Sarah has access to required systems
   - Compliance report generated for audit trail
   - Quarterly access review scheduled

### Example: Employee Termination

When Sarah leaves the company:

1. **HR marks as Terminated**
2. **Deprovisioning triggered automatically**
   - AWS: Disable IAM user (preserve for audit)
   - Active Directory: Disable account within 4 hours
   - Okta: Revoke sessions, deactivate user
   - ServiceNow: Archive account
   - Kafka events published to alert systems

3. **Complete within 1 hour**
   - All access revoked
   - Audit trail preserved
   - Equipment tracking updated
   - Compliance report generated

------------------------------------------------------------------------

# What MDM for Identity/IAM Solves

## Before IAM/MDM Platform

```
Manual Provisioning (5-7 days):
1. HR submits new hire
2. Ticket created in ServiceNow
3. IT analyst reviews requirements
4. Analyst manually creates AD account
5. Analyst creates AWS IAM user
6. Analyst provisions Okta
7. Analyst sets permissions
8. Manager reviews access (often incomplete)
9. Employee notified with credentials

Result: Days of delay, 15% errors, no compliance trail
Cost: $500 per employee, 2 FTE managing manually
```

## After IAM/MDM Platform

```
Automated Provisioning (30 minutes):
1. HR posts new hire via API
2. Lambda triggered automatically
3. Golden identity record created
4. Access policies applied
5. All accounts created in parallel:
   ├─ AWS IAM user
   ├─ Active Directory account
   ├─ Okta user
   ├─ ServiceNow account
   └─ Compliance tracking
6. Credentials sent
7. Audit trail generated

Result: 30 minutes, 0% errors, full compliance
Cost: $5 per employee, 0.2 FTE managing
```

------------------------------------------------------------------------

# Benefits of IAM/MDM

## Operational Efficiency
- 80% reduction in manual provisioning work
- Faster onboarding (30 min vs 7 days)
- Fewer identity-related support tickets
- Reduced cost per employee

## Security
- Immediate deprovisioning (1 hour vs days)
- Centralized access control
- Segregation of duties enforcement
- Consistent MFA enforcement

## Compliance
- Complete audit trail (7-year retention)
- Access approval documentation
- Change tracking (who, what, when, why)
- Automated compliance reports
- Evidence for regulatory audits (SOX, GLBA)

## Data Quality
- Single source of truth for identities
- No duplicate/conflicting records
- Consistent data across all systems
- Data quality scoring (0-100)

------------------------------------------------------------------------

# What to Build: Core IAM vs Supporting Systems

## START WITH (Months 1-6): Core IAM Master Data

**Essential master data tables to build first:**

1. ✅ Employee Master - WHO (identity)
2. ✅ Contractor Master - WHO (external identity)
3. ✅ System Master - WHAT (systems/applications)
4. ✅ Role & Permission Master - WHAT (permissions)
5. ✅ Access Rights Master - HOW (who gets what)
6. ✅ Group Master - WHAT (security groups)
7. ✅ Approval Authority Master - WHO APPROVES
8. ✅ Security Policy Master - HOW (MFA, passwords)
9. ✅ SOD Rules Master - PREVENT (segregation of duties)
   - **Prevents conflicting role combinations** (fraud prevention)
   - Example: Cannot have BOTH "Create Loan" AND "Approve Loan" (same person)
   - Enforces compliance rules (SOX, GLBA, Dodd-Frank)
   - Blocks dangerous access combinations automatically
   - See: `SOD_RULES_MASTER_EXPLAINED.md` for details
10. ✅ System Access History Master - AUDIT (trail)
11. ✅ External Access Master - WHO (third-party)
12. ✅ Emergency Access Master - SECURITY (break-glass)
13. ✅ Entitlement Master - WHAT (bundled access)
14. ✅ Audit/Compliance Rules Master - COMPLIANCE

**Result:** Core IAM platform ready. Can integrate with ServiceNow, Okta, AWS.

---

## ADD LATER (Months 7-9): Supporting Systems

**Add these if needed:**

15. ⚠️ Department Master - If organizing by department
16. ⚠️ Location Master - If regional compliance needed (GDPR, CCPA)
17. ⚠️ Device Master - If device compliance required
18. ⚠️ Training Master - If training prerequisites required

---

## INTEGRATE WITH (Concurrent): ServiceNow & Keychain

**These are PROCESSES and SYSTEMS, not master data:**

- **ServiceNow** - Access request workflow
  - Queries master data for approvals
  - Updates Access Rights Master when approved
  - Triggers provisioning (Glue, Lambda)

- **Keychain** - Credential management
  - Stores passwords securely
  - Uses Access Rights Master to know who gets what
  - Sends credentials to employees

**Note:** These integrate WITH your master data platform, not part of building master data.

---

# Which Master Tables Need Full MDM Treatment?

**Important:** You don't need to perform full MDM on all 18 tables. MDM is expensive (data quality checks, deduplication, matching logic). Focus it on tables that drive critical decisions and come from multiple sources.

---

## MDM Prioritization by Table

### HIGH Priority for Full MDM (Must Do)

These tables come from multiple systems and directly affect access control. They need:
- Data quality checks
- Deduplication and matching logic
- Golden record creation
- Data lineage tracking

| Table | Why MDM is Critical | Data Sources | MDM Effort |
|-------|-------------------|--------------|-----------|
| **Employee Master** | Employees exist in HR, AD, AWS, Okta (different names, IDs). Needs matching to single golden record. | SAP HR, Active Directory, AWS IAM | **HIGH** |
| **Contractor Master** | External vendors from multiple systems. Hard to reconcile. | Vendor DB, HR, ServiceNow, Finance | **HIGH** |
| **Role & Permission Master** | Each system defines roles differently (AWS roles ≠ Okta roles ≠ Salesforce roles). Must normalize. | AWS IAM, Salesforce, Okta API | **HIGH** |
| **Access Rights Master** | Most critical table - drives WHO gets WHAT access. Needs validation and conflict checking. | ServiceNow approvals, manual requests, bulk imports | **HIGH** |
| **Group Master** | Groups exist in multiple places (AD groups, Okta groups, AWS IAM groups). Must reconcile. | Active Directory, Okta, AWS IAM | **HIGH** |

**Result:** These 5 tables are your MDM foundation. Focus here first.

---

### MEDIUM Priority for MDM (Should Do)

| Table | Why MDM Helps | Data Sources | MDM Effort |
|-------|---------------|--------------|-----------|
| **System/Application Master** | Multiple systems report themselves differently. Standardize naming. | AWS, Okta, ServiceNow, manual inventory | **MEDIUM** |
| **Approval Authority Master** | Single HR source mostly, but needs org structure validation. | SAP HR, org charts | **MEDIUM** |
| **Department Master** | Usually single source (HR), but needs hierarchy validation. | SAP HR | **LOW-MEDIUM** |

---

### LOW Priority for MDM (Minimal/Reference Data Only)

These don't need heavy MDM. They're either static rules or audit-only:

| Table | Why MDM Not Needed | Treatment | MDM Effort |
|-------|-------------------|-----------|-----------|
| **Security Policy Master** | Static rules. No matching or dedup needed. | Direct ingestion from policy system | **MINIMAL** |
| **SOD Rules Master** | Static configuration. No reconciliation needed. | Upload CSV/JSON, version control | **MINIMAL** |
| **Audit/Compliance Rules Master** | Static rules. No dedup needed. | Direct ingestion from compliance system | **MINIMAL** |
| **Device Master** | Usually single source (asset system). Assets are unique. | Direct ingestion, basic validation | **LOW** |
| **External Access Master** | Reference table. No dedup needed. | Maintain as-is with validation | **MINIMAL** |
| **Emergency Access Master** | Audit trail. Append-only records. | Log events, no matching | **MINIMAL** |
| **Training Master** | Audit trail. Append-only records. | Log completion events | **MINIMAL** |
| **System Access History Master** | Audit trail. Append-only records. | Log all access grants/revokes | **MINIMAL** |

---

## Recommended Implementation Timeline

### **Phase 1 (Months 1-3): Build HIGH Priority MDM**

Focus on the 5 critical tables:

```
Week 1-2:   Design data model for Employee Master
Week 3-4:   Build ingestion from HR, AD, Okta
Week 5-6:   Implement matching/dedup logic
Week 7-8:   Build Contractor Master (similar pattern)
Week 9-10:  Build Role & Permission Master
Week 11-12: Build Access Rights Master (most complex)
```

**Deliverable:** Core MDM platform with 5 tables. Can integrate with ServiceNow.

**Value:** 80% of the functionality, 40% of the effort.

---

### **Phase 2 (Months 4-6): Add MEDIUM Priority Tables**

```
Week 13-14: Add System/Application Master
Week 15-16: Add Approval Authority Master
Week 17-18: Add Department Master
Week 19-20: Optimization & quality checks
```

**Deliverable:** Extended MDM platform. Now fully operational.

---

### **Phase 3 (Months 7+): Add LOW Priority Reference Data**

```
Add one table per month as needed:
- Month 7: Security Policy Master (upload policy rules)
- Month 8: SOD Rules Master (define segregation rules)
- Month 9: Device Master (if device compliance required)
- Month 10+: Other tables as business needs arise
```

**Note:** These tables can be added anytime because they don't require heavy MDM logic. They're simple reference data.

---

## Decision Framework: Does This Table Need MDM?

Ask these 3 questions:

| Question | Answer | Needs MDM? |
|----------|--------|-----------|
| Does it come from **multiple systems**? | Yes | ✅ YES |
| Does it have **data quality issues** (duplicates, inconsistencies)? | Yes | ✅ YES |
| Does it **directly drive access decisions**? | Yes | ✅ YES |
| --- | --- | --- |
| Is it a **static reference table** (rules, policies)? | Yes | ❌ NO |
| Does it come from **single authoritative source**? | Yes | ❌ NO (just validate) |
| Is it **audit/history only** (append-only)? | Yes | ❌ NO |

**Examples:**
- Employee Master: Multiple sources + quality issues + drives access = **MDM CRITICAL** ✅
- SOD Rules Master: Single source + static = **No MDM needed** ❌
- Access Rights Master: Multiple sources (HR, requests, approvals) + drives access = **MDM CRITICAL** ✅
- Training Master: Audit trail only = **No MDM needed** ❌

---

## What "Full MDM" Means for a Table

When you do full MDM on a table, you implement:

1. **Data Ingestion** - Pull from all source systems
2. **Data Validation** - Check for required fields, data types, format
3. **Data Standardization** - Normalize names, formats, codes
4. **Matching & Deduplication** - Find duplicates across systems
5. **Golden Record Creation** - Create single source of truth
6. **Conflict Resolution** - Decide which source is authoritative
7. **Data Quality Scoring** - Rate quality 0-100%
8. **Change Tracking** - Audit who changed what, when
9. **Export to Consumers** - Distribute to downstream systems
10. **Monitoring** - Alert on quality issues

**For 5 HIGH priority tables:** Implement all 10 steps.

**For 3 MEDIUM priority tables:** Implement steps 1-6, skip 7-10.

**For 10 LOW priority tables:** Implement steps 1-3 only (basic ingestion + validation).

---

## Cost Implication

**HIGH Priority MDM (5 tables):** 60% of effort, 90% of value

**MEDIUM Priority MDM (3 tables):** 30% of effort, 8% of value

**LOW Priority (10 tables):** 10% of effort, 2% of value

**Recommendation:** Start with HIGH priority. You'll have a working MDM system in 6 months. Add MEDIUM priority for polish. Add LOW priority only if business needs it.

---

# Technology Stack for IAM/MDM

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Ingestion** | AWS Glue | ETL for identity data from HR, AD, AWS, Okta |
| **Ingestion** | Lambda | Real-time webhooks and API calls |
| **Storage** | S3 | Data lake (raw, master, exports) |
| **Storage** | RDS PostgreSQL | Golden identity records and audit logs |
| **APIs** | Lambda + API Gateway | Query identity data in real-time |
| **Events** | Kafka (MSK) | Stream identity changes to subscribers |
| **Events** | EventBridge | Route events to provisioning functions |
| **Orchestration** | Step Functions | Automate provisioning workflows |
| **Monitoring** | CloudWatch | Logs, metrics, dashboards |
| **Infrastructure** | Terraform | Infrastructure as Code |

------------------------------------------------------------------------

# Implementation Timeline

| Phase | Duration | What Happens |
|-------|----------|--------------|
| **Discovery** | Weeks 1-4 | Audit current state, gather requirements, cost-benefit analysis |
| **Architecture** | Weeks 5-8 | Design data model, design AWS architecture |
| **Development** | Months 2-6 | Build ingestion jobs, matching logic, provisioning workflows |
| **Pilot** | Weeks 7-12 | Test with 1,000 employees, gather feedback |
| **Production** | Weeks 13-16 | Phased rollout to all employees |
| **Operations** | Ongoing | Monitor, optimize, add new features |

**Total: 9 months to production**

------------------------------------------------------------------------

# Responsibilities of an IAM/MDM Data Engineer

Typical responsibilities include:

- Designing cloud-native identity architectures on AWS
- Building data ingestion pipelines from multiple sources
- Developing PySpark and Python ETL jobs
- Matching and deduplicating employee records
- Implementing data quality and validation rules
- Creating golden identity records
- Building automated provisioning workflows
- Exposing identity data through APIs
- Publishing identity changes through event streams
- Implementing audit logging and compliance reporting
- Monitoring data quality and pipeline health
- Supporting CI/CD and Infrastructure as Code

The ultimate objective is to provide a reliable, scalable, and governed **single source of truth** for employee identities that every system and business function can trust.

------------------------------------------------------------------------

# Key Questions This Solves

**Q: How do we onboard new employees faster?**
A: Automate provisioning with IAM/MDM - 30 minutes instead of 7 days

**Q: How do we ensure consistent identity across systems?**
A: Single golden record is source of truth for all systems

**Q: How do we immediately revoke access when someone leaves?**
A: Automatic deprovisioning across all systems within 1 hour

**Q: How do we comply with regulatory audits?**
A: Complete audit trail of all identity changes and access approvals

**Q: How do we reduce manual identity management work?**
A: Automation reduces manual effort by 80%

**Q: How do we know who has access to what?**
A: Real-time dashboard and APIs query golden identity record

------------------------------------------------------------------------

# Is MDM the Best Solution for IAM?

**Alternative 1: Point-to-Point Integrations**
- Each system syncs to every other system
- Works for 2-3 years, then becomes unmaintainable
- "Spaghetti architecture" with 15+ connectors
- Recommended for: Small companies (< 500 employees)

**Alternative 2: Custom Built Solution**
- Build from scratch tailored to your needs
- Costs $2-3M, takes 18-24 months
- High maintenance burden
- Recommended for: Rare - almost never the best choice

**Alternative 3: Master Data Management (MDM)** ✅ **RECOMMENDED**
- Investment: $650K (Year 1)
- Annual savings: $750K
- Payback period: 10 months
- 5-year net benefit: $2.3M
- Scalable to 50,000+ employees
- Industry best practices
- Recommended for: Large enterprises (50,000+ employees, 100+ systems)

------------------------------------------------------------------------

## Conclusion

IAM/MDM is MDM applied to employee identities instead of customers or vendors. It creates a single, authoritative source of truth for who employees are, what access they need, and what systems they can use.

For large enterprises with:
- 50,000+ employees
- 100+ systems requiring access management
- Strong compliance requirements
- Need for operational efficiency

**IAM/MDM is the best solution.** It pays for itself in less than a year while dramatically improving security, compliance, and operational efficiency.
