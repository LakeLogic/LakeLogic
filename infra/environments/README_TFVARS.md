# Terraform Variables Files (.tfvars)

This directory contains environment-specific configuration in `.tfvars` files.

## 📁 **File Structure**

```
infra/environments/
├── dev/
│   ├── dev.tfvars              ✅ Committed (non-sensitive config)
│   ├── secrets.tfvars.template  📝 Template for secrets
│   ├── secrets.auto.tfvars      🚫 Gitignored (your local secrets)
│   ├── main.tf
│   ├── variables.tf
│   └── ...
├── test/
│   ├── test.tfvars             ✅ Committed
│   └── ...
└── prod/
    ├── prod.tfvars             ✅ Committed
    └── ...
```

---

## 🔐 **Secrets vs. Configuration**

### **Non-Sensitive (in .tfvars files):**
✅ Project name, environment, region  
✅ VM sizes, CPU/memory allocation  
✅ Network CIDR blocks  
✅ Database SKUs  
✅ Tags and metadata  

### **Sensitive (NOT in .tfvars files):**
🔒 Database passwords  
🔒 SSH private keys  
🔒 API keys, tokens  
🔒 Azure subscription/tenant IDs  

---

## 🚀 **Usage**

### **In CI/CD (GitHub Actions):**

The workflows automatically use the correct `.tfvars` file:

```yaml
terraform plan -var-file="${{ env.ENVIRONMENT_NAME }}.tfvars"
```

Secrets are provided via environment variables:
```yaml
env:
  TF_VAR_db_admin_password: ${{ secrets.DB_ADMIN_PASSWORD }}
  TF_VAR_jumpbox_ssh_public_key: ${{ secrets.JUMPBOX_SSH_PUBLIC_KEY }}
```

---

### **Local Development:**

#### **Option 1: Using secrets.auto.tfvars (Recommended)**

```bash
cd infra/environments/dev

# Copy template
cp secrets.tfvars.template secrets.auto.tfvars

# Edit with your actual secrets
nano secrets.auto.tfvars

# Run Terraform (auto.tfvars is loaded automatically)
terraform plan
terraform apply
```

#### **Option 2: Using Environment Variables**

```bash
export TF_VAR_db_admin_password="YourSecurePassword123!"
export TF_VAR_jumpbox_ssh_public_key="ssh-rsa AAAA..."
export TF_VAR_subscription_id="00000000-0000-0000-0000-000000000000"
export TF_VAR_tenant_id="00000000-0000-0000-0000-000000000000"

terraform plan -var-file="dev.tfvars"
```

#### **Option 3: Interactive Prompt**

```bash
terraform plan -var-file="dev.tfvars"
# Terraform will prompt for missing required variables
```

---

## 📝 **File Contents**

### **dev.tfvars**
```hcl
project_name = "lineagelogic"
environment  = "dev"
location     = "uksouth"

compute_config = {
  cpu    = 1.0
  memory = "2.0Gi"
}
```

### **test.tfvars**
Same structure, production-like config for staging

### **prod.tfvars**
Same structure, production-grade resources

---

## 🔄 **Adding a New Variable**

### 1. Define in `variables.tf`

```hcl
variable "new_feature_flag" {
  description = "Enable new feature"
  type        = bool
  default     = false
}
```

### 2. Add to environment `.tfvars`

```hcl
# dev.tfvars
new_feature_flag = true

# test.tfvars
new_feature_flag = true

# prod.tfvars
new_feature_flag = false  # Keep disabled in prod until tested
```

### 3. Use in Terraform code

```hcl
resource "azurerm_something" "example" {
  count = var.new_feature_flag ? 1 : 0
  # ...
}
```

---

## ⚠️ **Security Best Practices**

1. ✅ **Never commit secrets** to `.tfvars` files
2. ✅ **Use `*.auto.tfvars`** for local secrets (git-ignored)
3. ✅ **Review diffs** before committing `.tfvars` changes
4. ✅ **Use environment variables** for secrets in CI/CD
5. ✅ **Rotate secrets regularly**
6. ✅ **Restrict IP access** in production (jumpbox_allowed_ips)

---

## 🆚 **Environment Differences**

| Variable       | Dev             | Test               | Prod               |
| -------------- | --------------- | ------------------ | ------------------ |
| **CPU**        | 1.0             | 1.0                | 2.0                |
| **Memory**     | 2.0Gi           | 2.0Gi              | 4.0Gi              |
| **DB SKU**     | B_Standard_B1ms | GP_Standard_D2s_v3 | GP_Standard_D4s_v3 |
| **VNet**       | 10.1.0.0/16     | 10.2.0.0/16        | 10.0.0.0/16        |
| **SSH Access** | 0.0.0.0/0       | Restricted IPs     | Restricted IPs     |

---

## 🐛 **Troubleshooting**

### **Error: Missing required variable**

```
Error: No value for required variable
│ 
│   on variables.tf line 41:
│   41: variable "db_admin_password" {
```

**Solution:** Provide via environment variable or `secrets.auto.tfvars`

```bash
export TF_VAR_db_admin_password="YourPassword"
```

---

### **Plan shows unexpected changes**

Check if the correct `.tfvars` file is being used:

```bash
terraform plan -var-file="dev.tfvars"  # Explicit
```

---

### **Secrets in git history**

If you accidentally committed secrets:

1. Remove from git immediately
2. Rotate the compromised secrets
3. Use: `git filter-branch` or BFG Repo-Cleaner to remove from history

---

## 📚 **Related Documentation**

- [Terraform Input Variables](https://www.terraform.io/language/values/variables)
- [Variable Definition Files](https://www.terraform.io/language/values/variables#variable-definitions-tfvars-files)
- [GitHub Workflows README](./README_INFRA_WORKFLOWS.md)
