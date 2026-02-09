# Jump Server Deployment Guide

The jump server (bastion host) is a Windows Server VM with GUI database tools for managing your PostgreSQL database.

## 💰 **IMPORTANT: Cost Savings**

The jump server **automatically stops after creation** to save costs (~$60/month).

**To use the jump server:**
1. **Start it:** Run `scripts\start_jumpbox.bat` (takes 2-3 min)
2. **Connect:** Use Azure Bastion from Portal
3. **Stop it:** Run `scripts\stop_jumpbox.bat` when done

**Monthly Costs:**
- **Running 24/7:** ~$200/month (VM $60 + Bastion $140)
- **Stopped (only storage):** ~$150/month (Disk $10 + Bastion $140)
- **Savings when stopped:** ~$50/month

💡 **Tip:** Only run the jump server when you need it!

## Prerequisites

### 1. Generate SSH Key (If You Don't Have One)

```bash
# On your local machine
ssh-keygen -t rsa -b 4096 -C "your_email@example.com" -f ~/.ssh/lineagelogic_jumpbox

# View your public key
cat ~/.ssh/lineagelogic_jumpbox.pub
```

Copy the entire output (starts with `ssh-rsa ...`)

### 2. Add SSH Key to GitHub Secrets

1. Go to GitHub → Settings → Secrets → Actions
2. Add new secret:
   - Name: `JUMPBOX_SSH_PUBLIC_KEY`
   - Value: Your SSH public key from above

## Deployment

### Option 1: Deploy via GitHub Actions

The jump server will be deployed automatically when you push changes to `infra/` on the `dev` branch.

```bash
git add infra/
git commit -m "feat: add jump server for database admin"
git push origin dev
```

### Option 2: Deploy Manually via Terraform

```bash
cd infra/environments/dev

# Set the SSH key as environment variable
export TF_VAR_jumpbox_ssh_public_key="ssh-rsa AAAAB3NzaC1y..."

# Initialize and apply
terraform init
terraform apply
```

## Usage

### Connect to Jump Server

After deployment, get the connection info:

```bash
# Via Terraform
cd infra/environments/dev
terraform output jumpbox_ssh_command

# Or check Azure Portal for the public IP
```

Then SSH in:

```bash
ssh -i ~/.ssh/lineagelogic_jumpbox azureuser@<JUMPBOX_IP>
```

### Import Database

Once connected to the jump server:

```bash
# 1. Upload your SQL file (from your local machine)
scp -i ~/.ssh/lineagelogic_jumpbox db_backup_20260201.sql azureuser@<JUMPBOX_IP>:~/

# 2. SSH into jump server
ssh -i ~/.ssh/lineagelogic_jumpbox azureuser@<JUMPBOX_IP>

# 3. Import to PostgreSQL
export PGPASSWORD='your-postgres-password'

psql "host=lineagelogic-dev-postgres.postgres.database.azure.com port=5432 dbname=lineagelogic user=lladmin sslmode=require" -f ~/db_backup_20260201.sql
```

### Other Admin Tasks

```bash
# Connect to PostgreSQL interactively
psql "host=lineagelogic-dev-postgres.postgres.database.azure.com port=5432 dbname=lineagelogic user=lladmin sslmode=require"

# Run queries
psql "..." -c "SELECT * FROM users LIMIT 10;"

# Backup database
pg_dump -h lineagelogic-dev-postgres.postgres.database.azure.com -U lladmin -d lineagelogic > backup_$(date +%Y%m%d).sql
```

## Security Best Practices

### Restrict SSH Access by IP

Update `infra/environments/dev/jumpbox-vars.tf`:

```hcl
variable "jumpbox_allowed_ips" {
  default = [
    "1.2.3.4/32",   # Your home IP
    "5.6.7.8/32"    # Your office IP
  ]
}
```

### Use SSH Agent Forwarding

```bash
# Add key to agent
ssh-add ~/.ssh/lineagelogic_jumpbox

# Connect with agent forwarding
ssh -A azureuser@<JUMPBOX_IP>
```

### Disable Jump Server When Not Needed

```bash
# Stop the VM to save costs
az vm stop --name lineagelogic-dev-jumpbox --resource-group lineagelogic-dev-rg

# Start it when needed
az vm start --name lineagelogic-dev-jumpbox --resource-group lineagelogic-dev-rg
```

## Cost Optimization

- **VM Size:** Standard_B1s (~$10/month)
- **Disk:** Standard HDD 30GB (~$2/month)
- **Total:** ~$12/month when running 24/7

**Tip:** Stop the VM when not in use to only pay for storage (~$2/month).

## Troubleshooting

### Can't Connect via SSH

1. Check NSG rules allow your IP
2. Verify SSH key is correct
3. Check VM is running: `az vm list --query "[?name=='lineagelogic-dev-jumpbox'].{Name:name, PowerState:powerState}" -o table`

### Can't Reach PostgreSQL

1. Verify jump server is in the correct subnet (db subnet)
2. Check PostgreSQL allows VNet connections
3. Test DNS resolution: `nslookup lineagelogic-dev-postgres.postgres.database.azure.com`

## Removal

To remove the jump server:

```bash
# Comment out the jumpbox module in main.tf
# Then apply
terraform apply
```

Or delete manually:
```bash
az vm delete --name lineagelogic-dev-jumpbox --resource-group lineagelogic-dev-rg --yes
```
