# Infrastructure Deployment Workflows

This directory contains two separate workflows for safe infrastructure changes:

## 🔄 **Workflows**

### 1. **`infra-plan.yml`** - Automatic Planning (Read-Only)

**Triggers:**
- Push to `dev` or `stage` branches (when `infra/**` changes)
- Pull requests to `dev`, `stage`, or `main`
- Manual dispatch

**What it does:**
- ✅ Runs `terraform plan` automatically
- ✅ Shows what changes will be made
- ✅ Detects destructive changes and warns
- ✅ Posts plan output to PR comments
- ✅ Saves plan artifact for apply workflow
- ❌ **Does NOT apply any changes**

**Use when:**
- You want to see what will change before applying
- Reviewing PRs with infrastructure changes
- Validating Terraform configurations

---

### 2. **`infra-apply.yml`** - Manual Application (Write)

**Triggers:**
- **Manual dispatch ONLY** (no automatic runs)

**What it does:**
- ✅ Applies infrastructure changes
- ✅ Can use saved plan from `infra-plan` run
- ✅ Runs fresh plan if no saved plan provided
- ✅ Final safety check for destructive changes
- ✅ Outputs deployment results

**Use when:**
- You've reviewed the plan and are ready to apply
- Making intentional infrastructure changes
- Deploying to production

---

## 🚀 **Recommended Workflow**

### For Development Changes:

```bash
# 1. Make infrastructure changes locally
git checkout -b feature/add-jumpbox
# ... edit Terraform files ...

# 2. Commit and push
git add infra/
git commit -m "feat: add jump server"
git push origin feature/add-jumpbox

# 3. Create PR
# GitHub will automatically run `infra-plan` and comment with results

# 4. Review the plan in PR comments
# Look for:
#   - Resources being created/updated/deleted
#   - Any "must be replaced" warnings
#   - Unexpected changes

# 5. If plan looks good, merge PR
git checkout dev
git merge feature/add-jumpbox
git push origin dev

# 6. Manually trigger `infra-apply` workflow
# Go to: Actions → Terraform Apply → Run workflow → Select environment
```

---

### For Production Changes:

```bash
# 1. Test in dev first
# ... follow dev workflow above ...

# 2. Create PR to main
git checkout -b release/v1.2.0
git push origin release/v1.2.0

# 3. Review plan on PR carefully
# The plan workflow will run automatically
# Check for any production-specific impacts

# 4. Get team approval on PR

# 5. Merge to main

# 6. Manually trigger apply (with caution!)
# Actions → Terraform Apply → Run workflow
# Environment: prod
# Review the final plan
# Click "Approve and run"
```

---

## ⚠️ **Safety Features**

### Destructive Change Detection

Both workflows check for:
- Resources being **replaced** (`must be replaced`)
- Resources being **destroyed** (`will be destroyed`)

If detected, workflows will:
- ⚠️ Display a warning
- 📝 List affected resources
- ✋ Require manual review (via environment protection)

### Environment Protection Rules

Recommended GitHub Environment settings:

#### **Dev Environment:**
- Required reviewers: 0 (auto-deploy)
- Wait timer: 0 minutes

#### **Test Environment:**
- Required reviewers: 1
- Wait timer: 0 minutes

#### **Prod Environment:**
- Required reviewers: 2+
- Wait timer: 5 minutes
- Restrict to protected branches only

---

## 📋 **Quick Reference**

| Scenario             | Workflow      | Trigger              | Auto-Apply?          |
| -------------------- | ------------- | -------------------- | -------------------- |
| See what will change | `infra-plan`  | Automatic on push/PR | No                   |
| Apply dev changes    | `infra-apply` | Manual               | Yes (after approval) |
| Apply prod changes   | `infra-apply` | Manual               | Yes (2+ approvals)   |
| PR review            | `infra-plan`  | Automatic            | No                   |

---

## 🔧 **Manual Commands** (Local Testing)

```bash
cd infra/environments/dev

# Plan only (safe)
terraform plan

# Plan and save
terraform plan -out=tfplan

# Apply saved plan
terraform apply tfplan

# Check for state lock issues
terraform force-unlock <LOCK_ID>
```

---

## 🐛 **Troubleshooting**

### Plan shows unexpected changes

1. Check if you pulled latest from remote
2. Verify all required secrets are set
3. Run `terraform refresh` to sync state

### State lock errors

```bash
# Force unlock (use with caution)
terraform force-unlock c67deecf-d677-355a-0ce6-3fe64a4dc87c
```

### Apply workflow can't find plan

- Plan artifacts expire after 5 days
- Re-run `infra-plan` or run fresh plan in `infra-apply`

---

## 📚 **Best Practices**

1. ✅ **Always review plans** before applying
2. ✅ **Test in dev first**, then test, then prod
3. ✅ **Use PRs** for infrastructure changes
4. ✅ **Get team approval** for production changes
5. ✅ **Check for destructive changes** warnings
6. ✅ **Keep environment protection rules** enabled
7. ❌ **Never bypass** environment approvals for prod
8. ❌ **Don't rush** infrastructure changes

---

## 🔗 **Related Files**

- `.github/workflows/infra-plan.yml` - Plan workflow
- `.github/workflows/infra-apply.yml` - Apply workflow
- `infra/environments/*/` - Environment-specific configs
- `infra/modules/*/` - Reusable Terraform modules
