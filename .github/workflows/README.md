# GitHub Workflows Documentation

This directory contains comprehensive GitHub Actions workflows that automatically fix issues and maintain code quality.

## 🤖 Available Workflows

### 1. Auto-Fix Workflow (`auto-fix.yml`)
**Triggers:** Push to main/develop/hotfix/feature branches, Pull Requests

**What it does:**
- ✅ **Automatically fixes** code formatting with Black
- ✅ **Automatically sorts** imports with isort
- ✅ **Automatically applies** PEP8 fixes with autopep8
- ✅ **Fixes common issues** (missing newlines, trailing whitespace)
- ✅ **Validates** Python syntax
- ✅ **Scans** for security issues
- ✅ **Auto-commits** fixes when running on push
- ✅ **Comments** on PRs with fix summary

### 2. Pylint Code Quality (`pylint.yml`)
**Triggers:** Push to main/develop/hotfix/feature branches, Pull Requests

**What it does:**
- 🔍 **Analyzes** code with Pylint
- 📊 **Provides** detailed quality score
- 📝 **Comments** on PRs with results
- ⚠️ **Only fails** on actual errors (not warnings)
- 📁 **Uploads** detailed reports as artifacts

### 3. Pre-commit Checks (`pre-commit.yml`)
**Triggers:** Pull Requests only

**What it does:**
- 🔍 **Runs** comprehensive pre-commit hooks
- 🛡️ **Security scanning** with Bandit, Safety, and Semgrep
- 📝 **Provides** detailed fix instructions
- 💬 **Comments** on PRs with actionable feedback

### 4. AutoPEP8 Formatter (`autopep8.yml`)
**Triggers:** Push to main/develop, Pull Requests

**What it does:**
- 🔧 **Applies** autopep8 formatting
- 🤖 **Auto-commits** changes
- ⚡ **Fast** and focused on PEP8 compliance

## 🚀 Benefits Over Previous Workflows

### Before (Problems):
- ❌ Workflows would **only complain** about issues
- ❌ **No automatic fixes** applied
- ❌ **Syntax errors** in workflow files
- ❌ **Generic error messages** with no actionable feedback
- ❌ **Failures without guidance** on how to fix

### After (Solutions):
- ✅ **Automatically fixes** most common issues
- ✅ **Clear, actionable feedback** with specific fix instructions
- ✅ **Valid YAML syntax** in all workflow files
- ✅ **Detailed reports** with scores and summaries
- ✅ **Progressive enhancement** - fix what can be fixed, report what needs manual attention
- ✅ **PR comments** with helpful guidance
- ✅ **Artifact uploads** for detailed analysis

## 🔧 How to Use

### For Developers:
1. **Nothing special required** - workflows run automatically
2. **Check PR comments** for automatic feedback
3. **Download artifacts** for detailed reports
4. **Follow fix suggestions** in workflow comments

### For Repository Maintainers:
1. **Ensure secrets are set** (if using Claude workflow)
2. **Review workflow permissions** in repository settings
3. **Customize workflow triggers** as needed for your branching strategy

## 📋 Workflow Features

### Auto-Fix Workflow Features:
- 🔧 **Black** - Code formatting
- 🔧 **isort** - Import sorting  
- 🔧 **autopep8** - PEP8 compliance
- 🔧 **File fixes** - Newlines, whitespace, encoding
- 🔍 **Syntax validation** - Ensures code compiles
- 🛡️ **Security scanning** - Basic security checks
- 📊 **Summary reporting** - Clear results summary

### Quality Assurance Features:
- 📈 **Code scoring** with Pylint
- 🛡️ **Multi-tool security scanning**
- 📁 **Artifact preservation** for detailed analysis
- 💬 **Intelligent PR commenting**
- ⚠️ **Error vs Warning distinction**

## 🎯 Customization

### Adjusting Pylint Rules:
Edit the `.pylintrc` configuration created by the workflow, or add a permanent one to the repository root.

### Modifying Auto-Fix Behavior:
Edit the `auto-fix.yml` workflow to add/remove tools or change their configuration.

### Adding New Security Scans:
The `pre-commit.yml` workflow can be extended with additional security tools.

## 🔄 Workflow Dependencies

All workflows are designed to work with:
- **Python 3.10-3.11**
- **Ubuntu latest** runners
- **Standard Python packages** (see requirements.txt)
- **No external dependencies** beyond GitHub Actions marketplace

## 📞 Troubleshooting

### If workflows fail:
1. **Check the logs** in the Actions tab
2. **Look for PR comments** with specific guidance
3. **Download artifacts** for detailed reports
4. **Run tools locally** using the same commands as in workflows

### Common fixes:
```bash
# Install and run locally
pip install black isort autopep8 pylint
black .
isort .
autopep8 --recursive --in-place --aggressive --aggressive .
```

## 🔮 Future Enhancements

Potential improvements:
- **Test coverage** reporting
- **Performance benchmarking**
- **Dependency vulnerability** auto-fixing
- **Documentation generation**
- **Release automation**

---

**Note:** These workflows replace the previous problematic workflows that only complained about issues without providing solutions. The new approach focuses on **automatic fixes** and **actionable feedback**.