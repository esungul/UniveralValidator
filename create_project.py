#!/usr/bin/env python3
"""
Universal Order Validator - Complete Setup Script
Run: python3 setup_simple.py
"""

import os
import sys
import subprocess
import platform
from pathlib import Path

# Colors
GREEN = '\033[92m'
BLUE = '\033[94m'
YELLOW = '\033[93m'
RED = '\033[91m'
END = '\033[0m'

PROJECT_DIR = Path("/Users/esungul/Documents/Projects/UniveralValidator")
PYTHON_CMD = sys.executable

def print_header(text):
    print(f"\n{BLUE}{text}{END}")
    print("=" * 70)

def print_success(text):
    print(f"{GREEN}✓ {text}{END}")

def print_error(text):
    print(f"{RED}✗ {text}{END}")

def print_warning(text):
    print(f"{YELLOW}⚠ {text}{END}")

def run_command(cmd, description="", silent=False):
    """Run a command and return success status"""
    try:
        if silent:
            subprocess.run(cmd, shell=True, check=True, 
                         stdout=subprocess.DEVNULL, 
                         stderr=subprocess.DEVNULL)
        else:
            subprocess.run(cmd, shell=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        if description:
            print_error(description)
        return False

def main():
    print("\n")
    print("╔═══════════════════════════════════════════════════════════════════════╗")
    print("║                                                                       ║")
    print("║     UNIVERSAL ORDER VALIDATOR - SETUP (SIMPLIFIED)                  ║")
    print("║                                                                       ║")
    print("╚═══════════════════════════════════════════════════════════════════════╝")
    print()

    # Step 1: Check Prerequisites
    print_header("Step 1: Checking Prerequisites")
    
    # Check Python
    print(f"Python version: {sys.version.split()[0]}")
    if sys.version_info < (3, 7):
        print_error("Python 3.7+ required")
        return False
    print_success("Python 3.7+ found")
    
    # Check pip
    result = run_command(f"{PYTHON_CMD} -m pip --version", silent=True)
    if result:
        print_success("pip found")
    else:
        print_error("pip not found")
        return False

    # Step 2: Verify Project Directory
    print_header("Step 2: Verifying Project Directory")
    
    if not PROJECT_DIR.exists():
        print(f"Creating project directory: {PROJECT_DIR}")
        PROJECT_DIR.mkdir(parents=True, exist_ok=True)
    
    print_success(f"Project directory: {PROJECT_DIR}")
    os.chdir(PROJECT_DIR)
    print_success("Changed to project directory")

    # Step 3: Check Existing Files
    print_header("Step 3: Checking Existing Files")
    
    main_py = PROJECT_DIR / "main.py"
    if not main_py.exists():
        print_error("main.py not found!")
        print("Make sure you've run the project generator first")
        return False
    
    print_success("main.py exists")
    
    py_files = list(PROJECT_DIR.glob("**/*.py"))
    print_success(f"Found {len(py_files)} Python files")

    # Step 4: Install Dependencies
    print_header("Step 4: Installing Python Dependencies")
    
    req_file = PROJECT_DIR / "requirements.txt"
    if not req_file.exists():
        print_error("requirements.txt not found")
        return False
    
    print("Installing dependencies...")
    print("(This may take 1-2 minutes on first run)")
    
    # Try different installation methods
    install_cmd = f"{PYTHON_CMD} -m pip install --upgrade pip setuptools wheel -q"
    run_command(install_cmd, silent=True)
    
    # Install requirements
    install_req_cmd = f"{PYTHON_CMD} -m pip install -r requirements.txt --no-build-isolation -q"
    
    if not run_command(install_req_cmd, silent=True):
        print_warning("Standard installation had issues, trying alternative...")
        
        # Try with user flag
        install_req_cmd = f"{PYTHON_CMD} -m pip install -r requirements.txt --user -q"
        if not run_command(install_req_cmd, silent=True):
            print_error("Could not install dependencies")
            print("Try manually:")
            print(f"  {PYTHON_CMD} -m pip install -r requirements.txt")
            return False
    
    print_success("Dependencies installed")

    # Step 5: Verify Installation
    print_header("Step 5: Verifying Installation")
    
    packages = {
        'flask': 'Flask',
        'simple_salesforce': 'simple-salesforce',
        'pydantic': 'Pydantic'
    }
    
    for module, name in packages.items():
        try:
            __import__(module)
            print_success(f"{name} installed")
        except ImportError:
            print_error(f"{name} not found")
            return False

    # Step 6: Test CLI Mode
    print_header("Step 6: Running CLI Test")
    
    print("Testing with demo MSISDN: 12218071145")
    
    test_cmd = f"{PYTHON_CMD} main.py --mode cli --msisdn 12218071145"
    
    try:
        result = subprocess.run(
            test_cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            print_success("CLI test PASSED")
            
            # Try to parse JSON output
            import json
            try:
                # Find JSON in output
                output = result.stdout
                json_start = output.find('{')
                if json_start != -1:
                    json_str = output[json_start:]
                    data = json.loads(json_str)
                    print_success("JSON output is valid")
                    
                    # Show summary
                    print("\nSample output:")
                    print("─" * 70)
                    print(f"  Status: {data.get('status', 'N/A')}")
                    print(f"  Date: {data.get('date_validated', 'N/A')}")
                    summary = data.get('summary', {})
                    print(f"  MSISDNs Validated: {summary.get('total_msisdns_validated', 0)}")
                    print(f"  Passed: {summary.get('passed', 0)}")
                    print(f"  Failed: {summary.get('failed', 0)}")
                    print(f"  Success Rate: {summary.get('success_rate', 0)}%")
                    print("─" * 70)
            except json.JSONDecodeError:
                print_warning("Could not parse JSON, but CLI ran successfully")
        else:
            print_error("CLI test FAILED")
            print("STDOUT:", result.stdout[:200])
            print("STDERR:", result.stderr[:200])
            return False
            
    except subprocess.TimeoutExpired:
        print_error("CLI test timed out")
        return False
    except Exception as e:
        print_error(f"CLI test error: {e}")
        return False

    # Step 7: Environment Setup Info
    print_header("Step 7: Environment Setup")
    
    env_file = PROJECT_DIR / ".env"
    if env_file.exists():
        print_success(".env file exists")
    else:
        print_warning(".env file not found (optional)")
        print("\nTo connect to real Salesforce, create .env file:")
        print("  cat > .env << 'EOF'")
        print("  SF_USERNAME=your_username")
        print("  SF_PASSWORD=your_password")
        print("  SF_SECURITY_TOKEN=your_token")
        print("  SF_DOMAIN=login")
        print("  EOF")

    # Final message
    print("\n")
    print("╔═══════════════════════════════════════════════════════════════════════╗")
    print("║                    ✅ SETUP COMPLETE!                                ║")
    print("╚═══════════════════════════════════════════════════════════════════════╝")
    print()
    
    print(f"{GREEN}All systems operational!{END}")
    print()
    print("Next steps:")
    print()
    print("1️⃣  Run CLI validation:")
    print(f"   {BLUE}python3 main.py --mode cli --msisdn 12218071145{END}")
    print()
    print("2️⃣  Start API server:")
    print(f"   {BLUE}python3 main.py --mode api{END}")
    print("   Then visit: http://localhost:5000/health")
    print()
    print("3️⃣  Edit validation rules:")
    print(f"   {BLUE}nano config/config.json{END}")
    print()
    print("4️⃣  View documentation:")
    print(f"   {BLUE}cat README.md{END}")
    print(f"   {BLUE}bash QUICK_REFERENCE.sh{END}")
    print()
    print("5️⃣  Connect to real Salesforce:")
    print("   Create .env file with credentials")
    print("   Replace MockSalesforceConnection in main.py")
    print()
    
    print(f"{YELLOW}📍 Project Directory:{END}")
    print(f"   {PROJECT_DIR}")
    print()
    
    print(f"{GREEN}Happy validating! 🚀{END}")
    print()
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)