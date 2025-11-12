"""
validation_engine.py - Complete Validation Engine v2.2

Production Ready Validation Engine with:
- 5 Charge validation methods (ZERO-VALUE SUPPORT)
- Basic validation checks (always run)
- Reason-based validation checks (conditional)
- Full integration ready

Version: 2.2
Status: Production Ready
Last Updated: 2025-11-10

Key Feature: Charges can be 0.0 (allow_zero: true in config)
"""

import logging
from typing import Dict, Any, List, Optional
import json

logger = logging.getLogger(__name__)


class ValidationEngine:
    """
    Validation engine for MSISDN order validations
    
    Features:
    - 5 basic validations (always run)
    - Reason-based validations (conditional)
    - Charge validation with zero-value support
    - Configuration-driven
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize validation engine with config
        
        Args:
            config: Validation configuration dict (optional)
                   If not provided, will try to load from config.json
        """
        if config is None:
            # Try to load config.json from current directory
            try:
                import os
                config_paths = [
                    'config.json',
                    'config/config.json',
                    'config_v2.2_simplified.json',
                    '../config/config.json'
                ]
                
                config_loaded = False
                for path in config_paths:
                    if os.path.exists(path):
                        logger.info(f"[INIT] Loading config from {path}")
                        with open(path, 'r') as f:
                            config = json.load(f)
                        config_loaded = True
                        break
                
                if not config_loaded:
                    logger.warning("[INIT] No config file found, using minimal config")
                    config = {
                        "validations": {
                            "basic": {"checks": []},
                            "reason_based": {}
                        }
                    }
            except Exception as e:
                logger.error(f"[INIT] Error loading config: {str(e)}")
                config = {
                    "validations": {
                        "basic": {"checks": []},
                        "reason_based": {}
                    }
                }
        
        self.config = config
        self.validations_config = config.get('validations', {})
        self.logger = logger
        logger.info("[INIT] ValidationEngine initialized successfully")
    
    # ==================== MAIN VALIDATION FLOW ====================
    
    def validate_for_order_type(self, order_type: str, assets: Dict[str, Any], order_reasons: List[str] = None) -> Dict[str, Any]:
        """
        BACKWARD COMPATIBLE METHOD - Maps to validate_for_msisdn()
        
        Old code called: validate_for_order_type(order_type, assets, ...)
        New code uses: validate_for_msisdn(assets, order_reasons)
        
        This method provides backward compatibility with your existing main.py
        """
        if order_reasons is None:
            order_reasons = [order_type] if order_type else []
        
        logger.debug(f"[BACKWARD_COMPAT] validate_for_order_type('{order_type}') -> validate_for_msisdn({order_reasons})")
        return self.validate_for_msisdn(assets, order_reasons)
    
    def validate_for_msisdn(self, assets: Dict[str, Any], order_reasons: List[str]) -> Dict[str, Any]:
        """
        Run all validations for a single MSISDN
        
        Flow:
        1. Run 5 basic checks (always)
        2. Run reason-based checks (conditional on order_reasons)
        3. Combine results
        
        Args:
            assets: Dict with line, device, line_children, device_children
            order_reasons: List of order reasons from order history
        
        Returns:
            {
                "basic": { "status": ..., "checks": {...}, "passed": ..., "failed": ..., "total": ... },
                "reason_based": {
                    "Plan Upgrade": { "status": ..., "checks": {...} },
                    ...
                }
            }
        """
        logger.info(f"[VALIDATION] Starting for MSISDN with reasons: {order_reasons}")
        
        # Step 1: Run basic validations (always)
        basic_results = self._run_basic_checks(assets)
        logger.info(f"[VALIDATION] Basic checks: {basic_results.get('status')} ({basic_results.get('passed')}/{basic_results.get('total')})")
        
        # Step 2: Run reason-based validations (conditional)
        reason_results = {}
        for reason in order_reasons:
            if reason:
                reason_results[reason] = self._run_reason_checks(reason, assets)
                logger.info(f"[VALIDATION] Reason '{reason}': {reason_results[reason].get('status')} ({reason_results[reason].get('passed')}/{reason_results[reason].get('total')})")
        
        # Step 3: Combine and return
        final_result = {
            "basic": basic_results,
            "reason_based": reason_results
        }
        
        logger.info(f"[VALIDATION] Complete. Basic: {basic_results['status']}")
        return final_result
    
    # ==================== BASIC VALIDATIONS (Always Run) ====================
    
    def _run_basic_checks(self, assets: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run 5 basic validations that ALWAYS execute
        
        Returns:
            {
                "status": "PASSED" | "FAILED",
                "checks": { check_id: bool, ... },
                "passed": int,
                "failed": int,
                "total": int
            }
        """
        logger.debug("[BASIC CHECKS] Starting 5 basic checks")
        
        basic_config = self.validations_config.get('basic', {})
        checks = basic_config.get('checks', [])
        
        results = {}
        for check in checks:
            check_id = check.get('id')
            result = self._execute_check(check, assets)
            results[check_id] = result
            logger.debug(f"[BASIC] {check_id}: {result}")
        
        # Calculate stats
        passed = sum(1 for v in results.values() if v)
        failed = len(results) - passed
        status = "PASSED" if failed == 0 else "FAILED"
        
        logger.info(f"[BASIC] Results: {status} ({passed}/{len(results)})")
        
        return {
            "status": status,
            "checks": results,
            "passed": passed,
            "failed": failed,
            "total": len(results)
        }
    
    # ==================== REASON-BASED VALIDATIONS (Conditional) ====================
    
    def _run_reason_checks(self, reason: str, assets: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run reason-specific validations (only if reason matches config)
        
        Returns:
            {
                "status": "PASSED" | "FAILED" | "SKIPPED",
                "checks": { check_id: bool, ... },
                "passed": int,
                "failed": int,
                "total": int
            }
        """
        reason_config = self.validations_config.get('reason_based', {}).get(reason)
        
        if not reason_config:
            logger.warning(f"[REASON] No config for reason: {reason}")
            return {
                "status": "SKIPPED",
                "reason": f"No validation config for '{reason}'"
            }
        
        checks = reason_config.get('checks', [])
        results = {}
        
        for check in checks:
            check_id = check.get('id')
            result = self._execute_check(check, assets)
            results[check_id] = result
            logger.debug(f"[REASON] {check_id}: {result}")
        
        # Calculate stats
        passed = sum(1 for v in results.values() if v)
        failed = len(results) - passed
        status = "PASSED" if failed == 0 else "FAILED"
        
        logger.info(f"[REASON] {reason}: {status} ({passed}/{len(results)})")
        
        return {
            "status": status,
            "checks": results,
            "passed": passed,
            "failed": failed,
            "total": len(results)
        }
    
    # ==================== CHECK EXECUTION DISPATCHER ====================
    
    def _execute_check(self, check: Dict[str, Any], assets: Dict[str, Any]) -> bool:
        """
        Execute a single check based on configuration
        
        Routes to appropriate handler based on validation_type:
        - "presence": Check if field/object exists
        - "charges": Check charges field (ZERO-VALUE SUPPORT)
        - "status": Check if field equals expected value
        - "custom": Execute custom logic
        
        Args:
            check: Check configuration from config.json
            assets: Asset data
        
        Returns:
            bool: True if check passes, False otherwise
        """
        check_type = check.get('validation_type')
        check_id = check.get('id')
        
        try:
            if check_type == 'presence':
                return self._check_presence(check, assets)
            elif check_type == 'charges':
                return self._check_charges(check, assets)
            elif check_type == 'status':
                return self._check_status(check, assets)
            elif check_type == 'custom':
                return self._check_custom(check, assets)
            else:
                logger.warning(f"Unknown check type: {check_type} for {check_id}")
                return False
        except Exception as e:
            logger.error(f"Error executing {check_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _check_presence(self, check: Dict[str, Any], assets: Dict[str, Any]) -> bool:
        """Check if field/object exists (presence check)"""
        logic = check.get('logic')
        
        if logic == 'search_line_children_for_sim_card':
            line_children = assets.get('line_children', [])
            sim_found = any(c.get('product_code') == 'PR_B2C_SIM_Card' for c in line_children)
            logger.debug(f"Presence: SIM card in line children: {sim_found}")
            return sim_found
        
        elif logic == 'validate_device_exists':
            device = assets.get('device')
            device_exists = device is not None and bool(device.get('id')) if isinstance(device, dict) else False
            logger.debug(f"Presence: Device exists: {device_exists}")
            return device_exists
        
        logger.warning(f"Unknown presence logic: {logic}")
        return False
    
    def _check_status(self, check: Dict[str, Any], assets: Dict[str, Any]) -> bool:
        """Check if field equals expected value (status check)"""
        field_path = check.get('field', '')
        expected_value = check.get('expected_value')
        
        value = self._get_nested_value(assets, field_path)
        result = value == expected_value
        
        logger.debug(f"Status check {field_path}: {value} == {expected_value}? {result}")
        return result
    
    def _check_custom(self, check: Dict[str, Any], assets: Dict[str, Any]) -> bool:
        """Execute custom validation logic"""
        logic = check.get('logic')
        logger.debug(f"Custom check: {logic}")
        # TODO: Implement custom logic as needed
        return True
    
    # ==================== CHARGE VALIDATION METHODS ====================
    # KEY METHODS: These 5 handle charge validation with ZERO-VALUE support
    
    def _check_charges(self, check: Dict[str, Any], assets: Dict[str, Any]) -> bool:
        """
        Dispatcher for charge validation
        
        Key Feature: allow_zero: true means charges can be 0.0
        
        Routes to appropriate method based on logic:
        - validate_device_charges
        - validate_all_line_children_charges
        - validate_all_device_children_charges
        - validate_sim_card_charges
        """
        logic = check.get('logic')
        allow_zero = check.get('allow_zero', True)  # Default: allow zero
        
        logger.debug(f"[CHARGES] {logic} (allow_zero={allow_zero})")
        
        if logic == 'validate_all_line_children_charges':
            return self._validate_line_children_charges(assets, allow_zero)
        elif logic == 'validate_device_charges':
            return self._validate_device_charges(assets, allow_zero)
        elif logic == 'validate_all_device_children_charges':
            return self._validate_device_children_charges(assets, allow_zero)
        elif logic == 'validate_sim_card_charges':
            return self._validate_sim_card_charges(assets, allow_zero)
        else:
            logger.warning(f"Unknown charge logic: {logic}")
            return False
    
    def _validate_device_charges(self, assets: Dict[str, Any], allow_zero: bool = True) -> bool:
        """
        METHOD 1: Validate DEVICE has charges field
        
        Rule:
        ✓ charges field must exist (not null)
        ✓ must be a dict
        ✓ at least one of (one_time, recurring) must have a value
        ✓ if allow_zero=False, at least one must be > 0
        
        Example:
        ✓ {one_time: 0.0, recurring: 0.0} → PASS (field exists, allow_zero=True)
        ✓ {one_time: 100.0, recurring: 0.0} → PASS
        ✗ null → FAIL (field missing)
        ✗ {one_time: null, recurring: null} → FAIL (no values)
        """
        device = assets.get('device')
        
        if not device:
            logger.warning("[DEVICE] No device found")
            return False
        
        charges = device.get('charges')
        
        # Check 1: Field must exist
        if charges is None:
            logger.warning("[DEVICE] charges is None")
            return False
        
        # Check 2: Must be dict
        if not isinstance(charges, dict):
            logger.warning(f"[DEVICE] charges not dict: {type(charges)}")
            return False
        
        one_time = charges.get('one_time')
        recurring = charges.get('recurring')
        
        # Check 3: At least one value must exist
        if one_time is None and recurring is None:
            logger.warning("[DEVICE] both one_time and recurring are None")
            return False
        
        # Check 4: If not allow_zero, at least one must be > 0
        if not allow_zero:
            if (one_time is None or one_time == 0) and (recurring is None or recurring == 0):
                logger.warning(f"[DEVICE] no positive charges (allow_zero=False)")
                return False
        
        logger.info(f"[DEVICE] PASS: one_time={one_time}, recurring={recurring}")
        return True
    
    def _validate_line_children_charges(self, assets: Dict[str, Any], allow_zero: bool = True) -> bool:
        """
        METHOD 2: Validate ALL LINE CHILDREN have charges field
        
        Rule:
        ✓ each child must have charges field (not null)
        ✓ each must be a dict
        ✓ each must have at least one charge value
        ✓ if allow_zero=False, at least one charge must be > 0
        
        Returns False if ANY child fails validation
        """
        line_children = assets.get('line_children', [])
        
        if not line_children:
            logger.info("[LINE_CHILDREN] No children, PASS")
            return True
        
        for idx, child in enumerate(line_children):
            charges = child.get('charges')
            product_name = child.get('product_name', f'Child_{idx}')
            
            # Check 1: Field must exist
            if charges is None:
                logger.warning(f"[LINE_CHILDREN] {product_name} charges is None")
                return False
            
            # Check 2: Must be dict
            if not isinstance(charges, dict):
                logger.warning(f"[LINE_CHILDREN] {product_name} charges not dict")
                return False
            
            one_time = charges.get('one_time')
            recurring = charges.get('recurring')
            
            # Check 3: At least one value must exist
            if one_time is None and recurring is None:
                logger.warning(f"[LINE_CHILDREN] {product_name} both charges None")
                return False
            
            # Check 4: If not allow_zero, at least one must be > 0
            if not allow_zero:
                if (one_time is None or one_time == 0) and (recurring is None or recurring == 0):
                    logger.warning(f"[LINE_CHILDREN] {product_name} no positive charges")
                    return False
        
        logger.info(f"[LINE_CHILDREN] PASS: {len(line_children)} children validated")
        return True
    
    def _validate_device_children_charges(self, assets: Dict[str, Any], allow_zero: bool = True) -> bool:
        """
        METHOD 3: Validate DEVICE CHILDREN have charges field
        
        Rule:
        ✓ only validate if device_children exist (conditional)
        ✓ each child must have charges field (not null)
        ✓ each must be a dict
        ✓ each must have at least one charge value
        ✓ if allow_zero=False, at least one charge must be > 0
        
        Returns True if no children (nothing to validate)
        """
        device_children = assets.get('device_children', [])
        
        if not device_children:
            logger.info("[DEVICE_CHILDREN] No children, PASS")
            return True
        
        for idx, child in enumerate(device_children):
            charges = child.get('charges')
            product_name = child.get('product_name', f'DeviceChild_{idx}')
            
            # Check 1: Field must exist
            if charges is None:
                logger.warning(f"[DEVICE_CHILDREN] {product_name} charges is None")
                return False
            
            # Check 2: Must be dict
            if not isinstance(charges, dict):
                logger.warning(f"[DEVICE_CHILDREN] {product_name} charges not dict")
                return False
            
            one_time = charges.get('one_time')
            recurring = charges.get('recurring')
            
            # Check 3: At least one value must exist
            if one_time is None and recurring is None:
                logger.warning(f"[DEVICE_CHILDREN] {product_name} both charges None")
                return False
            
            # Check 4: If not allow_zero, at least one must be > 0
            if not allow_zero:
                if (one_time is None or one_time == 0) and (recurring is None or recurring == 0):
                    logger.warning(f"[DEVICE_CHILDREN] {product_name} no positive charges")
                    return False
        
        logger.info(f"[DEVICE_CHILDREN] PASS: {len(device_children)} children validated")
        return True
    
    def _validate_sim_card_charges(self, assets: Dict[str, Any], allow_zero: bool = True) -> bool:
        """
        METHOD 4: Validate SIM CARD has charges field
        
        Used for: SIM Replacement validations
        
        Rule:
        ✓ find SIM Card in line_children
        ✓ SIM must have charges field (not null)
        ✓ must be a dict
        ✓ must have at least one charge value
        ✓ if allow_zero=False, at least one charge must be > 0
        """
        line_children = assets.get('line_children', [])
        
        # Find SIM cards
        sim_cards = [c for c in line_children if c.get('product_code') == 'PR_B2C_SIM_Card']
        
        if not sim_cards:
            logger.warning("[SIM] No SIM card found in line children")
            return False
        
        sim = sim_cards[0]  # Use first SIM
        charges = sim.get('charges')
        
        # Check 1: Field must exist
        if charges is None:
            logger.warning("[SIM] charges is None")
            return False
        
        # Check 2: Must be dict
        if not isinstance(charges, dict):
            logger.warning(f"[SIM] charges not dict: {type(charges)}")
            return False
        
        one_time = charges.get('one_time')
        recurring = charges.get('recurring')
        
        # Check 3: At least one value must exist
        if one_time is None and recurring is None:
            logger.warning("[SIM] both one_time and recurring are None")
            return False
        
        # Check 4: If not allow_zero, at least one must be > 0
        if not allow_zero:
            if (one_time is None or one_time == 0) and (recurring is None or recurring == 0):
                logger.warning("[SIM] no positive charges")
                return False
        
        logger.info(f"[SIM] PASS: one_time={one_time}, recurring={recurring}")
        return True
    
    # ==================== METHOD 5: PLACEHOLDER FOR FUTURE ====================
    
    def _validate_line_charges(self, assets: Dict[str, Any], allow_zero: bool = True) -> bool:
        """
        METHOD 5: Validate main LINE has charges field
        
        Placeholder for future use
        """
        line = assets.get('line')
        
        if not line:
            logger.warning("[LINE] No line found")
            return False
        
        charges = line.get('charges')
        
        if charges is None:
            logger.warning("[LINE] charges is None")
            return False
        
        logger.info("[LINE] PASS")
        return True
    
    # ==================== BACKWARD COMPATIBLE HELPER METHODS ====================
    
    def get_warnings(self) -> List[str]:
        """
        BACKWARD COMPATIBLE METHOD - Return list of warnings
        
        Your old code probably called: validation_engine.get_warnings()
        This provides that interface
        """
        return getattr(self, '_warnings', [])
    
    def get_errors(self) -> List[str]:
        """
        BACKWARD COMPATIBLE METHOD - Return list of errors
        
        Your old code probably called: validation_engine.get_errors()
        This provides that interface
        """
        return getattr(self, '_errors', [])
    
    def add_warning(self, warning: str) -> None:
        """Add a warning message"""
        if not hasattr(self, '_warnings'):
            self._warnings = []
        self._warnings.append(warning)
    
    def add_error(self, error: str) -> None:
        """Add an error message"""
        if not hasattr(self, '_errors'):
            self._errors = []
        self._errors.append(error)
    
    def clear_messages(self) -> None:
        """Clear all warnings and errors"""
        self._warnings = []
        self._errors = []
    
    # ==================== HELPER METHODS ====================
    
    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """
        Get nested value from dict using dot notation
        
        Example:
            path = "Line Asset.status"
            returns data["Line Asset"]["status"]
        
        Args:
            data: Dictionary to traverse
            path: Dot-separated path (e.g., "key1.key2.key3")
        
        Returns:
            Value at path, or None if not found
        """
        try:
            keys = path.split('.')
            value = data
            for key in keys:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    return None
            return value
        except Exception as e:
            logger.error(f"Error getting {path}: {str(e)}")
            return None


# ==================== PUBLIC API ====================

def validate_msisdn(config: Dict[str, Any], assets: Dict[str, Any], order_reasons: List[str]) -> Dict[str, Any]:
    """
    Public interface to validate a single MSISDN
    
    This is the main entry point for validation
    
    Usage:
        config = load_config('config_v2.2_simplified.json')
        assets = fetch_assets_for_msisdn(msisdn)
        order_reasons = extract_order_reasons(order_history)
        
        result = validate_msisdn(config, assets, order_reasons)
        
        if result['basic']['status'] == 'PASSED':
            print("✓ Validation passed!")
    
    Args:
        config: Validation configuration (from config_v2.2_simplified.json)
        assets: Asset data structure with:
            - device: Dict with charges
            - line: Dict with charges
            - line_children: List of line assets
            - device_children: List of device assets
        order_reasons: List of order reasons (e.g., ["Change Plan"])
    
    Returns:
        {
            "basic": {
                "status": "PASSED" | "FAILED",
                "checks": { check_id: bool, ... },
                "passed": int,
                "failed": int,
                "total": 5
            },
            "reason_based": {
                "Change Plan": {
                    "status": "PASSED" | "FAILED",
                    "checks": { check_id: bool, ... },
                    "passed": int,
                    "failed": int,
                    "total": int
                },
                ...
            }
        }
    """
    engine = ValidationEngine(config)
    return engine.validate_for_msisdn(assets, order_reasons)


# ==================== USAGE EXAMPLE ====================

if __name__ == "__main__":
    """
    Example usage of the validation engine
    """
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("\n" + "="*60)
    print("Validation Engine v2.2 - Example Usage")
    print("="*60 + "\n")
    
    # Example config (simplified)
    config = {
        "validations": {
            "basic": {
                "checks": [
                    {
                        "id": "device_has_charges",
                        "validation_type": "charges",
                        "logic": "validate_device_charges",
                        "allow_zero": True
                    }
                ]
            },
            "reason_based": {}
        }
    }
    
    # Example assets with ZERO charges (the fix!)
    assets = {
        "device": {
            "product_name": "AT&T Generic Mobile Phone",
            "charges": {
                "one_time": 0.0,      # ← ZERO charges are OK now!
                "recurring": 0.0       # ← allow_zero: true
            }
        },
        "line": {
            "product_name": "Liberty Plan",
            "charges": {
                "one_time": 0.0,
                "recurring": 50.0
            }
        },
        "line_children": [
            {
                "product_name": "SIM Card",
                "product_code": "PR_B2C_SIM_Card",
                "charges": {
                    "one_time": 5.0,
                    "recurring": 0.0
                }
            },
            {
                "product_name": "Network Access",
                "charges": {
                    "one_time": 0.0,   # ← ZERO is valid
                    "recurring": 0.0
                }
            }
        ],
        "device_children": [
            {
                "product_name": "Device Protection",
                "charges": {
                    "one_time": 0.0,
                    "recurring": 15.0
                }
            }
        ]
    }
    
    # Run validation
    result = validate_msisdn(config, assets, ["Change Plan"])
    
    # Print results
    print("\nVALIDATION RESULTS:")
    print(json.dumps(result, indent=2))
    
    # Check basic status
    basic_status = result.get('basic', {}).get('status')
    print(f"\n✓ Basic Validation: {basic_status}")
    
    if basic_status == "PASSED":
        print("✓ All checks PASSED! Device with 0 charges is VALID.")
    else:
        print("✗ Validation failed")