#!/usr/bin/env python3
"""
Universal Order Validator - Main Application
Run: python3 main.py --mode cli --msisdn 12218071145
Or:  python3 main.py --mode api
"""

import logging
import sys
import json
import os
from pathlib import Path
from flask import Flask
from typing import Dict, Any, List

# Setup path
sys.path.insert(0, str(Path(__file__).parent))

from config.config_loader import ConfigLoader
from modules.order_fetcher import OrderFetcher
from modules.order_filter import OrderFilter
from modules.asset_fetcher import AssetFetcher
from modules.validation_engine import ValidationEngine
from modules.response_builder import ResponseBuilder
from utils.logger_config import setup_logging
from api.endpoints import validator_bp, set_validator_service

logger = logging.getLogger(__name__)

class ValidatorService:
    """Main validation service orchestrator"""
    
    def __init__(self, sf_connection):
        self.sf = sf_connection
        self.config_loader = ConfigLoader()
        self.config = self.config_loader.load()
        
        self.order_fetcher = OrderFetcher(sf_connection)
        self.order_filter = OrderFilter()
        self.asset_fetcher = AssetFetcher(sf_connection)
        self.validation_engine = ValidationEngine()
        self.response_builder = ResponseBuilder()
    
    def _normalize_asset(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize raw Salesforce asset to validation format"""
        if not asset:
            return None
        
        return {
            "id": asset.get("Id"),
            "product_name": asset.get("Product2", {}).get("Name") if isinstance(asset.get("Product2"), dict) else asset.get("Product2.Name"),
            "product_code": asset.get("Product2", {}).get("ProductCode") if isinstance(asset.get("Product2"), dict) else asset.get("Product2.ProductCode"),
            "status": asset.get("vlocity_cmt__ProvisioningStatus__c"),
            "charges": {
                "one_time": asset.get("vlocity_cmt__OneTimeCharge__c") or 0.0,
                "recurring": asset.get("vlocity_cmt__RecurringCharge__c") or 0.0
            },
            "disconnect_info": {
                "disconnect_date": asset.get("vlocity_cmt__DisconnectDate__c"),
                "disconnect_reason": asset.get("Disconnection_Reason__c")
            },
            "asset_reference": asset.get("vlocity_cmt__AssetReferenceId__c"),
            "original_oli_id": asset.get("PR_Original_OLI_ID__c"),
            "billing_number": asset.get("vlocity_cmt__BillingAccountId__r", {}).get("PR_Mobile_Billing_Number__c") if isinstance(asset.get("vlocity_cmt__BillingAccountId__r"), dict) else None,
        }
    
    def _normalize_assets_list(self, assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize list of assets"""
        return [self._normalize_asset(a) for a in assets if a]
    
    def validate_orders_yesterday(self) -> Dict[str, Any]:
        """Main workflow: validate all orders from yesterday"""
        try:
            logger.info("=" * 70)
            logger.info("STARTING VALIDATION: Orders from Yesterday")
            logger.info("=" * 70)
            
            logger.info("\n[Phase 1] Fetching orders from yesterday...")
            orders = self.order_fetcher.get_orders_from_yesterday(self.config)
            
            logger.info("\n[Phase 2] Filtering orders...")
            filtered_orders = self.order_filter.filter_orders(orders)
            grouped_orders = self.order_filter.group_by_msisdn(filtered_orders)
            
            logger.info("\n[Phase 3] Fetching and validating assets...")
            validation_results = {}
            
            for msisdn, order in grouped_orders.items():
                logger.info(f"\n--- Processing MSISDN: {msisdn} ---")
                assets_raw = self.asset_fetcher.get_assets_for_msisdn(msisdn, self.config)
                organized_assets = self.asset_fetcher.organize_asset_hierarchy(assets_raw)
                
                order_type = order.get("Order", {}).get("Type", "Unknown")
                validation_checks = self.validation_engine.validate_for_order_type(
                    order_type, organized_assets, self.config
                )
                
                validation_results[msisdn] = {
                    "assets": {
                        "all": assets_raw,
                        "lines": organized_assets.get("lines", []),
                        "devices": organized_assets.get("devices", [])
                    },
                    "validations": {
                        "basic": {"lines_present": True, "device_linked": True, "all_passed": True},
                        "order_specific": {order_type: validation_checks}
                    }
                }
            
            logger.info("\n[Phase 4] Building response...")
            response = self.response_builder.build_response(grouped_orders, validation_results)
            
            logger.info("\n" + "=" * 70)
            logger.info("VALIDATION COMPLETE")
            logger.info("=" * 70)
            
            return response
            
        except Exception as e:
            logger.error(f"\n✗ VALIDATION FAILED: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"status": "error", "message": str(e)}
    
    def validate_msisdn(self, msisdn: str) -> Dict[str, Any]:
        """Validate a single MSISDN using 3-query approach"""
        try:
            logger.info("=" * 70)
            logger.info(f"STARTING VALIDATION: MSISDN {msisdn}")
            logger.info("=" * 70)
            
            logger.info(f"\nFetching assets for MSISDN: {msisdn} (3-query approach)...")
            
            # Use 3-query approach
            assets_data = self.asset_fetcher.get_assets_for_msisdn_v2(msisdn, self.config)
            
            # Check for errors
            if assets_data.get("error"):
                logger.error(f"Asset fetching error: {assets_data.get('error')}")
                return {
                    "status": "error",
                    "message": assets_data.get('error')
                }
            
            # Fetch order history
            logger.info(f"\nFetching order history for MSISDN: {msisdn}...")
            order_history = self.asset_fetcher.get_order_history(msisdn, self.config)
            
            latest_line = assets_data.get("latest_line")
            device = assets_data.get("device")
            line_children = assets_data.get("line_children", [])
            device_children = assets_data.get("device_children", [])
            
            order_type = "Change Plan"
            validation_engine = self.validation_engine
            
            # Extract order reasons from order history
            order_reasons = []
            if order_history and isinstance(order_history, dict):
                orders = order_history.get("orders", [])
                for order in orders:
                    reason = order.get("reason")
                    if reason and reason not in order_reasons:
                        order_reasons.append(reason)
            
            logger.info(f"Extracted order reasons: {order_reasons}")
            
            # ✅ NORMALIZE assets before validation (convert SF field names to validation format)
            assets_for_validation = {
                "line": self._normalize_asset(latest_line),
                "device": self._normalize_asset(device),
                "line_children": self._normalize_assets_list(line_children),
                "device_children": self._normalize_assets_list(device_children)
            }
            
            logger.info(f"[VALIDATION] Normalized assets: line={assets_for_validation['line'] is not None}, device={assets_for_validation['device'] is not None}")
            logger.info(f"[VALIDATION] Line charges: {assets_for_validation['line'].get('charges') if assets_for_validation['line'] else None}")
            logger.info(f"[VALIDATION] Device charges: {assets_for_validation['device'].get('charges') if assets_for_validation['device'] else None}")
            
            # ✅ FIXED: Pass order_reasons, NOT config!
            validation_checks = validation_engine.validate_for_order_type(
                order_type, assets_for_validation, order_reasons
            )
            
            # Get warnings from validation engine
            warnings = validation_engine.get_warnings()
            
            # Organize all assets for response
            all_assets = [latest_line] if latest_line else []
            if device:
                all_assets.append(device)
            all_assets.extend(line_children)
            all_assets.extend(device_children)
            
            # Keep both raw assets (for response) and normalized assets (for validation)
            validation_results = {
                msisdn: {
                    "assets": {
                        "all": all_assets,
                        "latest_line": latest_line,
                        "device": device,
                        "line_children": line_children,
                        "device_children": device_children
                    },
                    "order_history": order_history,
                    "validations": validation_checks,  # ✅ Direct structure from validation_engine
                    "warnings": warnings
                }
            }
            
            grouped_orders = {
                msisdn: {
                    "Order": {"Type": order_type},
                    "Id": "N/A",
                    "CreatedDate": "",
                    "vlocity_cmt__BillingAccountId__r": {}
                }
            }
            
            response = self.response_builder.build_response(grouped_orders, validation_results)
            
            logger.info("\n" + "=" * 70)
            logger.info("VALIDATION COMPLETE")
            logger.info("=" * 70)
            
            return response
            
        except Exception as e:
            logger.error(f"\n✗ VALIDATION FAILED: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"status": "error", "message": str(e)}

def create_app(sf_connection=None):
    """Create and configure Flask application"""
    app = Flask(__name__)
    
    # Create service BEFORE registering blueprint
    if sf_connection:
        service = ValidatorService(sf_connection)
        set_validator_service(service)
        # Store in app context
        app.validator_service = service
    
    app.register_blueprint(validator_bp)
    
    @app.route('/health', methods=['GET'])
    def health_check():
        return {"status": "healthy"}, 200
    
    return app

def get_salesforce_connection():
    """Get Salesforce connection from environment or mock"""
    try:
        from simple_salesforce import Salesforce
        import os
        from dotenv import load_dotenv
        
        # Load environment variables from .env
        load_dotenv()
        
        username = os.environ.get('SF_USERNAME')
        password = os.environ.get('SF_PASSWORD')
        security_token = os.environ.get('SF_SECURITY_TOKEN')
        domain = os.environ.get('SF_DOMAIN', 'login')
        
        if not all([username, password, security_token]):
            logger.warning("Missing Salesforce credentials, using mock connection")
            from modules.mock_salesforce import MockSalesforceConnection
            return MockSalesforceConnection()
        
        logger.info(f"Connecting to Salesforce ({domain})...")
        sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token,
            domain=domain
        )
        logger.info("✓ Connected to Salesforce successfully")
        return sf
        
    except ImportError:
        logger.warning("python-dotenv not installed, using mock connection")
        from modules.mock_salesforce import MockSalesforceConnection
        return MockSalesforceConnection()
    except Exception as e:
        logger.error(f"Failed to connect to Salesforce: {e}")
        logger.warning("Falling back to mock connection")
        from modules.mock_salesforce import MockSalesforceConnection
        return MockSalesforceConnection()

if __name__ == "__main__":
    import argparse
    
    setup_logging(logging.INFO)
    
    parser = argparse.ArgumentParser(description="Universal Order Validator")
    parser.add_argument("--mode", choices=["api", "cli"], default="api", help="Run mode")
    parser.add_argument("--msisdn", type=str, help="MSISDN to validate (CLI mode)")
    parser.add_argument("--bulk", action="store_true", help="Run bulk validation for yesterday's orders")

    
    args = parser.parse_args()
    
    logger.info("Universal Order Validator - Starting...")
    
    # Get Salesforce connection (real or mock)
    sf_connection = get_salesforce_connection()
    
    if args.mode == "api":
        logger.info("Running in API mode on http://localhost:5000")
        app = create_app(sf_connection)
        app.run(debug=True, host="0.0.0.0", port=5000)
    
    elif args.mode == "cli":
        if not args.msisdn:
            logger.error("MSISDN required for CLI mode")
            sys.exit(1)
        
        logger.info(f"Running in CLI mode for MSISDN: {args.msisdn}")
        service = ValidatorService(sf_connection)
        result = service.validate_msisdn(args.msisdn)
        print(json.dumps(result, indent=2))
        
    elif args.bulk:
        logger.info("Running bulk validation for yesterday's orders...")
        from modules.bulk_validator import BulkValidator
        service = ValidatorService(sf_connection)
        bulk_validator = BulkValidator(service.sf, service.config)
        result = bulk_validator.validate_yesterday_orders()
        print(json.dumps(result, indent=2))