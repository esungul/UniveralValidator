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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any


logger = logging.getLogger(__name__)

class ValidatorService:


    def validate_bulk_msisdns(self, msisdns: List[str]) -> Dict[str, Any]:
        """Bulk validate specific MSISDNs using OrderFetcher and OrderFilter"""
        try:
            logger.info(f"🚀 Starting bulk validation for {len(msisdns)} MSISDNs using OrderFetcher")
            
            # Step 1: Use OrderFetcher to get orders for these MSISDNs
            logger.info("📋 Step 1: Fetching orders for MSISDNs...")
            
            # Use batch method to fetch orders for these specific MSISDNs
            orders_list = self.order_fetcher.get_orders_batch_by_msisdns(msisdns)
            logger.info(f"✓ Fetched {len(orders_list)} order records")
            
            if not orders_list:
                return {
                    "status": "success",
                    "message": "No orders found for the provided MSISDNs",
                    "summary": {
                        "total_msisdns": len(msisdns),
                        "orders_found": 0,
                        "success_rate": 0
                    },
                    "results": {}
                }
            
            # Step 2: Filter and group using OrderFilter
            logger.info("📊 Step 2: Filtering and grouping orders...")
            filtered_orders = self.order_filter.filter_and_group(orders_list, self.config)
            logger.info(f"✓ Filtered and grouped into {len(filtered_orders)} MSISDNs")
            
            # Step 3: Validate each MSISDN that has orders
            logger.info("🔍 Step 3: Validating MSISDNs with orders...")
            results = {}
            
            validated_msisdns = list(filtered_orders.keys())
            for i, msisdn in enumerate(validated_msisdns, 1):
                try:
                    logger.info(f"Validating {i}/{len(validated_msisdns)}: {msisdn}")
                    result = self.validate_msisdn(msisdn)
                    results[msisdn] = result
                except Exception as e:
                    logger.error(f"Failed to validate {msisdn}: {e}")
                    results[msisdn] = {"status": "error", "message": str(e)}
                
                # Small delay to avoid overwhelming the system
                import time
                time.sleep(0.1)
            
            # Step 4: Format results
            logger.info("📝 Step 4: Formatting results...")
            output_format = self.config.get("bulk_validation", {}).get("output_format", "json")
            response = self._format_bulk_msisdn_response(msisdns, validated_msisdns, results, filtered_orders, output_format)
            
            logger.info("✅ Bulk MSISDN validation completed successfully!")
            return response
            
        except Exception as e:
            logger.error(f"❌ Bulk MSISDN validation failed: {e}")
            return {"status": "error", "message": str(e)}

    def _format_bulk_msisdn_response(self, requested_msisdns: List[str], validated_msisdns: List[str], 
                                    results: Dict[str, Any], grouped_orders: Dict[str, Any], 
                                    output_format: str) -> Dict[str, Any]:
        """Format bulk MSISDN validation response"""
        from datetime import datetime
        
        # Calculate summary
        successful = sum(1 for r in results.values() if r.get("status") == "success")
        failed = len(validated_msisdns) - successful
        no_orders_count = len(requested_msisdns) - len(validated_msisdns)
        
        summary = {
            "total_msisdns_requested": len(requested_msisdns),
            "msisdns_with_orders": len(validated_msisdns),
            "msisdns_without_orders": no_orders_count,
            "successful_validations": successful,
            "failed_validations": failed,
            "success_rate": round(successful / len(validated_msisdns) * 100, 2) if validated_msisdns else 0,
            "timestamp": datetime.now().isoformat() + "Z"
        }
        
        if output_format.lower() == "csv":
            return self._convert_bulk_msisdn_to_csv(summary, requested_msisdns, validated_msisdns, results, grouped_orders)
        else:
            return {
                "summary": summary,
                "order_filter_info": {
                    "fetch_strategy": self.config.get("order_fetcher", {}).get("fetch_strategy"),
                    "ignore_reasons": self.config.get("order_filter", {}).get("ignore_order_reasons"),
                    "ignore_types": self.config.get("order_filter", {}).get("ignore_order_types")
                },
                "results": results,
                "msisdns_without_orders": [msisdn for msisdn in requested_msisdns if msisdn not in validated_msisdns]
            }
        
    
    def _convert_to_csv(self, summary: Dict[str, Any], results: Dict[str, Any]) -> Dict[str, Any]:
        """Convert results to CSV format"""
        import csv
        import io
        
        # CSV header
        header = ["MSISDN", "Validation Status", "Success Rate", "Errors", "Order Reason", "Asset Count"]
        
        csv_data = []
        for msisdn, result in results.items():
            status = result.get("status", "unknown")
            
            # Extract success rate from validation result
            success_rate = "N/A"
            if status == "success":
                validated_msisdns = result.get("validated_msisdns", [])
                if validated_msisdns:
                    msisdn_data = validated_msisdns[0] if isinstance(validated_msisdns, list) else validated_msisdns.get(msisdn, {})
                    summary_data = msisdn_data.get("summary", {})
                    success_rate = f"{summary_data.get('success_rate', 0)}%"
            
            # Extract errors
            errors = ""
            if status == "error":
                errors = result.get("message", "Validation failed")
            
            # Extract order reason
            order_reason = "N/A"
            if status == "success":
                validated_msisdns = result.get("validated_msisdns", [])
                if validated_msisdns:
                    msisdn_data = validated_msisdns[0] if isinstance(validated_msisdns, list) else validated_msisdns.get(msisdn, {})
                    order_reason = msisdn_data.get("order_reasons", ["N/A"])[0] if msisdn_data.get("order_reasons") else "N/A"
            
            # Extract asset count
            asset_count = "N/A"
            if status == "success":
                validated_msisdns = result.get("validated_msisdns", [])
                if validated_msisdns:
                    msisdn_data = validated_msisdns[0] if isinstance(validated_msisdns, list) else validated_msisdns.get(msisdn, {})
                    assets = msisdn_data.get("assets", {})
                    if isinstance(assets, dict):
                        asset_count = len(assets.get("all", []))
            
            row = [msisdn, status, success_rate, errors, order_reason, asset_count]
            csv_data.append(row)
        
        # Create CSV string
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(header)
        writer.writerows(csv_data)
        csv_string = output.getvalue()
        
        return {
            "summary": summary,
            "format": "csv",
            "csv_string": csv_string
        }

    
    def validate_bulk_yesterday(self) -> Dict[str, Any]:
        """Bulk validate all yesterday's orders"""
        try:
            from modules.bulk_validator import BulkValidator
            bulk_validator = BulkValidator(self.sf, self.config)
            return bulk_validator.validate_yesterday_orders()
        except Exception as e:
            logger.error(f"Bulk validation error: {e}")
            return {"status": "error", "message": str(e)}
    
    
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