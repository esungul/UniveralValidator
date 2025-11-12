from typing import Dict, Any, List
import logging
logger = logging.getLogger(__name__)

class ResponseBuilder:
    """Build comprehensive validation responses"""
    
    def __init__(self):
        pass
    
    def build_response(self, grouped_orders: Dict[str, Any], 
                      validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """Build final response combining orders and validations"""
        
        all_msisdns = []
        for msisdn, validation_data in validation_results.items():
            order_data = grouped_orders.get(msisdn, {})
            msisdn_response = self._build_msisdn_response(msisdn, order_data, validation_data)
            all_msisdns.append(msisdn_response)
        
        # Calculate summary
        passed = sum(1 for m in all_msisdns if m.get("validation_status") == "PASSED")
        passed_with_warnings = sum(1 for m in all_msisdns if m.get("validation_status") == "PASSED_WITH_WARNINGS")
        failed = sum(1 for m in all_msisdns if m.get("validation_status") == "FAILED")
        
        from datetime import datetime
        
        return {
            "date_validated": datetime.now().strftime("%Y-%m-%d"),
            "timestamp": datetime.now().isoformat() + "Z",
            "status": "success",
            "summary": {
                "total_msisdns_validated": len(all_msisdns),
                "passed": passed,
                "passed_with_warnings": passed_with_warnings,
                "failed": failed,
                "success_rate": round(((passed + passed_with_warnings) / len(all_msisdns) * 100), 2) if all_msisdns else 0
            },
            "validated_msisdns": all_msisdns
        }
    
    def _build_msisdn_response(self, msisdn: str, order_data: Dict[str, Any], 
                               validation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build response for a single MSISDN"""
        
        # Extract assets
        assets_raw = validation_data.get("assets", {})
        latest_line = assets_raw.get("latest_line")
        device = assets_raw.get("device")
        line_children = assets_raw.get("line_children", [])
        device_children = assets_raw.get("device_children", [])
        
        # Extract order history and get reasons + status
        order_history = validation_data.get("order_history", [])
        order_reasons, order_status = self._extract_order_info(order_history)
        
        # Extract validations and warnings
        # NEW STRUCTURE from validation_engine: {"basic": {...}, "reason_based": {...}}
        validations = validation_data.get("validations", {})
        logger.debug(f"[RESPONSE] Validations from engine: {validations}")
        warnings = validation_data.get("warnings", [])
        
        # Calculate validation status from BOTH basic and reason-based checks
        # NEW STRUCTURE: { "basic": {...}, "reason_based": {...} }
        
        basic_checks = validations.get("basic", {})
        reason_checks = validations.get("reason_based", {})
        
        # Get basic validation results
        basic_status = basic_checks.get("status", "SKIPPED")
        basic_passed = basic_checks.get("passed", 0)
        basic_failed = basic_checks.get("failed", 0)
        basic_total = basic_checks.get("total", 0)
        
        # Get first reason-based validation results (if any)
        reason_status = "SKIPPED"
        reason_passed = 0
        reason_failed = 0
        reason_total = 0
        first_reason_key = None
        
        for reason_key, reason_data in reason_checks.items():
            if isinstance(reason_data, dict) and reason_data.get("status") != "SKIPPED":
                first_reason_key = reason_key
                reason_status = reason_data.get("status", "SKIPPED")
                reason_passed = reason_data.get("passed", 0)
                reason_failed = reason_data.get("failed", 0)
                reason_total = reason_data.get("total", 0)
                break
        
        # Overall validation status:
        # - FAILED if basic checks failed
        # - FAILED if reason checks failed (if they ran)
        # - PASSED if both basic and reason checks passed
        if basic_status == "FAILED":
            validation_status = "FAILED"
        elif reason_status == "FAILED":
            validation_status = "FAILED"
        elif basic_status == "PASSED" and (reason_status == "PASSED" or reason_status == "SKIPPED"):
            validation_status = "PASSED"
        else:
            validation_status = "PASSED_WITH_WARNINGS"
        
        # Add warnings if present
        if warnings and validation_status == "PASSED":
            validation_status = "PASSED_WITH_WARNINGS"
        
        # Prepare validation checks object for response
        validation_checks = {
            "basic": basic_checks,
            "reason_based": reason_checks if reason_checks else None
        }
        
        # Calculate totals
        total_checks = basic_total + reason_total
        passed_checks = basic_passed + reason_passed
        failed_checks = basic_failed + reason_failed
        
        # Extract line details
        line_details = {}
        if latest_line:
            line_details = self._extract_line_details(latest_line)
        
        # Build response
        response = {
            "msisdn": msisdn,
            "line_details": line_details,
            "validation_status": validation_status,
            "order_being_validated": "Change Plan",
            "order_reasons": order_reasons,
            "order_status": order_status,
            "order_history": self._format_order_history(order_history),
            
            # ===== ASSETS (Clear Naming) =====
            "Device Asset": self._clean_asset(device, "device") if device else None,
            "Line Asset": self._clean_asset(latest_line, "line") if latest_line else None,
            
            "Child of Line": [self._clean_asset(a) for a in line_children] if line_children else None,
            "Child of Device": [self._clean_asset(a) for a in device_children] if device_children else None,
            
            # ===== VALIDATIONS =====
            "validations": validation_checks,  # This already has "basic" and "reason_based"
            
            # ===== SUMMARY =====
            "summary": {
                "status": validation_status,
                "success_rate": round((passed_checks / total_checks * 100), 2) if total_checks > 0 else 0
            }
        }
        
        # Add warnings if present
        if warnings:
            manually_modified_users = list(set([w.get("modified_by") for w in warnings]))
            response["warnings"] = {
                "manual_modifications_detected": True,
                "modified_by_users": manually_modified_users,
                "assets_manually_modified": warnings
            }
        
        # Add notes
        if validation_status == "PASSED":
            response["notes"] = "All checks passed! ✓"
        elif validation_status == "PASSED_WITH_WARNINGS":
            response["notes"] = f"All checks passed but {len(warnings)} asset(s) manually modified. Review warnings. ⚠️"
        else:
            response["notes"] = f"Validation failed. {failed_checks} check(s) failed."
        
        return response
    
    def _extract_line_details(self, line_asset: Dict[str, Any]) -> Dict[str, Any]:
        """Extract basic line details"""
        if not line_asset:
            return {}
        
        return {
            "msisdn": line_asset.get("PR_MSISDN__c"),
            "segment": line_asset.get("vlocity_cmt__BillingAccountId__r", {}).get("Segment__c") if isinstance(line_asset.get("vlocity_cmt__BillingAccountId__r"), dict) else None,
            "cb_subscription_id": line_asset.get("CB_Subscription_Id__c"),
            "billing_number": line_asset.get("vlocity_cmt__BillingAccountId__r", {}).get("PR_Mobile_Billing_Number__c") if isinstance(line_asset.get("vlocity_cmt__BillingAccountId__r"), dict) else None
        }
    
    def _extract_order_info(self, order_history: List[Dict[str, Any]]) -> tuple:
        """Extract last 3 order reasons and status from order history"""
        order_reasons = []
        order_status = None
        
        for i, order_item in enumerate(order_history):
            order = order_item.get("Order", {})
            
            # Get reason
            reason = order.get("vlocity_cmt__Reason__c")
            if reason and reason not in order_reasons:
                order_reasons.append(reason)
            
            # Get status from first order
            if i == 0:
                order_status = order.get("vlocity_cmt__OrderStatus__c")
        
        return order_reasons[:3], order_status
    
        """Extract basic line details"""
        if not line_asset:
            return {}
        
        return {
            "msisdn": line_asset.get("PR_MSISDN__c"),
            "segment": line_asset.get("vlocity_cmt__BillingAccountId__r", {}).get("Segment__c") if isinstance(line_asset.get("vlocity_cmt__BillingAccountId__r"), dict) else None,
            "cb_subscription_id": line_asset.get("CB_Subscription_Id__c"),
            "billing_number": line_asset.get("vlocity_cmt__BillingAccountId__r", {}).get("PR_Mobile_Billing_Number__c") if isinstance(line_asset.get("vlocity_cmt__BillingAccountId__r"), dict) else None
        }
    
    def _format_order_history(self, order_history: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Format detailed order history from OrderItem query"""
        if not order_history:
            return {
                "count": 0,
                "orders": None
            }
        
        orders = []
        for order_item in order_history:
            order = order_item.get("Order", {})
            billing_account = order_item.get("vlocity_cmt__BillingAccountId__r", {})
            
            # Handle nested structures
            if isinstance(billing_account, dict):
                segment = billing_account.get("Segment__c")
                payment_type = billing_account.get("vlocity_cmt__AccountPaymentType__c")
            else:
                segment = order_item.get("vlocity_cmt__BillingAccountId__r.Segment__c")
                payment_type = order_item.get("vlocity_cmt__BillingAccountId__r.vlocity_cmt__AccountPaymentType__c")
            
            orders.append({
                "order_id": order.get("Id"),
                "order_item_id": order_item.get("Id"),
                "type": order.get("Type"),
                "reason": order.get("vlocity_cmt__Reason__c"),
                "status": order.get("vlocity_cmt__OrderStatus__c"),
                "notes": order.get("vlocity_cmt__Notes__c"),
                "created_date": order_item.get("CreatedDate"),
                "modified_date": order_item.get("LastModifiedDate"),
                "submitted_to_om_date": order.get("vlocity_cmt__SubmittedToOmDate__c"),
                "created_by": order.get("CreatedBy", {}).get("Name") if isinstance(order.get("CreatedBy"), dict) else order.get("CreatedBy"),
                "orchestration_created_by": order.get("vlocity_cmt__OrchestrationPlanId__r", {}).get("CreatedBy", {}).get("Name") if isinstance(order.get("vlocity_cmt__OrchestrationPlanId__r"), dict) else None,
                "product_name": order_item.get("vlocity_cmt__Product2Id__r", {}).get("Name") if isinstance(order_item.get("vlocity_cmt__Product2Id__r"), dict) else order_item.get("vlocity_cmt__Product2Id__r.Name"),
                "product_class": order_item.get("vlocity_cmt__Product2Id__r", {}).get("vlocity_cmt__ParentClassCode__c") if isinstance(order_item.get("vlocity_cmt__Product2Id__r"), dict) else order_item.get("vlocity_cmt__Product2Id__r.vlocity_cmt__ParentClassCode__c"),
                "segment": segment,
                "payment_type": payment_type
            })
        
        return {
            "count": len(orders),
            "orders": orders
        }
    
    def _clean_asset(self, asset: Dict[str, Any], asset_type: str = None) -> Dict[str, Any]:
        """Extract and format asset fields"""
        if not asset:
            return None
        
        product = asset.get("Product2", {})
        modified_by = asset.get("LastModifiedBy", {})
        created_by = asset.get("CreatedBy", {})
        product_class = product.get("vlocity_cmt__ParentClassCode__c")
        
        # Handle nested structures
        if isinstance(modified_by, dict):
            modified_by_name = modified_by.get("Name")
        else:
            modified_by_name = modified_by
        
        if isinstance(created_by, dict):
            created_by_name = created_by.get("Name")
        else:
            created_by_name = created_by
        
        # Base asset structure (same for all)
        cleaned = {
            "id": asset.get("Id"),
            "product_name": product.get("Name"),
            "product_code": product.get("ProductCode"),
            "product_class": product_class,
            "status": asset.get("vlocity_cmt__ProvisioningStatus__c"),
            "asset_reference": asset.get("vlocity_cmt__AssetReferenceId__c"),
            "original_oli_id": asset.get("PR_Original_OLI_ID__c"),
            "charges": {
                "one_time": asset.get("vlocity_cmt__OneTimeCharge__c"),
                "recurring": asset.get("vlocity_cmt__RecurringCharge__c")
            },
            "disconnect_info": {
                "disconnect_date": asset.get("vlocity_cmt__DisconnectDate__c"),
                "disconnect_reason": asset.get("Disconnection_Reason__c")
            },
            "audit": {
                "created_date": asset.get("CreatedDate"),
                "created_by": created_by_name,
                "modified_date": asset.get("LastModifiedDate"),
                "modified_by": modified_by_name
            }
        }
        
        # Add device-specific fields ONLY for device assets (at top level)
        if product_class in ["PR_B2C_Mobile_Device_Class", "PR_B2C_Mobile_BYOD_Device_Class"]:
            cleaned["reporting_string"] = asset.get("PR_Generic_Reporting_String__c")
            cleaned["contract_term"] = asset.get("Contract_Term__c")
            cleaned["installment_id"] = asset.get("PRB2C_Installment_ID__c")
        
        return cleaned