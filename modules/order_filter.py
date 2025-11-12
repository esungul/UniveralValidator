import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class OrderFilter:
    """Filter and group orders with configuration-driven rules"""
    
    def filter_and_group(self, orders: List[Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Any]:
        """
        NEW: Unified method for filtering, grouping, and multiple order detection
        
        Steps:
        1. Filter ignored order reasons
        2. Filter ignored order types
        3. Filter disconnects
        4. Group by MSISDN
        5. Detect multiple orders per MSISDN
        
        Args:
            orders: List of order records from Salesforce
            config: Configuration with filter settings
        
        Returns:
            Dict of {msisdn: order_details with multiple_order_flag}
        """
        logger.info("=" * 80)
        logger.info("[OrderFilter] Starting filter_and_group process")
        logger.info("=" * 80)
        
        filter_config = config.get("order_filter", {})
        ignore_reasons = filter_config.get("ignore_order_reasons", [])
        ignore_types = filter_config.get("ignore_order_types", [])
        
        logger.info(f"[OrderFilter] Ignore reasons: {ignore_reasons}")
        logger.info(f"[OrderFilter] Ignore types: {ignore_types}")
        
        # Step 1: Filter ignored reasons
        logger.info("\n[OrderFilter] Step 1: Filtering ignored order reasons...")
        filtered = self._filter_ignored_reasons(orders, ignore_reasons)
        logger.info(f"  ✓ After reason filter: {len(filtered)} orders (removed {len(orders) - len(filtered)})")
        
        # Step 2: Filter ignored types
        logger.info("\n[OrderFilter] Step 2: Filtering ignored order types...")
        filtered = self._filter_ignored_types(filtered, ignore_types)
        logger.info(f"  ✓ After type filter: {len(filtered)} orders")
        
        # Step 3: Filter disconnects (existing logic)
        logger.info("\n[OrderFilter] Step 3: Filtering disconnect orders...")
        filtered = self.filter_orders(filtered)
        logger.info(f"  ✓ After disconnect filter: {len(filtered)} orders")
        
        # Step 4: Group by MSISDN (existing logic)
        logger.info("\n[OrderFilter] Step 4: Grouping by MSISDN...")
        grouped = self.group_by_msisdn(filtered)
        logger.info(f"  ✓ Grouped into {len(grouped)} unique MSISDNs")
        
        # Step 5: Detect multiple orders per MSISDN
        logger.info("\n[OrderFilter] Step 5: Detecting multiple orders per MSISDN...")
        multiple_count = 0
        msisdns_by_count = {"single": 0, "multiple": 0}
        
        for msisdn in grouped:
            # Count how many original orders map to this MSISDN
            msisdn_orders = [o for o in filtered if o.get("PR_MSISDN__c") == msisdn]
            has_multiple = len(msisdn_orders) > 1
            grouped[msisdn]["has_multiple_orders"] = has_multiple
            grouped[msisdn]["order_count"] = len(msisdn_orders)
            
            if has_multiple:
                multiple_count += 1
                msisdns_by_count["multiple"] += 1
            else:
                msisdns_by_count["single"] += 1
        
        logger.info(f"  ✓ Single order: {msisdns_by_count['single']}")
        logger.info(f"  ✓ Multiple orders: {msisdns_by_count['multiple']}")
        
        logger.info("\n" + "=" * 80)
        logger.info("[OrderFilter] ✓ Filter & Group Complete")
        logger.info("=" * 80)
        
        return grouped
    
    def _filter_ignored_reasons(self, orders: List[Dict[str, Any]], ignore_reasons: List[str]) -> List[Dict[str, Any]]:
        """
        NEW: Filter out orders with ignored order reasons
        FIXED: Handle None values
        """
        if not ignore_reasons:
            return orders
        
        filtered = []
        removed_count = 0
        
        for order in orders:
            reason = order.get("Order", {}).get("vlocity_cmt__Reason__c")
            
            # Handle None values
            if reason is None:
                # Keep orders with None reasons (or skip based on your requirements)
                filtered.append(order)
            elif reason in ignore_reasons:
                removed_count += 1
                logger.debug(f"  [Removed] Order reason '{reason}' in ignore list")
            else:
                filtered.append(order)
        
        if removed_count > 0:
            logger.info(f"[OrderFilter] Removed {removed_count} orders with ignored reasons")
        
        return filtered
    
    def _filter_ignored_types(self, orders: List[Dict[str, Any]], ignore_types: List[str]) -> List[Dict[str, Any]]:
        """
        NEW: Filter out orders with ignored order types
        FIXED: Handle None values
        """
        if not ignore_types:
            return orders
        
        filtered = []
        removed_count = 0
        
        for order in orders:
            order_type = order.get("Order", {}).get("Type")
            
            # Handle None values
            if order_type is None:
                # Keep orders with None types (or skip based on your requirements)
                filtered.append(order)
            elif order_type in ignore_types:
                removed_count += 1
                logger.debug(f"  [Removed] Order type '{order_type}' in ignore list")
            else:
                filtered.append(order)
        
        if removed_count > 0:
            logger.info(f"[OrderFilter] Removed {removed_count} orders with ignored types")
        
        return filtered
    
    def filter_orders(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        EXISTING METHOD: Filter out disconnect orders
        FIXED: Handle None values for reason field
        """
        filtered_orders = []
        
        for order in orders:
            order_obj = order.get("Order", {})
            reason = order_obj.get("vlocity_cmt__Reason__c", "")
            
            # Handle None values safely
            if reason is None:
                reason = ""
            else:
                reason = str(reason).lower()
            
            # Skip disconnect orders
            if "disconnect" in reason:
                logger.debug(f"Skipping disconnect order: {reason}")
                continue
            
            filtered_orders.append(order)
        
        return filtered_orders
    def group_by_msisdn(self, orders: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        EXISTING METHOD: Group orders by MSISDN, keep latest
        """
        grouped = {}
        
        for order in orders:
            msisdn = order.get("PR_MSISDN__c")
            
            if not msisdn:
                continue
            
            # Keep the latest order for each MSISDN
            if msisdn not in grouped:
                grouped[msisdn] = order
            else:
                existing_date = grouped[msisdn].get("CreatedDate", "")
                new_date = order.get("CreatedDate", "")
                
                if new_date > existing_date:
                    grouped[msisdn] = order
        
        return grouped