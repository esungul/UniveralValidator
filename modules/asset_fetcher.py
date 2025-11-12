import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class AssetFetcher:
    """Fetch and organize assets from Salesforce using 2-query approach"""
    
    def __init__(self, sf_connection):
        self.sf = sf_connection
    
    def get_assets_for_msisdn_v2(self, msisdn: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        3-Query approach:
        1. Get LATEST line
        2. Get DEVICE via asset reference link
        3. Get DEVICE children
        """
        try:
            logger.info(f"\n{'='*80}")
            logger.info(f"[AssetFetcher] 3-Query Approach for MSISDN: {msisdn}")
            logger.info(f"{'='*80}\n")
            
            # Query 1: Get latest line
            logger.info(f"[Query 1] Fetching LATEST line for MSISDN...")
            latest_line = self._get_latest_line(msisdn, config)
            
            if not latest_line:
                logger.warning(f"[AssetFetcher] ✗ No active line found for MSISDN: {msisdn}")
                return {
                    "latest_line": None,
                    "device": None,
                    "line_children": [],
                    "device_children": [],
                    "error": "No active line found"
                }
            
            logger.info(f"[AssetFetcher] ✓ Found latest line: {latest_line.get('Product2', {}).get('Name')}")
            line_asset_ref = latest_line.get('vlocity_cmt__AssetReferenceId__c')
            
            # Query 2: Get DEVICE via asset reference
            logger.info(f"[Query 2] Fetching DEVICE via asset reference: {line_asset_ref}...")
            device = self._get_device_for_line(msisdn, line_asset_ref, config)
            
            if not device:
                logger.error(f"[AssetFetcher] ✗ No device found for line asset reference: {line_asset_ref}")
                return {
                    "latest_line": latest_line,
                    "device": None,
                    "line_children": [],
                    "device_children": [],
                    "error": "No device found (mandatory)"
                }
            
            logger.info(f"[AssetFetcher] ✓ Found device: {device.get('Product2', {}).get('Name')}")
            device_root_id = device.get('vlocity_cmt__RootItemId__c')
            
            # Query 2b: Get LINE children (using line's root item)
            line_root_id = latest_line.get('vlocity_cmt__RootItemId__c')
            logger.info(f"[Query 2b] Fetching LINE children...")
            line_children = self._get_line_children(msisdn, line_root_id, config)
            
            # Query 3: Get DEVICE children
            logger.info(f"[Query 3] Fetching DEVICE children...")
            device_children = self._get_device_children(device_root_id, config)
            
            logger.info(f"[AssetFetcher] ✓ Found {len(line_children)} line children")
            logger.info(f"[AssetFetcher] ✓ Found {len(device_children)} device children\n")
            
            return {
                "latest_line": latest_line,
                "device": device,
                "line_children": line_children,
                "device_children": device_children,
                "error": None
            }
            
        except Exception as e:
            logger.error(f"[AssetFetcher] ✗ Error in 3-query approach: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def _get_device_for_line(self, msisdn: str, asset_reference_id: str, 
                             config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Query 2: Get DEVICE linked to LINE via asset reference"""
        try:
            query_config = config.get("salesforce_queries", {}).get("device_for_line", {})
            fields = query_config.get("fields", [])
            from_table = query_config.get("from_table", "Asset")
            where_conditions = query_config.get("where_conditions", [])
            
            # Replace placeholders
            where_conditions = [w.replace("{msisdn}", msisdn) for w in where_conditions]
            where_conditions = [w.replace("{asset_reference_id}", asset_reference_id) for w in where_conditions]
            
            soql = f"SELECT {', '.join(fields)} FROM {from_table}"
            if where_conditions:
                soql += " WHERE " + " AND ".join(where_conditions)
            
            # Log the query
            logger.info(f"\n{'='*80}")
            logger.info(f"[AssetFetcher] QUERY 2: Get Device via Asset Reference")
            logger.info(f"{'='*80}")
            logger.info(f"Query: {soql}")
            logger.info(f"{'='*80}\n")
            
            result = self.sf.query_all(soql)
            records = result.get("records", [])
            
            if records:
                logger.info(f"[AssetFetcher] ✓ Query 2 returned 1 device\n")
                return records[0]
            else:
                logger.warning(f"[AssetFetcher] No device found")
                return None
                
        except Exception as e:
            logger.error(f"[AssetFetcher] ✗ Error in Query 2: {e}")
            raise
    
    def _get_line_children(self, msisdn: str, line_root_id: str, 
                          config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Query 2b: Get all children of LINE"""
        try:
            query_config = config.get("salesforce_queries", {}).get("children_for_root_item", {})
            fields = query_config.get("fields", [])
            from_table = query_config.get("from_table", "Asset")
            where_conditions = query_config.get("where_conditions", [])
            
            # Replace placeholders
            where_conditions = [w.replace("{msisdn}", msisdn) for w in where_conditions]
            where_conditions = [w.replace("{root_item_id}", line_root_id) for w in where_conditions]
            
            soql = f"SELECT {', '.join(fields)} FROM {from_table}"
            if where_conditions:
                soql += " WHERE " + " AND ".join(where_conditions)
            
            logger.info(f"{'='*80}")
            logger.info(f"[AssetFetcher] QUERY 2b: Get Line Children")
            logger.info(f"{'='*80}")
            logger.info(f"Query: {soql}")
            logger.info(f"{'='*80}\n")
            
            result = self.sf.query_all(soql)
            records = result.get("records", [])
            
            logger.info(f"[AssetFetcher] ✓ Query 2b returned {len(records)} line children\n")
            
            if records:
                logger.info(f"[AssetFetcher] Line Children Summary:")
                for i, child in enumerate(records, 1):
                    product = child.get("Product2", {})
                    status = child.get("vlocity_cmt__ProvisioningStatus__c")
                    product_name = product.get("Name", "Unknown")
                    logger.info(f"  {i}. {product_name} - Status: {status}")
                logger.info("")
            
            return records
            
        except Exception as e:
            logger.error(f"[AssetFetcher] ✗ Error in Query 2b: {e}")
            raise
    
    def _get_device_children(self, device_root_id: str, 
                           config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Query 3: Get all children of DEVICE"""
        try:
            query_config = config.get("salesforce_queries", {}).get("device_children", {})
            fields = query_config.get("fields", [])
            from_table = query_config.get("from_table", "Asset")
            where_conditions = query_config.get("where_conditions", [])
            
            # Replace placeholders
            where_conditions = [w.replace("{device_root_id}", device_root_id) for w in where_conditions]
            
            soql = f"SELECT {', '.join(fields)} FROM {from_table}"
            if where_conditions:
                soql += " WHERE " + " AND ".join(where_conditions)
            
            logger.info(f"{'='*80}")
            logger.info(f"[AssetFetcher] QUERY 3: Get Device Children")
            logger.info(f"{'='*80}")
            logger.info(f"Query: {soql}")
            logger.info(f"{'='*80}\n")
            
            result = self.sf.query_all(soql)
            records = result.get("records", [])
            
            logger.info(f"[AssetFetcher] ✓ Query 3 returned {len(records)} device children\n")
            
            if records:
                logger.info(f"[AssetFetcher] Device Children Summary:")
                for i, child in enumerate(records, 1):
                    product = child.get("Product2", {})
                    status = child.get("vlocity_cmt__ProvisioningStatus__c")
                    product_name = product.get("Name", "Unknown")
                    logger.info(f"  {i}. {product_name} - Status: {status}")
                logger.info("")
            
            return records
            
        except Exception as e:
            logger.error(f"[AssetFetcher] ✗ Error in Query 3: {e}")
            raise
    
    def _get_latest_line(self, msisdn: str, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Query 1: Get the latest mobile line for MSISDN"""
        try:
            query_config = config.get("salesforce_queries", {}).get("latest_line_for_msisdn", {})
            fields = query_config.get("fields", [])
            from_table = query_config.get("from_table", "Asset")
            where_conditions = query_config.get("where_conditions", [])
            
            # Replace {msisdn} placeholder
            where_conditions = [w.replace("{msisdn}", msisdn) for w in where_conditions]
            
            soql = f"SELECT {', '.join(fields)} FROM {from_table}"
            if where_conditions:
                soql += " WHERE " + " AND ".join(where_conditions)
            
            soql += " ORDER BY CreatedDate DESC LIMIT 1"
            
            # Log the query
            logger.info(f"\n{'='*80}")
            logger.info(f"[AssetFetcher] QUERY 1: Get Latest Line")
            logger.info(f"{'='*80}")
            logger.info(f"Query: {soql}")
            logger.info(f"{'='*80}\n")
            
            result = self.sf.query_all(soql)
            records = result.get("records", [])
            
            if records:
                logger.info(f"[AssetFetcher] ✓ Query 1 returned 1 line")
                return records[0]
            else:
                logger.warning(f"[AssetFetcher] No lines found")
                return None
                
        except Exception as e:
            logger.error(f"[AssetFetcher] ✗ Error in Query 1: {e}")
            raise
    
    def _get_children_for_root_item(self, msisdn: str, root_item_id: str, 
                                     config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Query 2: Get all children of a specific root item"""
        try:
            query_config = config.get("salesforce_queries", {}).get("children_for_root_item", {})
            fields = query_config.get("fields", [])
            from_table = query_config.get("from_table", "Asset")
            where_conditions = query_config.get("where_conditions", [])
            
            # Replace {msisdn} and {root_item_id} placeholders
            where_conditions = [w.replace("{msisdn}", msisdn) for w in where_conditions]
            where_conditions = [w.replace("{root_item_id}", root_item_id) for w in where_conditions]
            
            soql = f"SELECT {', '.join(fields)} FROM {from_table}"
            if where_conditions:
                soql += " WHERE " + " AND ".join(where_conditions)
            
            # Log the query
            logger.info(f"{'='*80}")
            logger.info(f"[AssetFetcher] QUERY 2: Get Children of Root Item")
            logger.info(f"{'='*80}")
            logger.info(f"Query: {soql}")
            logger.info(f"{'='*80}\n")
            
            result = self.sf.query_all(soql)
            records = result.get("records", [])
            
            logger.info(f"[AssetFetcher] ✓ Query 2 returned {len(records)} children\n")
            
            # Log summary
            if records:
                logger.info(f"[AssetFetcher] Children Summary:")
                for i, child in enumerate(records, 1):
                    product = child.get("Product2", {})
                    status = child.get("vlocity_cmt__ProvisioningStatus__c")
                    product_name = product.get("Name", "Unknown")
                    logger.info(f"  {i}. {product_name} - Status: {status}")
                logger.info("")
            
            return records
            
        except Exception as e:
            logger.error(f"[AssetFetcher] ✗ Error in Query 2: {e}")
            raise
    
    def _organize_children(self, children: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Organize children by type (devices, add-ons, disconnected)"""
        devices = []
        add_ons = []
        disconnected = []
        
        for child in children:
            product_class = child.get("Product2", {}).get("vlocity_cmt__ParentClassCode__c", "")
            status = child.get("vlocity_cmt__ProvisioningStatus__c")
            
            # Check if disconnected
            if status in ["Deleted", "Disconnected"]:
                disconnected.append(child)
            # Check if device
            elif product_class in ["PR_B2C_Mobile_Device_Class", "PR_B2C_Mobile_BYOD_Device_Class"]:
                devices.append(child)
            # Otherwise it's an add-on or other
            else:
                add_ons.append(child)
        
        logger.info(f"[AssetFetcher] Organized: {len(devices)} devices, {len(add_ons)} add-ons, {len(disconnected)} disconnected")
        
        return {
            "devices": devices,
            "add_ons": add_ons,
            "disconnected": disconnected
        }
    
    def get_order_history(self, msisdn: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get last 3 orders for MSISDN with reason and status"""
        try:
            query_config = config.get("salesforce_queries", {}).get("order_history_for_msisdn", {})
            fields = query_config.get("fields", [])
            from_table = query_config.get("from_table", "OrderItem")
            where_conditions = query_config.get("where_conditions", [])
            order_by = query_config.get("order_by", "")
            limit = query_config.get("limit", 3)
            
            # Replace {msisdn} placeholder
            where_conditions = [w.replace("{msisdn}", msisdn) for w in where_conditions]
            
            soql = f"SELECT {', '.join(fields)} FROM {from_table}"
            if where_conditions:
                soql += " WHERE " + " AND ".join(where_conditions)
            if order_by:
                soql += f" ORDER BY {order_by}"
            if limit:
                soql += f" LIMIT {limit}"
            
            logger.info(f"\n{'='*80}")
            logger.info(f"[AssetFetcher] ORDER HISTORY: Get last 3 orders")
            logger.info(f"{'='*80}")
            logger.info(f"Query: {soql}")
            logger.info(f"{'='*80}\n")
            
            result = self.sf.query_all(soql)
            records = result.get("records", [])
            
            logger.info(f"[AssetFetcher] ✓ Found {len(records)} orders\n")
            
            if records:
                logger.info(f"[AssetFetcher] Order History:")
                for i, order in enumerate(records, 1):
                    order_obj = order.get("Order", {})
                    reason = order_obj.get("vlocity_cmt__Reason__c", "N/A")
                    status = order_obj.get("vlocity_cmt__OrderStatus__c", "N/A")
                    created = order_obj.get("CreatedDate", "N/A")
                    logger.info(f"  {i}. Reason: {reason} | Status: {status} | Created: {created}")
                logger.info("")
            
            return records
            
        except Exception as e:
            logger.error(f"[AssetFetcher] ✗ Error fetching order history: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []