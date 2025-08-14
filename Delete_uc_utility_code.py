# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Delete Utility
# MAGIC
# MAGIC This utility provides functions to delete Unity Catalog tables and views using service principal authentication.
# MAGIC It can be imported into other notebooks using `%run` command.
# MAGIC
# MAGIC **Usage from other notebooks:**
# MAGIC ```python
# MAGIC %run "/path/to/this/notebook"
# MAGIC
# MAGIC # Delete specific objects
# MAGIC result = delete_uc_objects(
# MAGIC     catalog="your_catalog",
# MAGIC     schema="your_schema", 
# MAGIC     objects=["table1", "view1"],
# MAGIC     dry_run=True
# MAGIC )
# MAGIC
# MAGIC # Delete objects by pattern
# MAGIC result = delete_uc_objects_by_pattern(
# MAGIC     catalog="your_catalog",
# MAGIC     schema="your_schema",
# MAGIC     prefix="temp_",
# MAGIC     object_types=["table", "view"],
# MAGIC     dry_run=False
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Configuration Required:**
# MAGIC - Update secret scope name in the configuration section below
# MAGIC - Ensure service principal has required Unity Catalog permissions
# MAGIC - Configure secrets: service-principal-tenant-id, service-principal-client-id, service-principal-client-secret

# COMMAND ----------

import os
import requests
import json
from azure.identity import ClientSecretCredential
from typing import List, Optional, Literal, Dict, Any

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC 
# MAGIC **IMPORTANT**: Update the secret scope name below to match your environment

# COMMAND ----------

# TODO: Update this with your actual secret scope name
SECRET_SCOPE = "your-keyvault-scope"

# Secret key names (update these if your keys have different names)
TENANT_ID_KEY = "service-principal-tenant-id"
CLIENT_ID_KEY = "service-principal-client-id"
CLIENT_SECRET_KEY = "service-principal-client-secret"

# Databricks resource ID (standard - usually no need to change)
DATABRICKS_RESOURCE_ID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Utility Functions

# COMMAND ----------

def _get_service_principal_token() -> str:
    """Get Azure AD token for the configured service principal"""
    try:
        tenant_id = dbutils.secrets.get(SECRET_SCOPE, TENANT_ID_KEY)
        client_id = dbutils.secrets.get(SECRET_SCOPE, CLIENT_ID_KEY)
        client_secret = dbutils.secrets.get(SECRET_SCOPE, CLIENT_SECRET_KEY)
        
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        token = credential.get_token(DATABRICKS_RESOURCE_ID)
        
        return token.token
    except Exception as e:
        raise Exception(f"Failed to get service principal token: {e}")

def _get_sql_warehouse() -> str:
    """Get an available SQL warehouse ID"""
    try:
        token = _get_service_principal_token()
        databricks_host = os.getenv("Host_Name")
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        warehouses_url = f"{databricks_host}/api/2.0/sql/warehouses"
        response = requests.get(warehouses_url, headers=headers)
        response.raise_for_status()
        
        warehouses = response.json().get("warehouses", [])
        if not warehouses:
            raise Exception("No SQL warehouses available")
        
        # Prefer running warehouses, otherwise use first available
        for warehouse in warehouses:
            if warehouse.get("state") == "RUNNING":
                return warehouse["id"]
        
        return warehouses[0]["id"]
        
    except Exception as e:
        raise Exception(f"Failed to get SQL warehouse: {e}")

def _execute_sql_via_warehouse(sql: str) -> Dict[str, Any]:
    """Execute SQL statement via SQL warehouse using service principal"""
    try:
        token = _get_service_principal_token()
        warehouse_id = _get_sql_warehouse()
        databricks_host = os.getenv("Host_Name")
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        sql_url = f"{databricks_host}/api/2.0/sql/statements"
        payload = {
            "warehouse_id": warehouse_id,
            "statement": sql,
            "wait_timeout": "60s"
        }
        
        response = requests.post(sql_url, headers=headers, json=payload)
        response.raise_for_status()
        
        result = response.json()
        if result.get("status", {}).get("state") == "SUCCEEDED":
            return {"success": True, "result": result}
        else:
            return {"success": False, "error": result}
            
    except Exception as e:
        return {"success": False, "error": str(e)}

def _list_objects_via_warehouse(catalog: str, schema: str, object_type: str) -> List[str]:
    """List tables or views using SQL warehouse (alternative to Spark)"""
    try:
        if object_type.lower() == "table":
            sql = f"SHOW TABLES IN `{catalog}`.`{schema}`"
        elif object_type.lower() == "view":
            sql = f"SHOW VIEWS IN `{catalog}`.`{schema}`"
        else:
            raise ValueError("object_type must be 'table' or 'view'")
        
        result = _execute_sql_via_warehouse(sql)
        if result["success"]:
            # Extract object names from the result
            data = result["result"].get("result", {}).get("data_array", [])
            if object_type.lower() == "table":
                # SHOW TABLES returns columns: database, tableName, isTemporary
                return [row[1] for row in data if len(row) > 1]
            else:
                # SHOW VIEWS returns columns: database, viewName, isTemporary  
                return [row[1] for row in data if len(row) > 1]
        else:
            print(f"Error listing {object_type}s via warehouse: {result.get('error')}")
            return []
    except Exception as e:
        print(f"Error listing {object_type}s via warehouse: {e}")
        return []

def _validate_catalog_schema(catalog: str, schema: str) -> bool:
    """Validate that catalog and schema exist and are accessible"""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        
        if spark is not None:
            # Use Spark if available
            catalogs_df = spark.sql("SHOW CATALOGS")
            catalogs = [row.catalog for row in catalogs_df.collect()]
            if catalog not in catalogs:
                raise Exception(f"Catalog '{catalog}' not found. Available: {catalogs}")
            
            schemas_df = spark.sql(f"SHOW SCHEMAS IN `{catalog}`")
            schemas = [row.databaseName for row in schemas_df.collect()]
            if schema not in schemas:
                raise Exception(f"Schema '{schema}' not found in catalog '{catalog}'. Available: {schemas}")
        else:
            # Use warehouse method as fallback
            catalog_result = _execute_sql_via_warehouse("SHOW CATALOGS")
            if not catalog_result["success"]:
                raise Exception(f"Failed to validate catalog: {catalog_result.get('error')}")
            
            schema_result = _execute_sql_via_warehouse(f"SHOW SCHEMAS IN `{catalog}`")
            if not schema_result["success"]:
                raise Exception(f"Failed to validate schema: {schema_result.get('error')}")
        
        return True
    except Exception as e:
        print(f"Validation error: {e}")
        return False

def _list_objects(catalog: str, schema: str, object_type: str) -> List[str]:
    """List tables or views in a schema (tries Spark first, then warehouse)"""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is not None:
            if object_type.lower() == "table":
                df = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`")
                return [row.tableName for row in df.collect()]
            elif object_type.lower() == "view":
                df = spark.sql(f"SHOW VIEWS IN `{catalog}`.`{schema}`")
                return [row.viewName for row in df.collect()]
            else:
                raise ValueError("object_type must be 'table' or 'view'")
        else:
            # Fallback to warehouse method
            return _list_objects_via_warehouse(catalog, schema, object_type)
    except Exception as e:
        print(f"Error listing {object_type}s via Spark, trying warehouse: {e}")
        # Fallback to warehouse method
        return _list_objects_via_warehouse(catalog, schema, object_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Utility Functions

# COMMAND ----------

def delete_uc_objects(
    catalog: str,
    schema: str,
    objects: List[str],
    *,
    object_types: Optional[List[Literal["table", "view"]]] = None,
    dry_run: bool = True,
    purge_tables: bool = False
) -> Dict[str, Any]:
    """
    Delete specific Unity Catalog objects using service principal authentication.
    
    Args:
        catalog: Target catalog name
        schema: Target schema name  
        objects: List of object names to delete
        object_types: List of object types to consider ["table", "view"]. If None, auto-detect.
        dry_run: If True, show what would be deleted without executing (default: True)
        purge_tables: If True, add PURGE option for table deletions (default: False)
        
    Returns:
        Dict with execution results: {"success": bool, "executed": List, "errors": List, "dry_run": bool}
        
    Example:
        result = delete_uc_objects(
            catalog="my_catalog",
            schema="my_schema",
            objects=["table1", "view1"],
            dry_run=True
        )
    """
    
    print(f"=== DELETE UC OBJECTS ===")
    print(f"Catalog: {catalog}, Schema: {schema}")
    print(f"Objects: {objects}")
    print(f"Dry run: {dry_run}")
    
    # Validate inputs
    if not _validate_catalog_schema(catalog, schema):
        return {"success": False, "error": "Invalid catalog or schema"}
    
    if not objects:
        return {"success": False, "error": "No objects specified"}
    
    # Auto-detect object types if not specified
    if object_types is None:
        object_types = ["table", "view"]
    
    executed = []
    errors = []
    
    for obj_name in objects:
        # Determine object type
        obj_type = None
        for otype in object_types:
            existing_objects = _list_objects(catalog, schema, otype)
            if obj_name in existing_objects:
                obj_type = otype
                break
        
        if obj_type is None:
            errors.append({
                "object": f"`{catalog}`.`{schema}`.`{obj_name}`",
                "error": f"Object not found or not a {'/'.join(object_types)}"
            })
            continue
        
        # Prepare SQL
        fq_name = f"`{catalog}`.`{schema}`.`{obj_name}`"
        drop_sql = f"DROP {obj_type.upper()} IF EXISTS {fq_name}"
        if purge_tables and obj_type == "table":
            drop_sql += " PURGE"
        
        if dry_run:
            print(f"DRY RUN: {drop_sql}")
            executed.append({"object": fq_name, "type": obj_type, "status": "dry_run", "sql": drop_sql})
        else:
            print(f"Executing: {drop_sql}")
            result = _execute_sql_via_warehouse(drop_sql)
            
            if result["success"]:
                executed.append({"object": fq_name, "type": obj_type, "status": "deleted", "sql": drop_sql})
                print(f"✅ Successfully deleted {obj_type}: {fq_name}")
            else:
                errors.append({"object": fq_name, "type": obj_type, "error": result["error"], "sql": drop_sql})
                print(f"❌ Failed to delete {obj_type}: {fq_name} - {result['error']}")
    
    # Summary
    if not dry_run:
        print(f"\n=== SUMMARY ===")
        print(f"Successfully deleted: {len(executed)} objects")
        print(f"Errors: {len(errors)} objects")
    
    return {
        "success": len(errors) == 0,
        "executed": executed,
        "errors": errors,
        "dry_run": dry_run
    }

def delete_uc_objects_by_pattern(
    catalog: str,
    schema: str,
    *,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
    contains: Optional[str] = None,
    object_types: List[Literal["table", "view"]] = ["table", "view"],
    dry_run: bool = True,
    purge_tables: bool = False
) -> Dict[str, Any]:
    """
    Delete Unity Catalog objects matching a pattern using service principal authentication.
    
    Args:
        catalog: Target catalog name
        schema: Target schema name
        prefix: Filter objects starting with this string (optional)
        suffix: Filter objects ending with this string (optional)
        contains: Filter objects containing this string (optional)
        object_types: Types of objects to consider ["table", "view"] (default: both)
        dry_run: If True, show what would be deleted without executing (default: True)
        purge_tables: If True, add PURGE option for table deletions (default: False)
        
    Returns:
        Dict with execution results: {"success": bool, "executed": List, "errors": List, "dry_run": bool}
        
    Example:
        result = delete_uc_objects_by_pattern(
            catalog="my_catalog",
            schema="my_schema",
            prefix="temp_",
            object_types=["table"],
            dry_run=True
        )
    """
    
    print(f"=== DELETE UC OBJECTS BY PATTERN ===")
    print(f"Catalog: {catalog}, Schema: {schema}")
    print(f"Prefix: {prefix}, Suffix: {suffix}, Contains: {contains}")
    print(f"Object types: {object_types}")
    print(f"Dry run: {dry_run}")
    
    # Validate inputs
    if not _validate_catalog_schema(catalog, schema):
        return {"success": False, "error": "Invalid catalog or schema"}
    
    if not any([prefix, suffix, contains]):
        return {"success": False, "error": "At least one filter (prefix, suffix, contains) must be specified"}
    
    # Get all objects matching pattern
    matching_objects = []
    
    for obj_type in object_types:
        objects = _list_objects(catalog, schema, obj_type)
        
        for obj_name in objects:
            matches = True
            
            if prefix and not obj_name.startswith(prefix):
                matches = False
            if suffix and not obj_name.endswith(suffix):
                matches = False
            if contains and contains not in obj_name:
                matches = False
            
            if matches:
                matching_objects.append(obj_name)
    
    # Remove duplicates while preserving order
    matching_objects = list(dict.fromkeys(matching_objects))
    
    print(f"Found {len(matching_objects)} objects matching pattern: {matching_objects}")
    
    if not matching_objects:
        return {"success": True, "executed": [], "errors": [], "dry_run": dry_run, "message": "No objects found matching pattern"}
    
    # Delete the matching objects
    return delete_uc_objects(
        catalog=catalog,
        schema=schema,
        objects=matching_objects,
        object_types=object_types,
        dry_run=dry_run,
        purge_tables=purge_tables
    )

def list_uc_objects(
    catalog: str, 
    schema: str, 
    object_types: List[Literal["table", "view"]] = ["table", "view"]
) -> Dict[str, List[str]]:
    """
    List Unity Catalog objects in a schema.
    
    Args:
        catalog: Target catalog name
        schema: Target schema name
        object_types: Types of objects to list ["table", "view"] (default: both)
        
    Returns:
        Dict with object lists: {"tables": List[str], "views": List[str]}
        
    Example:
        objects = list_uc_objects("my_catalog", "my_schema", ["view"])
        # Returns: {"views": ["view1", "view2"]}
    """
    
    if not _validate_catalog_schema(catalog, schema):
        return {"tables": [], "views": []}
    
    result = {}
    
    for obj_type in object_types:
        objects = _list_objects(catalog, schema, obj_type)
        result[f"{obj_type}s"] = objects
        print(f"Found {len(objects)} {obj_type}s in {catalog}.{schema}")
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions for Testing and Debugging

# COMMAND ----------

def test_service_principal_configuration() -> Dict[str, bool]:
    """
    Test service principal configuration and connectivity.
    
    Returns:
        Dict with test results for each component
    """
    
    print("=== TESTING SERVICE PRINCIPAL CONFIGURATION ===")
    results = {}
    
    # Test 1: Token generation
    print("1. Testing service principal token generation...")
    try:
        token = _get_service_principal_token()
        print("   ✅ Service principal token generated successfully")
        results["token_generation"] = True
    except Exception as e:
        print(f"   ❌ Token generation failed: {e}")
        results["token_generation"] = False
        return results  # Can't continue without token
    
    # Test 2: SQL Warehouse access
    print("2. Testing SQL warehouse access...")
    try:
        warehouse_id = _get_sql_warehouse()
        print(f"   ✅ SQL warehouse available: {warehouse_id}")
        results["warehouse_access"] = True
    except Exception as e:
        print(f"   ❌ SQL warehouse access failed: {e}")
        results["warehouse_access"] = False
        return results  # Can't continue without warehouse
    
    # Test 3: Basic SQL execution
    print("3. Testing basic SQL execution...")
    try:
        result = _execute_sql_via_warehouse("SELECT 1 as test")
        if result["success"]:
            print("   ✅ SQL execution successful")
            results["sql_execution"] = True
        else:
            print(f"   ❌ SQL execution failed: {result['error']}")
            results["sql_execution"] = False
    except Exception as e:
        print(f"   ❌ SQL execution failed: {e}")
        results["sql_execution"] = False
    
    # Test 4: Catalog access
    print("4. Testing catalog access...")
    try:
        catalog_result = _execute_sql_via_warehouse("SHOW CATALOGS")
        if catalog_result["success"]:
            print("   ✅ Catalog access successful")
            results["catalog_access"] = True
        else:
            print(f"   ❌ Catalog access failed: {catalog_result['error']}")
            results["catalog_access"] = False
    except Exception as e:
        print(f"   ❌ Catalog access failed: {e}")
        results["catalog_access"] = False
    
    print(f"\n=== TEST SUMMARY ===")
    passed = sum(results.values())
    total = len(results)
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed! Service principal is configured correctly.")
    else:
        print("⚠️  Some tests failed. Check configuration and permissions.")
    
    return results

def get_configuration_status() -> Dict[str, str]:
    """
    Get current configuration status and settings.
    
    Returns:
        Dict with configuration information
    """
    
    config = {
        "secret_scope": SECRET_SCOPE,
        "tenant_id_key": TENANT_ID_KEY,
        "client_id_key": CLIENT_ID_KEY,
        "client_secret_key": CLIENT_SECRET_KEY,
        "databricks_resource_id": DATABRICKS_RESOURCE_ID
    }
    
    print("=== CURRENT CONFIGURATION ===")
    for key, value in config.items():
        print(f"{key}: {value}")
    
    # Test secret access
    print("\n=== SECRET ACCESS TEST ===")
    try:
        # Try to access secrets (don't print values)
        dbutils.secrets.get(SECRET_SCOPE, TENANT_ID_KEY)
        print("✅ Tenant ID secret accessible")
    except Exception as e:
        print(f"❌ Tenant ID secret error: {e}")
    
    try:
        dbutils.secrets.get(SECRET_SCOPE, CLIENT_ID_KEY)
        print("✅ Client ID secret accessible")
    except Exception as e:
        print(f"❌ Client ID secret error: {e}")
    
    try:
        dbutils.secrets.get(SECRET_SCOPE, CLIENT_SECRET_KEY)
        print("✅ Client Secret accessible")
    except Exception as e:
        print(f"❌ Client Secret error: {e}")
    
    return config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples and Quick Start
# MAGIC 
# MAGIC Run the cell below to see usage examples and verify the utility is working.

# COMMAND ----------

def show_usage_examples():
    """Display usage examples and perform basic connectivity test"""
    
    print("=== UNITY CATALOG DELETE UTILITY ===")
    print("Version: 1.2")
    print("Documentation: See wiki for complete guide")
    print()
    
    print("=== QUICK CONNECTIVITY TEST ===")
    test_results = test_service_principal_configuration()
    
    if all(test_results.values()):
        print("\n✅ Service principal configuration is working!")
        print("\n=== USAGE EXAMPLES ===")
        print()
        
        print("1. List objects in a schema:")
        print("   objects = list_uc_objects('catalog', 'schema', ['table', 'view'])")
        print()
        
        print("2. Delete specific objects (dry run):")
        print("   result = delete_uc_objects(")
        print("       catalog='my_catalog',")
        print("       schema='my_schema',")
        print("       objects=['table1', 'view1'],")
        print("       dry_run=True")
        print("   )")
        print()
        
        print("3. Delete objects by pattern:")
        print("   result = delete_uc_objects_by_pattern(")
        print("       catalog='my_catalog',")
        print("       schema='my_schema',")
        print("       prefix='temp_',")
        print("       object_types=['table'],")
        print("       dry_run=True")
        print("   )")
        print()
        
        print("4. Actual deletion (remove dry_run or set to False):")
        print("   result = delete_uc_objects(..., dry_run=False)")
        print()
        
        print("🚀 Ready to use! Start with list_uc_objects() to explore your data.")
    else:
        print("\n❌ Configuration issues detected!")
        print("Please check:")
        print("1. Update SECRET_SCOPE variable with your actual secret scope name")
        print("2. Ensure secrets exist in the scope with correct key names")
        print("3. Verify service principal has required permissions")
        print("4. Check SQL warehouse access for the service principal")

# Run the examples and test
show_usage_examples()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Important Notes
# MAGIC 
# MAGIC ### ⚠️ Before Using This Utility:
# MAGIC 
# MAGIC 1. **Update Configuration**: Modify the `SECRET_SCOPE` variable in the configuration section
# MAGIC 2. **Configure Secrets**: Ensure your secret scope contains the required service principal credentials
# MAGIC 3. **Test Configuration**: Run `test_service_principal_configuration()` to verify setup
# MAGIC 4. **Verify Permissions**: Ensure service principal has MANAGE permissions on target objects
# MAGIC 
# MAGIC ### 🛡️ Safety Reminders:
# MAGIC 
# MAGIC - **Always use `dry_run=True` first** to preview deletions
# MAGIC - **Verify object lists** with `list_uc_objects()` before deletion
# MAGIC - **Test with non-critical objects** before production use
# MAGIC - **Backup important data** before deleting tables
# MAGIC 
# MAGIC ### 📚 Documentation:
# MAGIC 
# MAGIC For complete documentation, troubleshooting, and best practices, refer to the Unity Catalog Delete Utility wiki.