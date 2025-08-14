# Unity Catalog Delete Utility

## Overview

The Unity Catalog Delete Utility is a Databricks notebook utility that provides safe and efficient deletion of Unity Catalog tables and views using service principal authentication. It offers both individual object deletion and pattern-based bulk deletion with comprehensive safety features.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [API Reference](#api-reference)
- [Safety Features](#safety-features)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

## Features

✅ **Service Principal Authentication** - Secure deletion using service principal credentials  
✅ **Dry Run Support** - Preview deletions before execution  
✅ **Pattern Matching** - Delete objects by prefix, suffix, or contains filters  
✅ **Auto-Detection** - Automatically identifies table vs view types  
✅ **Dual Execution Methods** - Uses Spark SQL or SQL Warehouse API  
✅ **Comprehensive Error Handling** - Detailed error reporting and recovery  
✅ **Bulk Operations** - Delete multiple objects efficiently  
✅ **Safe Defaults** - Dry run enabled by default  

## Prerequisites

### Required Permissions
- **Service Principal** configured in Databricks with:
  - `USAGE` permission on target catalogs
  - `USAGE` permission on target schemas  
  - `MANAGE` or `OWN` permission on objects to be deleted
- **SQL Warehouse** access for the service principal
- **Key Vault** access for retrieving service principal credentials

### Required Secrets
The utility requires these secrets to be configured in Databricks Secret Scope:

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `service-principal-tenant-id` | Azure AD Tenant ID | `12345678-1234-5678-9abc-123456789012` |
| `service-principal-client-id` | Service Principal Application ID | `87654321-4321-8765-cba9-987654321098` |
| `service-principal-client-secret` | Service Principal Secret | `your-secret-value` |

### Environment Variables
- `Host_Name` - Databricks workspace URL (automatically available in notebooks)

## Installation

### Step 1: Create the Utility Notebook

1. Create a new Databricks notebook named `uc_delete_utility`
2. Copy the [utility code](#utility-code) into the notebook
3. Save the notebook in a shared location (e.g., `/Shared/utilities/`)

### Step 2: Configure Secret Scope

```python
# Create secret scope (run once)
databricks secrets create-scope --scope "your-keyvault-scope"

# Add required secrets
databricks secrets put --scope "your-keyvault-scope" --key "service-principal-tenant-id"
databricks secrets put --scope "your-keyvault-scope" --key "service-principal-client-id" 
databricks secrets put --scope "your-keyvault-scope" --key "service-principal-client-secret"
```

### Step 3: Update Configuration

In the utility notebook, update the secret scope name:

```python
# Update this line with your secret scope name
tenant_id = dbutils.secrets.get('your-keyvault-scope', 'service-principal-tenant-id')
client_id = dbutils.secrets.get("your-keyvault-scope", "service-principal-client-id")
client_secret = dbutils.secrets.get("your-keyvault-scope", "service-principal-client-secret")
```

## Configuration

### Service Principal Setup

1. **Create Service Principal** in Azure AD
2. **Grant Permissions** in Databricks:
   ```sql
   -- Grant usage on catalog
   GRANT USAGE ON CATALOG your_catalog TO `service-principal-client-id`;
   
   -- Grant usage on schema  
   GRANT USAGE ON SCHEMA your_catalog.your_schema TO `service-principal-client-id`;
   
   -- Grant manage on specific objects (or schema-level)
   GRANT MANAGE ON TABLE your_catalog.your_schema.your_table TO `service-principal-client-id`;
   ```

3. **Configure SQL Warehouse Access** for the service principal

### Cluster Configuration (Optional)

For better performance, configure your cluster with:

```ini
spark.databricks.service.client.enabled true
spark.sql.execution.arrow.pyspark.enabled false
```

## Usage

### Import the Utility

```python
# Import the utility into your notebook
%run /Shared/utilities/uc_delete_utility
```

### Basic Usage Examples

#### 1. List Objects in Schema

```python
# List all objects in a schema
objects = list_uc_objects(
    catalog="your_catalog",
    schema="your_schema",
    object_types=["table", "view"]
)

print("Available objects:")
for obj_type, obj_list in objects.items():
    print(f"{obj_type}: {len(obj_list)} items")
```

#### 2. Delete Specific Objects

```python
# Delete specific objects (dry run first)
result = delete_uc_objects(
    catalog="your_catalog",
    schema="your_schema",
    objects=["table1", "view1", "table2"],
    dry_run=True  # Always start with dry run!
)

# Check results
if result['success']:
    print(f"✅ Would delete {len(result['executed'])} objects")
else:
    print("❌ Errors found:", result['errors'])
```

#### 3. Delete Objects by Pattern

```python
# Delete all temporary objects
result = delete_uc_objects_by_pattern(
    catalog="your_catalog",
    schema="your_schema",
    prefix="temp_",
    object_types=["table", "view"],
    dry_run=True,
    purge_tables=True
)
```

#### 4. Advanced Pattern Matching

```python
# Delete with multiple filters
result = delete_uc_objects_by_pattern(
    catalog="your_catalog", 
    schema="your_schema",
    prefix="staging_",
    contains="2023",
    suffix="_backup",
    object_types=["table"],
    dry_run=False  # Actually execute
)
```

### Production Deletion Workflow

```python
# Step 1: Explore what's available
objects = list_uc_objects("your_catalog", "your_schema")

# Step 2: Dry run to see what would be deleted
dry_result = delete_uc_objects_by_pattern(
    catalog="your_catalog",
    schema="your_schema", 
    prefix="temp_",
    dry_run=True
)

# Step 3: Review the results
print(f"Would delete {len(dry_result['executed'])} objects:")
for item in dry_result['executed']:
    print(f"  - {item['object']}")

# Step 4: Execute if satisfied
if input("Proceed with deletion? (yes/no): ").lower() == "yes":
    actual_result = delete_uc_objects_by_pattern(
        catalog="your_catalog",
        schema="your_schema",
        prefix="temp_", 
        dry_run=False
    )
```

## API Reference

### `list_uc_objects(catalog, schema, object_types)`

Lists Unity Catalog objects in a schema.

**Parameters:**
- `catalog` (str): Target catalog name
- `schema` (str): Target schema name  
- `object_types` (List[str]): Types to list `["table", "view"]`

**Returns:**
- `Dict[str, List[str]]`: Object lists by type

**Example:**
```python
objects = list_uc_objects("catalog1", "schema1", ["view"])
# Returns: {"views": ["view1", "view2"]}
```

### `delete_uc_objects(catalog, schema, objects, **kwargs)`

Deletes specific Unity Catalog objects.

**Parameters:**
- `catalog` (str): Target catalog name
- `schema` (str): Target schema name
- `objects` (List[str]): Object names to delete
- `object_types` (Optional[List[str]]): Object types to consider
- `dry_run` (bool): Preview mode (default: True)
- `purge_tables` (bool): Add PURGE for tables (default: False)

**Returns:**
- `Dict[str, Any]`: Execution results with success status, executed items, and errors

**Example:**
```python
result = delete_uc_objects(
    catalog="catalog1",
    schema="schema1", 
    objects=["table1", "view1"],
    dry_run=False,
    purge_tables=True
)
```

### `delete_uc_objects_by_pattern(catalog, schema, **kwargs)`

Deletes objects matching specified patterns.

**Parameters:**
- `catalog` (str): Target catalog name
- `schema` (str): Target schema name
- `prefix` (Optional[str]): Filter by prefix
- `suffix` (Optional[str]): Filter by suffix  
- `contains` (Optional[str]): Filter by substring
- `object_types` (List[str]): Object types (default: ["table", "view"])
- `dry_run` (bool): Preview mode (default: True)
- `purge_tables` (bool): Add PURGE for tables (default: False)

**Returns:**
- `Dict[str, Any]`: Execution results

**Example:**
```python
result = delete_uc_objects_by_pattern(
    catalog="catalog1",
    schema="schema1",
    prefix="staging_",
    contains="2023", 
    object_types=["table"],
    dry_run=False
)
```

## Safety Features

### 🛡️ Built-in Safety Mechanisms

1. **Dry Run Default**: All operations default to `dry_run=True`
2. **Validation**: Automatic catalog and schema existence validation
3. **Error Handling**: Comprehensive error catching and reporting
4. **Object Type Detection**: Automatic identification of tables vs views
5. **Detailed Logging**: Complete audit trail of all operations
6. **Graceful Failures**: Individual object failures don't stop batch operations

### 🔍 Result Structure

All functions return a standardized result dictionary:

```python
{
    "success": bool,           # Overall operation success
    "executed": [              # Successfully processed objects
        {
            "object": "catalog.schema.object",
            "type": "table|view", 
            "status": "deleted|dry_run",
            "sql": "DROP VIEW ..."
        }
    ],
    "errors": [                # Failed operations
        {
            "object": "catalog.schema.object",
            "error": "error description"
        }
    ],
    "dry_run": bool           # Whether this was a preview
}
```

## Troubleshooting

### Common Issues

#### Issue: "Service principal token failed"
**Cause**: Incorrect secret configuration or permissions
**Solution**: 
1. Verify secret scope and key names
2. Check service principal credentials in Azure AD
3. Ensure service principal has Databricks access

#### Issue: "No SQL warehouses available"  
**Cause**: Service principal lacks SQL warehouse access
**Solution**: Grant service principal access to at least one SQL warehouse

#### Issue: "Permission denied" errors
**Cause**: Insufficient Unity Catalog permissions
**Solution**: Grant required permissions:
```sql
GRANT USAGE ON CATALOG your_catalog TO `your-service-principal`;
GRANT USAGE ON SCHEMA your_catalog.your_schema TO `your-service-principal`;
GRANT MANAGE ON TABLE your_catalog.your_schema.your_table TO `your-service-principal`;
```

#### Issue: "Object not found" 
**Cause**: Object doesn't exist or wrong catalog/schema
**Solution**: 
1. Use `list_uc_objects()` to verify object existence
2. Check catalog and schema names for typos

#### Issue: "Spark session not available"
**Cause**: Normal - utility automatically falls back to SQL Warehouse
**Solution**: No action needed, this is expected behavior

### Debug Mode

Enable verbose logging by adding this to your notebook:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Manual Testing

Test service principal connectivity:

```python
# Test 1: Token generation
try:
    token = _get_service_principal_token()
    print("✅ Service principal token generated successfully")
except Exception as e:
    print(f"❌ Token generation failed: {e}")

# Test 2: SQL Warehouse access
try:
    warehouse_id = _get_sql_warehouse()
    print(f"✅ SQL warehouse available: {warehouse_id}")
except Exception as e:
    print(f"❌ SQL warehouse access failed: {e}")

# Test 3: Basic SQL execution
try:
    result = _execute_sql_via_warehouse("SELECT 1 as test")
    print("✅ SQL execution successful")
except Exception as e:
    print(f"❌ SQL execution failed: {e}")
```

## Best Practices

### 🎯 Safety Guidelines

1. **Always start with dry runs** - Never skip the preview step
2. **Test with small batches** - Start with 1-2 objects before bulk operations
3. **Verify object names** - Use `list_uc_objects()` to confirm targets exist
4. **Use specific patterns** - Avoid overly broad filters that might catch unexpected objects
5. **Document deletions** - Keep records of what was deleted and why
6. **Backup critical data** - For tables containing important data

### 🔧 Performance Optimization

1. **Use pattern matching** for bulk operations instead of individual deletions
2. **Limit object types** - Specify only "table" or "view" when you know the type
3. **Batch operations** - Delete related objects together
4. **Monitor SQL warehouse** - Ensure warehouse is running for better performance

### 📝 Operational Guidelines

1. **Establish naming conventions** for temporary objects (e.g., `temp_`, `staging_`)
2. **Regular cleanup schedules** - Use the utility in scheduled jobs
3. **Monitor permissions** - Regularly audit service principal access
4. **Version control** - Keep the utility notebook in source control
5. **Team training** - Ensure team members understand safe usage

### 🚨 What NOT to Do

❌ **Don't skip dry runs** - Always preview first  
❌ **Don't use overly broad patterns** - Avoid patterns like `prefix=""`  
❌ **Don't ignore errors** - Investigate and resolve permission issues  
❌ **Don't delete without understanding** - Know what each object contains  
❌ **Don't run in production without testing** - Test in dev/staging first  

## Advanced Usage

### Scheduled Cleanup Job

```python
# Example: Daily cleanup of temporary objects
def daily_cleanup():
    schemas_to_clean = ["staging", "temp", "dev"]
    temp_patterns = ["temp_", "tmp_", "test_"]
    
    for schema in schemas_to_clean:
        for pattern in temp_patterns:
            result = delete_uc_objects_by_pattern(
                catalog="your_catalog",
                schema=schema,
                prefix=pattern,
                dry_run=False,
                purge_tables=True
            )
            
            if result['success']:
                print(f"✅ Cleaned {len(result['executed'])} objects from {schema}")
            else:
                print(f"❌ Cleanup failed for {schema}: {result['errors']}")

# Schedule this function to run daily
```

### Integration with Workflows

```python
# Example: Cleanup as part of data pipeline
def pipeline_cleanup(job_id: str):
    """Clean up objects created by a specific job"""
    
    # Delete objects with job ID in name
    result = delete_uc_objects_by_pattern(
        catalog="your_catalog",
        schema="staging",
        contains=f"job_{job_id}",
        dry_run=False
    )
    
    return result['success']

# Use in your data pipeline
if pipeline_cleanup("20231201_001"):
    print("Pipeline cleanup successful")
```

## Support and Contributions

### Getting Help

1. **Check this wiki** for common solutions
2. **Review error messages** - they often contain specific guidance
3. **Test individual components** using the debug commands above
4. **Contact your Databricks administrator** for permission issues

### Contributing

To improve this utility:

1. Test thoroughly in development environment
2. Follow existing code patterns and safety practices
3. Update documentation for any new features
4. Ensure backward compatibility

### Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-08-10 | Initial release with basic deletion functionality |
| 1.1 | 2025-08-12 | Added pattern matching and dry run features |
| 1.2 | 2025-08-14 | Enhanced error handling and SQL warehouse fallback |

---

**⚠️ Important Notice**: This utility can permanently delete data. Always use dry runs first and ensure you have proper backups of important data before executing deletions.
