using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace MDXHelper.SSASAutomation
{
    public class AS_METADATA_SQLServer:AS_METADATA
    {
        public DataTable GET_SSAS_DSV_SET(DB_SQLHELPER_BASE sqlHelper)
        {
            String QueryString = @"
SELECT DISTINCT dsv_schema_name,
                db_table_name,
                table_type,
                is_named_query
FROM   [dbo].[ssas_dsv] as tb with(nolock)
";
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString);
        }
        public DataTable GET_SSAS_DIMENSION_SET(DB_SQLHELPER_BASE sqlHelper)
        {
            String QueryString = @"
SELECT DISTINCT dimension_id,
                dimension_name,
                dsv_schema_name,
                dimension_type
FROM   dbo.[ssas_dimension] AS tb WITH(nolock) 
";
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString);
        }
        public DataTable GET_SSAS_ATTRIBUTES_SET(DB_SQLHELPER_BASE sqlHelper,String dimension_id = null)
        {
            String QueryString = String.Format(@"
SELECT DISTINCT attr.attribute_id                                                         AS attribute_id,
                Isnull(attr.attribute_name_customized, attr.attribute_name)               AS attribbute_name,
                dim.dimension_id                                                          AS dimension_id,
                dim.dsv_schema_name                                                       AS dsv_schema_name,
                Isnull(attr.key_column_db_column_customized, attr.key_column_db_column)   AS key_column_db_column,
                Isnull(attr.key_column_oledb_type_customized, attr.key_column_oledb_type) AS key_column_oledb_type,
                attr.attribute_usage                                                      AS attribute_usage,
                Isnull(attr.name_column_customized, attr.name_column)                     AS name_column,
                attr.visible                                                              AS visible,
                attr.atthier_enabled                                                      AS atthier_enabled,
                Isnull(attr.order_by_customized, attr.order_by)                           AS order_by
FROM   [dbo].[ssas_attributes] attr WITH(nolock)
       INNER JOIN [dbo].[ssas_dimension] AS dim WITH(nolock)
               ON attr.[dimension_id] = dim.[dimension_id] 
WHERE  dim.[dimension_id]= '{0}'", dimension_id);
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString);
        }
        public DataTable GET_SSAS_ATTRIBUTE_RELATION_SHIPS_SET(DB_SQLHELPER_BASE sqlHelper,String dimension_id = null)
        {
            String QueryString = String.Format(@"
SELECT DISTINCT dimension_id,
                based_attribute_id,
                related_attribute_id,
                relationship_type
FROM   ssas_attributes_relationship AS rs WITH(nolock)
WHERE  is_enabled = 1
       AND dimension_id='{0}'", dimension_id);
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString);
        }
        public DataTable GET_SSAS_HIERARCHIES_SET(DB_SQLHELPER_BASE sqlHelper,String dimension_id = null)
        {
            String QueryString = String.Format(@"
SELECT DISTINCT dimension_id,
                hierarchy_name,
                level_name,
                level_id,
                source_attribute_id,
                hierarchies_id
FROM   ssas_attributes_hierarchies AS hie WITH(nolock)
WHERE  is_enabled = 1
       AND dimension_id <> '{0}'
ORDER  BY hierarchy_name,
          level_id ", dimension_id);
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString);
        }
        public DataTable GET_SSAS_CORE_MEASURES_SET(DB_SQLHELPER_BASE sqlHelper,String measure_group_id = null)
        {
            String QueryString = String.Format(@"
            SELECT DISTINCT
                   mg.measureGroupID,
                   mg.measureGroupName,
                   measure.MeasureId,
                   measure.MeasureName,
                   map.MeasureDataType,
                   map.DBColumn,
                   mg.DSVSchemaName,
                   map.AggregationFunction,
                   measure.DisplayFolder,
                   measure.FormatString
            FROM   CLB_MetaData_Measures AS measure
                   JOIN CLB_MetaData_Measures_Mapping AS map
                     ON map.MeasureID = measure.MeasureID
                   JOIN CLB_MetaData_MeasureGroup AS mg
                     ON mg.MeasureGroupID = map.MeasureGroupID
                   JOIN CLB_MetaData_Measures_Description AS descrip
                     ON descrip.MeasureID = measure.MeasureID
            WHERE mg.measureGroupID='{0}'", measure_group_id);
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString);
        }
        public DataTable GET_SSAS_MEASURE_GROUPS_SET(DB_SQLHELPER_BASE sqlHelper, int isRolap)
        {
            String isRolapFilter = (isRolap == 0) ? " AND IsRealTime<>1" : "";
            String QueryString = String.Format(@"
SELECT DISTINCT
		order_query.order_index,
        mg.measureGroupID,
		mg.DSVSchemaName,
        mg.measureGroupName,
        isnull(mg.KeyNotFound_Action,'0') AS KeyNotFound_Action,
        mg.IsRealTime,
		dsv.DependedFactTable,
        isnull(mg.Is_Rolap_Mg,isnull(mg.IsRealTime,0)) AS Is_Rolap_Mg
FROM    CLB_MetaData_Measures AS measure
        JOIN CLB_MetaData_Measures_Mapping AS map
            ON map.MeasureID = measure.MeasureID
        JOIN CLB_MetaData_MeasureGroup AS mg
            ON map.MeasureGroupID = mg.MeasureGroupID
        JOIN CLB_MetaData_Measures_Description AS descrip
            ON descrip.MeasureID = measure.MeasureID
		LEFT JOIN [dbo].[CLB_MetaData_ETL_Module] as fact_module
			ON fact_module.ModuleName=replace(mg.DSVSchemaName,'OLAP_','') AND fact_module.Enabled=1
		JOIN [dbo].[CLB_MetaData_DSV] as dsv
		    ON dsv.DSVSchemaName=mg.DSVSchemaName
		LEFT JOIN (
			select min(order_index) as order_index,v1.MeasureGroupID
			from 
			(
			select distinct 1 as order_index,v2.MeasureGroupID from [dbo].[CLB_MetaData_DimUsage] v1
			inner join [dbo].[CLB_MetaData_DimUsage] v2 on v1.InternalMeasureGroupID=v2.MeasureGroupID
			union all
			select max(case when [DimUsageType]='regular' then 2
						when [DimUsageType]='reference' then 3
						when [DimUsageType]='manytomany' then 4
						else 4 end) as order_index, MeasureGroupID from [dbo].[CLB_MetaData_DimUsage]
						group by MeasureGroupID) v1
			group by v1.MeasureGroupID
		) order_query
		on order_query.MeasureGroupID= mg.MeasureGroupID
WHERE 1=1  {0}
order by order_query.order_index,IsRealTime desc,measureGroupName desc", isRolapFilter);
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString);

        }
        public DataTable GET_SSAS_PARTITION_SET(DB_SQLHELPER_BASE sqlHelper,String measure_group_id = null)
        {
            String QueryString = String.Format(@"
            select top 1  
		            mg.DSVSchemaName,
		            usage.factFKDimColumnName,
		            mg.MeasureGroupID
            from [dbo].[CLB_MetaData_DimUsage]  AS usage
            INNER JOIN dbo.CLB_MetaData_MeasureGroup AS mg
	            ON mg.MeasureGroupID = usage.MeasureGroupID
            where dimensionid ='OLAP_DIM_Date' and mg.MeasureGroupID='{0}'", measure_group_id);
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString); ;
        }
        public DataTable GET_SSAS_AGGREGATION_DESIGN_SET(DB_SQLHELPER_BASE sqlHelper,String measure_group_id = null)
        {
            String QueryString = String.Format(@"
            select 
	            mg.MeasureGroupID,
	            mg.AggregationDesignName,
	            agg.AggregationName,
	            agg.DimensionID,
	            agg.AttributeID,
	            design.Description
            from [dbo].[CLB_MetaData_MeasureGroup] as mg
            inner join CLB_MetaData_AggregationDesign as design
            on mg.AggregationDesignName=design.AggregationDesignName
            inner join CLB_MetaData_AggregationDesign_Attribute as agg
            on agg.AggregationName=design.AggregationName
            where mg.MeasureGroupID='{0}'", measure_group_id);
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString);
        }
        public DataTable GET_SSAS_DIM_USAGE_SET(DB_SQLHELPER_BASE sqlHelper,String measure_group_id = null)
        {
            String QueryString = String.Format(@"
            SELECT mg.MeasureGroupID,
                   DimUsageType,
                   mg.DSVSchemaName,
                   factFKDimColumnName,
                   DataType,
                   DimensionID,
                   AttributeID,
                   InternalDimID,
                   InternalDimAttrID,
                   InternalMeasureGroupID
            FROM   dbo.CLB_MetaData_DimUsage AS usage
                   INNER JOIN dbo.CLB_MetaData_MeasureGroup AS mg
                           ON mg.MeasureGroupID = usage.MeasureGroupID
				   LEFT JOIN [dbo].[CLB_MetaData_ETL_Module] as fact_module--only get the enabled module
						   ON fact_module.ModuleName=replace(mg.DSVSchemaName,'OLAP_','') AND fact_module.Enabled=1 
            WHERE  mg.MeasureGroupID = '{0}' 
            ORDER by CASE WHEN DimUsageType='Regular' THEN 1 
			              WHEN DimUsageType='Reference' THEN 2 
			              WHEN DimUsageType='ManyToMany' THEN 3 
		             ELSE 4 END ", measure_group_id);
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString);
        }

        public string GET_SSAS_PARTITION_FILTER(String filterColumnName, int partitinIndex, int partitionMonthVolumn)
        {
            String partitionFilter = "";
            partitionFilter = String.Format(@"
AND {0} >=convert(varchar(8),dateadd(month,-{1}*{2},dateadd(year,datediff(year,'1999-01-01',getdate())+1,'1999-01-01')),112) 
AND {3} <convert(varchar(8),dateadd(month,-{4}*{5},dateadd(year,datediff(year,'1999-01-01',getdate())+1,'1999-01-01')),112)",
filterColumnName,
(partitinIndex + 1).ToString(),
partitionMonthVolumn.ToString(),
filterColumnName,
partitinIndex.ToString(),
partitionMonthVolumn.ToString()
);
            return partitionFilter;
        }
    }
}
