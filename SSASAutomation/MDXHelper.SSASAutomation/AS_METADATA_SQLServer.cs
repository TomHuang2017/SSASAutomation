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
       AND dimension_id = '{0}'
ORDER  BY hierarchy_name,
          level_id ", dimension_id);
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString);
        }
        public DataTable GET_SSAS_CORE_MEASURES_SET(DB_SQLHELPER_BASE sqlHelper,String measure_group_id = null)
        {
            String QueryString = String.Format(@"
SELECT DISTINCT mg.measure_group_id                                                             AS measure_group_id,
                mg.measure_group_name                                                           AS measure_group_name,
                mea.measure_id                                                                  AS measure_id,
                mea.measure_name                                                                AS measure_name,
                mapp.measure_data_type                                                          AS measure_data_type,
                Isnull(mapp.db_column_customized, mapp.db_column_default)                       AS db_column,
                mg.dsv_schema_name                                                              AS dsv_schema_name,
                Isnull(mapp.aggregation_function_customized, mapp.aggregation_function_default) AS aggregation_function,
                mea.display_folder                                                              AS display_folder,
                mea.format_string                                                               AS format_string
FROM   ssas_measures AS mea WITH(nolock)
       INNER JOIN ssas_measures_mapping AS mapp WITH(nolock)
               ON mea.measure_id = mapp.measure_id
       INNER JOIN ssas_measure_group AS mg WITH(nolock)
               ON mg.measure_group_id = mapp.measure_group_id
       INNER JOIN ssas_measures_description AS descr WITH(nolock)
               ON descr.measure_id = mea.measure_id
WHERE  mea.is_enabled = 1
       AND mg.measure_group_id='{0}'", measure_group_id);
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString);
        }
        public DataTable GET_SSAS_MEASURE_GROUPS_SET(DB_SQLHELPER_BASE sqlHelper, int isRolap)
        {
            String isRolapFilter = (isRolap == 0) ? " AND mg.is_real_time<>1" : "";
            String QueryString = String.Format(@"
SELECT DISTINCT order_query.order_index                            AS order_index,
                mg.measure_group_id                                AS measure_group_id,
                mg.dsv_schema_name                                 AS dsv_schema_name,
                mg.measure_group_name                              AS measure_group_name,
                Isnull(mg.key_not_found_action, '0')               AS key_not_found_action,
                mg.is_real_time                                    AS is_real_time,
                dsv.depended_fact_table                            AS depended_fact_table,
                cast(Isnull(mg.is_rolap_mg, Isnull(mg.is_real_time, 0)) as int) AS is_rolap_mg
FROM   ssas_measures AS mea WITH(nolock)
       INNER JOIN ssas_measures_mapping AS mapp WITH(nolock)
               ON mea.measure_id = mapp.measure_id
       INNER JOIN ssas_measure_group AS mg WITH(nolock)
               ON mg.measure_group_id = mapp.measure_group_id
       INNER JOIN ssas_measures_description AS descr WITH(nolock)
               ON descr.measure_id = mea.measure_id
       LEFT JOIN ssas_etl_module AS module WITH(nolock)
              ON module.module_name = Replace(mg.dsv_schema_name, 'olap_', '')
                 AND module.is_enabled = 1
       INNER JOIN ssas_dsv AS dsv WITH(nolock)
               ON mg.dsv_schema_name = dsv.dsv_schema_name
       LEFT JOIN (SELECT Min(order_index) AS order_index,
                         v1.measure_group_id
                  FROM   (SELECT DISTINCT 1 AS order_index,
                                          v2.measure_group_id
                          FROM   ssas_dim_usage v1
                                 INNER JOIN ssas_dim_usage v2
                                         ON v1.internal_measure_group_id = v2.measure_group_id
                          UNION ALL
                          SELECT Max(CASE
                                       WHEN Lower(dim_usage_type) = 'regular' THEN 2
                                       WHEN Lower(dim_usage_type) = 'reference' THEN 3
                                       WHEN Lower(dim_usage_type) = 'manytomany' THEN 4
                                       ELSE 4
                                     END) AS order_index,
                                 measure_group_id
                          FROM   [dbo].ssas_dim_usage
                          GROUP  BY measure_group_id) v1
                  GROUP  BY v1.measure_group_id)order_query
              ON order_query.measure_group_id = mg.measure_group_id
WHERE  1 = 1 {0}
ORDER  BY order_query.order_index,
          mg.is_real_time DESC,
          mg.measure_group_name DESC ", isRolapFilter);
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString);

        }
        public DataTable GET_SSAS_PARTITION_SET(DB_SQLHELPER_BASE sqlHelper,String measure_group_id = null)
        {
            String QueryString = String.Format(@"
SELECT DISTINCT mg.dsv_schema_name                                                              AS dsv_schema_name,
                Isnull(usage.fact_fk_dim_column_name_customized, usage.fact_fk_dim_column_name) AS fact_fk_dim_column_name,
                mg.measure_group_id                                                             AS measure_group_id
FROM   ssas_dim_usage AS usage WITH(nolock)
       INNER JOIN ssas_measure_group AS mg WITH(nolock)
               ON usage.measure_group_id = mg.measure_group_id
WHERE  Lower(usage.dimension_id) = 'olap_dim_date'
       AND mg.measure_group_id ='{0}'", measure_group_id);
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString); ;
        }
        public DataTable GET_SSAS_AGGREGATION_DESIGN_SET(DB_SQLHELPER_BASE sqlHelper,String measure_group_id = null)
        {
            String QueryString = String.Format(@"
SELECT DISTINCT mg.measure_group_id,
                mg.aggregation_design_name,
                agg_d_att.aggregation_name,
                agg_d_att.dimension_id,
                agg_d_att.attribute_id,
                agg_d.description
FROM   ssas_measure_group AS mg WITH(nolock)
       INNER JOIN ssas_aggregation_design AS agg_d WITH(nolock)
               ON mg.aggregation_design_name = agg_d.aggregation_design_name
       INNER JOIN ssas_aggregation_design_attribute AS agg_d_att WITH(nolock)
               ON agg_d.aggregation_name = agg_d_att.aggregation_name
WHERE  mg.measure_group_id='{0}'", measure_group_id);
            return sqlHelper.EXECUTE_SQL_QUERY_RETURN_TABLE(sqlHelper,QueryString);
        }
        public DataTable GET_SSAS_DIM_USAGE_SET(DB_SQLHELPER_BASE sqlHelper,String measure_group_id = null)
        {
            String QueryString = String.Format(@"
SELECT mg.measure_group_id                                                                       AS measure_group_id,
       usage.dim_usage_type                                                                      AS dim_usage_type,
       mg.dsv_schema_name                                                                        AS dsv_schema_name,
       Isnull(usage.fact_fk_dim_column_name_customized, usage.fact_fk_dim_column_name)           AS fact_fk_dim_column_name,
       Isnull(usage.fact_fk_dim_column_data_type_customized, usage.fact_fk_dim_column_data_type) AS fact_fk_dim_column_data_type,
       usage.dimension_id                                                                        AS dimension_id,
       usage.attribute_id                                                                        AS attribute_id,
       usage.internal_dim_id                                                                     AS internal_dim_id,
       usage.internal_dim_attrid                                                                 AS internal_dim_attrid,
       usage.internal_measure_group_id                                                           AS internal_measure_group_id
FROM   ssas_dim_usage AS usage WITH(nolock)
       INNER JOIN ssas_measure_group AS mg WITH(nolock)
               ON usage.measure_group_id = mg.measure_group_id
--       INNER JOIN ssas_etl_module AS module WITH(nolock)
--               ON module.module_name = Replace(mg.dsv_schema_name, 'olap_', '')
--                  AND module.is_enabled = 1
WHERE  mg.measure_group_id = '{0}'
ORDER  BY CASE
            WHEN Lower(usage.dim_usage_type) = 'regular' THEN '1'
            WHEN Lower(usage.dim_usage_type) = 'reference' THEN '2'
            WHEN Lower(usage.dim_usage_type) = 'manytomany' THEN '3'
            ELSE '4'
          END ", measure_group_id);
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
