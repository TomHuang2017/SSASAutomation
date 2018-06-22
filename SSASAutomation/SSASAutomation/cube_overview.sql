 
 --MG_MR_MedicalRecord	OLAP_DIM_Disease

 --select * from ssas_dim_usage as first_usage
 --inner join ssas_dim_usage as fact_usage
 --on fact_usage.measure_group_id=first_usage.measure_group_id and lower(fact_usage.dim_usage_type)='fact'
 ----inner join ssas_dim_usage as dim_usage
 ----on dim_usage.dimension_id=first_usage.dimension_id and lower(dim_usage.dim_usage_type)='regular'
 --where first_usage.measure_group_id='MG_MR_MedicalRecord' and first_usage.dimension_id='OLAP_DIM_Disease'
 --and ltrim(first_usage.dim_usage_type)='manytomany'

 --select distinct dim_usage_type from ssas_dim_usage as fact_usage where  lower(fact_usage.dim_usage_type)='fact'

SELECT 
	   mg.is_real_time,
	   --mg.measure_group_id                                                                       AS measure_group_id,
	   mg.measure_group_name,
	   mea.measure_name,
       usage.dim_usage_type,
	   dim.dimension_name,
	   dim.dimension_id,
	   dim_internal.dimension_name,
	   mg_internal.measure_group_name,
	   '['+mg.measure_group_name+']'
	   +case when dim_internal.dimension_name is null then '' 
	         else '=>['+dim_internal.dimension_name +']'
		end 
	   +'=>['+dim.dimension_name+']' as [path]
	   --case when dim_usage_type='ManyToMany' then 
FROM   ssas_dim_usage AS usage WITH(nolock)
       INNER JOIN ssas_measure_group AS mg WITH(nolock)
               ON usage.measure_group_id = mg.measure_group_id
	   left JOIN ssas_measure_group AS mg_internal WITH(nolock)
               ON usage.internal_measure_group_id = mg_internal.measure_group_id
	   inner join ssas_dimension as dim
	   on dim.dimension_id=usage.dimension_id
	   left join ssas_dimension as dim_internal
	   on dim_internal.dimension_id=usage.internal_dim_id
	   inner join ssas_measures_mapping as measure_map
	   on measure_map.measure_group_id=mg.measure_group_id
	   inner join ssas_measures as mea
	   on mea.measure_id=measure_map.measure_id
--       INNER JOIN ssas_etl_module AS module WITH(nolock)
--               ON module.module_name = Replace(mg.dsv_schema_name, 'olap_', '')
--                  AND module.is_enabled = 1

ORDER  BY CASE
            WHEN Lower(usage.dim_usage_type) = 'regular' THEN '1'
            WHEN Lower(usage.dim_usage_type) = 'reference' THEN '2'
            WHEN Lower(usage.dim_usage_type) = 'manytomany' THEN '3'
            ELSE '4'
          END


go


SELECT DISTINCT is_rolap_cube,
                measure_group_id,
                measure_group_name,
                dim_usage_type,
                dimension_name,
                dimension_id
FROM   (SELECT mg.is_real_time AS is_rolap_cube,
               mg.measure_group_id,
               mg.measure_group_name,
               mea.measure_name,
               usage.dim_usage_type,
               dim.dimension_name,
               dim.dimension_id
        FROM   ssas_dim_usage AS usage WITH(nolock)
               INNER JOIN ssas_measure_group AS mg WITH(nolock)
                       ON usage.measure_group_id = mg.measure_group_id
               LEFT JOIN ssas_measure_group AS mg_internal WITH(nolock)
                      ON usage.internal_measure_group_id = mg_internal.measure_group_id
               INNER JOIN ssas_dimension AS dim
                       ON dim.dimension_id = usage.dimension_id
               LEFT JOIN ssas_dimension AS dim_internal
                      ON dim_internal.dimension_id = usage.internal_dim_id
               INNER JOIN ssas_measures_mapping AS measure_map
                       ON measure_map.measure_group_id = mg.measure_group_id
               INNER JOIN ssas_measures AS mea
                       ON mea.measure_id = measure_map.measure_id
               INNER JOIN ssas_etl_module AS module WITH(nolock)
                       ON module.module_name = Replace(mg.dsv_schema_name, 'olap_', '')
                          AND module.is_enabled = 1
        WHERE  mea.measure_name LIKE '%%'
                OR dim.dimension_name LIKE '%%'
                OR mg.measure_group_name LIKE '%%') AS fact 



declare
@dimension_name nvarchar(100)='员工',
	@measure_group_name_or_measure_name nvarchar(100)=null,
	@cube_type bit=0

declare @filter_query nvarchar(max)=''
set @filter_query=' mg.is_real_time='+ltrim(@cube_type)+' and dim.dimension_name LIKE ''%'+isnull(@dimension_name,'')+'%'''+
                  ' and (mea.measure_name LIKE ''%'+isnull(@measure_group_name_or_measure_name,'')+'%'''+
				  ' or mg.measure_group_name LIKE ''%'+isnull(@measure_group_name_or_measure_name,'')+'%'')'
print @filter_query



exec sp_ssas_cube_over_view '科室','费'