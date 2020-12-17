
/*
ssas cube里，对于默认的只有禁用和启用，是逻辑删除，
			 对于新增的功能，再做删除，则物理删除
*/
if object_id('dbo.ssas_dsv') IS NOT NULL 
	drop table dbo.ssas_dsv
GO
/*
删除：
	默认的不会，留着，只是数据为空，
	定制化的会，物理删除
修改：
	默认的不会，
	定制化的，会
增加：
	定制化的，会
*/
create table dbo.ssas_dsv
(
	dsv_schema_name nvarchar(100), 
	db_table_name nvarchar(100),--table name and friendly name
	table_type nvarchar(50),--view or table
	is_named_query bit,
	depended_fact_table NVARCHAR(200),
	is_default bit,--新建的为0，默认的为1
	created_at datetime NOT NULL,
	updated_at datetime NOT NULL,
	primary key(dsv_schema_name)
)
GO
insert into ssas_dsv
select 
	DSVSchemaName as dsv_schema_name,
	DBTableName as db_table_name,
	TableType as table_type,
	IsNamedQuery as is_named_query,
	DependedFactTable as depended_fact_table,
	1 as is_default,
	getdate(),
	getdate()
from  [dbo].[CLB_MetaData_DSV]
--select * from ssas_dsv
go
if object_id('dbo.ssas_measure_group') IS NOT NULL 
	drop table dbo.ssas_measure_group

GO
/*
删除：
	默认会，用is_enable=0，逻辑删除，建议改partition 加上 and 1=2
	定制化的会，物理删除!
修改：
	默认的不会，
	定制化的，会
增加：
	定制化的，会
*/
create table dbo.ssas_measure_group
(
	measure_group_id nvarchar(100),--英文名
	measure_group_name nvarchar(100),--中文名
	dsv_schema_name nvarchar(100),
	aggregation_design_name nvarchar(100),
	key_not_found_action nvarchar(100) null,
	is_rolap_mg bit,
	is_real_time bit,--是否实时数据
	is_default bit,--是否默认，新建的为0，默认的为1
	is_enabled bit,--可以禁用一些没必要的度量值组，提高cube process的性能，禁用可以是不创建（需要建立measure依赖，不创建依赖的measure才行）或者改下partition的where条件加1=2
	created_at datetime NOT NULL,
	updated_at datetime NOT NULL,
	primary key(measure_group_id)
	--foreign key (dsv_schema_name) references ssas_dsv(dsv_schema_name)
)
GO
insert into ssas_measure_group
select 
MeasureGroupID as measure_group_id,
MeasureGroupName as measure_group_name,
DSVSchemaName as dsv_schema_name,
AggregationDesignName as aggregation_design_name,
KeyNotFound_Action as key_not_found_action,
Is_Rolap_Mg as is_rolap_mg,
IsRealTime as is_real_time,
1 as is_default,
1 as is_enable,
getdate(),
getdate()
from [dbo].[CLB_MetaData_MeasureGroup] 
go
--select * from ssas_measure_group
go
if object_id('dbo.ssas_dimension') IS NOT NULL 
	drop table dbo.ssas_dimension
GO
/*
删除：
	默认不会
	定制化的会，会，物理删除，且删除相应的依赖对象，如attribute以及dim uage!
修改：
	默认的不会，
	定制化的，会
增加：
	定制化的，会
*/
create table dbo.ssas_dimension
(
	dimension_id nvarchar(100),--英文名
	dimension_name nvarchar(100),--中文名
	dsv_schema_name nvarchar(100),
	dimension_type nvarchar(20),--regular,time,account etc..
	is_default bit ,--默认为1，新加的为0，不支持删除
	created_at datetime NOT NULL,
	updated_at datetime NOT NULL,
	primary key(dimension_id)
	--foreign key (dsv_schema_name) references ssas_dsv(dsv_schema_name)
)
GO
insert into ssas_dimension
select 
DimensionID as dimension_id,
DimensionName as dimension_name,
DSVSchemaName as dsv_schema_name,
DimensionType as dimension_type,
1 as is_default,
getdate(),
getdate()
from [dbo].[CLB_MetaData_Dimension]
go
--select * from ssas_dimension
go
if object_id('dbo.ssas_measures') IS NOT NULL 
	drop table dbo.ssas_measures

GO
/*
删除：
	默认不会，要删除就禁用measure group，给partition 加and 1=2
	定制化的会，会，物理删除，自己处理相应的depended calcualted measures
修改：
	默认的不会，
	定制化的，会
增加：
	定制化的，会
*/
create table dbo.ssas_measures
(
	measure_name nvarchar(255),--中文名
	measure_id nvarchar(255),--英文名
	display_folder nvarchar(255),--显示文件夹
	format_string nvarchar(20),--格式
	is_default bit,--默认的是1，新加的是0
	is_enabled bit,
	created_at datetime NOT NULL,
	updated_at datetime NOT NULL,
	primary key(measure_id)
)
go
insert into ssas_measures
select 
MeasureName as measure_name,
MeasureID as measure_id,
DisplayFolder as display_folder,
FormatString as format_string,
1 as is_default,
1 as is_enable,
getdate(),
getdate()
from [dbo].[CLB_MetaData_Measures]
go
select * from ssas_measures
go
 /*配置表*/
if object_id('dbo.ssas_measures_mapping') IS NOT NULL 
	drop table dbo.ssas_measures_mapping
GO
/*
删除：
	默认不会，要的话就通过禁用measuregroup 给partition 加1=2
	定制化的会,物理删除所有
修改：
	默认的会，通过修改customized字段，
	定制化的会，通过修改customized字段
增加：
	定制化的，会
*/
create table dbo.ssas_measures_mapping
(
	measure_id nvarchar(255),--英文名
	measure_data_type nvarchar(100),--Measure的数据类型
	measure_group_id  nvarchar(100),--对应measure_groupID名称
	db_column_default nvarchar(255),--对应DB中的列
	db_column_customized nvarchar(255),--对应DB中的列-定制化修改
	aggregation_function_default nvarchar(50),--聚合方式
	aggregation_function_customized nvarchar(50),--定制化聚合方式
	is_default bit,
	created_at datetime NOT NULL,
	updated_at datetime NOT NULL,
	primary key(measure_id)
	--foreign key (measure_group_id) references ssas_measure_group(measure_group_id),
	--foreign key (measure_id) references ssas_measures(measure_id)
)
GO
insert into ssas_measures_mapping
select 
	MeasureID as measure_id,
	MeasureDataType as measure_data_type,
	MeasureGroupID as measure_group_id,
	DBColumn as db_column_default,
	null as db_column_customized,
	AggregationFunction as aggregation_function_default,
	null as aggregation_function_customized,
	1 as is_default,
	getdate(),
	getdate()
from [dbo].[CLB_MetaData_Measures_Mapping]
go
select * from ssas_measures_mapping
go
if object_id('dbo.ssas_measures_description') IS NOT NULL 
	drop table dbo.ssas_measures_description
GO
/*
删除：
	默认不会，要的话就通过禁用measuregroup 给partition 加1=2
	定制化的会,物理删除所有
修改：
	默认的会，通过修改customized字段，
	定制化的会，通过修改customized字段
增加：
	定制化的，会
*/
create table dbo.ssas_measures_description
(
	measure_id nvarchar(255),--英文名
	customer_name nvarchar(255),--客户名称
	description nvarchar(500),--公式，若为Core Measure则填成Core即可。
	description_customized nvarchar(500),--定制化描述公式，若为Core Measure则填成Core即可。
	is_default bit,--新加的为0，默认的为1
	created_at datetime NOT NULL,
	updated_at datetime NOT NULL,
	primary key(measure_id)
	--foreign key (measure_id) references ssas_measures(measure_id)
)

insert into ssas_measures_description
select
	MeasureID as measure_id,
	HospitalName as customer_name,
	Description as description,
	null as description_customized,
	1 as is_default,
	getdate(),
	getdate()
from [dbo].[CLB_MetaData_Measures_Description]
go
 --select * from ssas_measures_description
go
if object_id('dbo.ssas_attributes') IS NOT NULL 
	drop table dbo.ssas_attributes

GO
/*
删除：
	默认不会
	定制化的会,物理删除所有
修改：
	默认的会，通过修改customized字段，
	定制化的会，通过修改customized字段
增加：
	定制化的，会
*/
create table dbo.ssas_attributes
(
	attribute_id nvarchar(100) ,--英文名
	attribute_name nvarchar(100),--默认的中文名
	attribute_name_customized nvarchar(100),--定制化attribute的显示名
	dimension_id nvarchar(100),
	key_column_db_column nvarchar(100),--默认的映射列,Key Column
	key_column_db_column_customized nvarchar(100),--定制化映射到db里的列，key Column
	key_column_oledb_type nvarchar(20),--默认的数据类型，key column的type
	key_column_oledb_type_customized nvarchar(20),--定制化数据类型，key column的type
	attribute_usage nvarchar(20),
	name_column nvarchar(100),--默认的值列,type默认为wchar
	name_column_customized nvarchar(100),--定制化显示值的名称,type默认为wchar
	visible bit ,
	atthier_enabled bit ,
	order_by nvarchar(100),
	order_by_customized nvarchar(100),
	is_default bit,
	created_at datetime NOT NULL,
	updated_at datetime NOT NULL,
	primary key(attribute_id,dimension_id)
	--foreign key (dimension_id) references ssas_dimension(dimension_id)
)
GO
 

insert into ssas_attributes
select
	AttributeID as attribute_id,
	AttributeName as attribute_name,
	null as attribute_name_customized,
	DimensionID as dimension_id,
	DBColumn as key_column_db_column,
	null as key_column_db_column_customized,
	OleDbType as key_column_oledb_type,
	null as key_column_oledb_type_customized,
	AttributeUsage as attribute_usage,
	NameColumn as name_column,
	null as name_column_customized,
	Visible as visible,
	AttHierEnabled as atthier_enabled,
	OrderBy as order_by,
	null as order_by_customized,
	1 as is_default,
	getdate(),
	getdate()
from [dbo].[CLB_MetaData_Attributes]
go
 select * from ssas_attributes
go
if object_id('dbo.ssas_dim_usage') IS NOT NULL 
	drop table dbo.ssas_dim_usage
GO
/*
删除：
	默认的会，用逻辑删除,is_enabled=0
	定制化的会,物理删除所有
修改：
	默认的会，通过修改customized字段，
	定制化的会，通过修改customized字段
增加：
	定制化的，会
*/
create table dbo.ssas_dim_usage
(
	dim_usage_type nvarchar(20),
	measure_group_id nvarchar(100),
	fact_fk_dim_column_name nvarchar(100),
	fact_fk_dim_column_name_customized nvarchar(100),--定制化dim_usage时，fact用的列，
	fact_fk_dim_column_data_type varchar(50),
	fact_fk_dim_column_data_type_customized varchar(50),--定制化dim_usage时，fact用的列的数据类型
	dimension_id nvarchar(100),
	attribute_id nvarchar(100),
	internal_dim_id nvarchar(100),
	internal_dim_attrid nvarchar(100),
	internal_measure_group_id nvarchar(100),
	is_default bit,
	is_enabled bit,
	created_at datetime NOT NULL,
	updated_at datetime NOT NULL,
	primary key(measure_group_id,dimension_id)
	--,foreign key (dimension_id) references ssas_dimension(dimension_id),
	--foreign key (measure_group_id) references ssas_measure_group(measure_group_id)
)
GO
insert into ssas_dim_usage
select
	DimUsageType as dim_usage_type,
	MeasureGroupID as measure_group_id,
	factFKDimColumnName as fact_fk_dim_column_name,
	null as fact_fk_dim_column_name_customized,
	DataType as fact_fk_dim_column_data_type,
	null as fact_fk_dim_column_data_type_customized,
	DimensionID as dimension_id,
	AttributeID as attribute_id,
	InternalDimID as internal_dim_id,
	InternalDimAttrID as internal_dim_attrid,
	InternalMeasureGroupID as internal_measure_group_id,
	1 as is_default,
	1 as is_enabled,
	getdate(),
	getdate()
from [dbo].[CLB_MetaData_DimUsage]
go
select * from ssas_dim_usage
go
if object_id('dbo.ssas_attributes_relationship') IS NOT NULL 
	drop table dbo.ssas_attributes_relationship
GO
/*
删除：
	默认的会，用逻辑删除,is_enabled=0
	定制化的会,物理删除所有
修改：
	所有的修改，都通过删除和增加
增加：
	定制化的，会
*/
create table dbo.ssas_attributes_relationship
(
	relationship_id int identity(1,1), 
	dimension_id nvarchar(100),
	based_attribute_id nvarchar(100),
	related_attribute_id nvarchar(100),
	relationship_type varchar(100) ,
	is_default bit,
	is_enabled bit,
	created_at datetime NOT NULL,
	updated_at datetime NOT NULL,
	primary key(relationship_id)
	
	--,foreign key (dimension_id) references ssas_dimension(dimension_id),
	--foreign key (based_attribute_id) references ssas_attributes(attribute_id)
)
GO
insert into ssas_attributes_relationship
select
	DimensionID as dimension_id,
	BasedAttributeID as based_attribute_id,
	RelatedAttributeID as related_attribute_id,
	RelationShipType as relationship_type,
	1 as is_default,
	1 as is_enabled,
	getdate(),
	getdate()
from [dbo].[CLB_MetaData_Attributes_RelationShip]
go
select * from ssas_attributes_relationship
go
if object_id('dbo.ssas_attributes_hierarchies') IS NOT NULL 
	drop table dbo.ssas_attributes_hierarchies
GO
/*
删除：
	默认的会，用逻辑删除,is_enabled=0
	定制化的会,物理删除所有
修改：
	所有的修改，都通过删除和增加
增加：
	定制化的，会
*/
create table dbo.ssas_attributes_hierarchies
(
	hierarchies_id int identity(1,1), 
	dimension_id nvarchar(100),
	hierarchy_name nvarchar(100),
	level_id SMALLINT,
	level_name varchar(100) ,
	source_attribute_id nvarchar(100) ,
	is_default bit,
	is_enabled bit,
	created_at datetime NOT NULL,
	updated_at datetime NOT NULL,
	primary key(hierarchies_id)
	--,foreign key (dimension_id) references ssas_dimension(dimension_id),
	--foreign key (source_attribute_id) references ssas_attributes(attribute_id)
)
GO
insert into ssas_attributes_hierarchies
select 
	DimensionID as dimension_id,
	HierarchyName as hierarchy_name,
	LevelID as level_id,
	LevelName as level_name,
	SourceAttributeID as source_attribute_id,
	1 as is_default,
	1 as is_enabled,
	getdate(),
	getdate()
from [dbo].[CLB_MetaData_Attributes_Hierarchies]
go
select * from ssas_attributes_hierarchies
go
if object_id('dbo.ssas_aggregation_design') IS NOT NULL 
	drop table dbo.ssas_aggregation_design
GO
Create Table ssas_aggregation_design 
(
	aggregation_name nvarchar(200),
	aggregation_design_name nvarchar(200),
	[description] nvarchar(500),
	is_enabled bit,
	is_default bit,
	created_at datetime NOT NULL,
	updated_at datetime NOT NULL,
	primary key (aggregation_design_name)
)
GO
insert into ssas_aggregation_design
select
	AggregationName as aggregation_name,
	AggregationDesignName as aggregation_design_name,
	Description as [description],
	1 as is_default,
	1 as is_enabled,
	getdate(),
	getdate()
from [dbo].[CLB_MetaData_AggregationDesign]
go
select * from ssas_aggregation_design
go

if object_id('dbo.ssas_etl_module','V') IS NOT NULL 
	drop view dbo.ssas_etl_module
GO
create view dbo.ssas_etl_module as
select distinct
	ModuleName as module_name,
	Enabled as is_enabled 
from CLB_MetaData_ETL_Module
go
if object_id('dbo.ssas_aggregation_design_attribute') IS NOT NULL 
	drop table dbo.ssas_aggregation_design_attribute
GO
Create Table ssas_aggregation_design_attribute 
(
	id int identity(1,1) ,
	aggregation_name nvarchar(200),
	dimension_id nvarchar(100),
	attribute_id nvarchar(100),
	is_enabled bit,
	is_default bit,
	created_at datetime NOT NULL,
	updated_at datetime NOT NULL,
	primary key (id)
	
	--,foreign key (aggregation_name) references ssas_aggregation_design(aggregation_name),
	--foreign key (dimension_id) references ssas_dimension(dimension_id),
	--foreign key (attribute_id) references ssas_attributes(attribute_id)
)
go
insert into ssas_aggregation_design_attribute
select
	AggregationName as aggregation_name,
	DimensionID as dimension_id,
	AttributeID as attribute_id,
	1 as is_default,
	1 as is_enabled,
	getdate(),
	getdate()
from [dbo].[CLB_MetaData_AggregationDesign_Attribute]
go
select * from ssas_aggregation_design_attribute
go
--mdx 分组
if object_id('dbo.ssas_mdx_group') IS NOT NULL 
	drop table dbo.ssas_mdx_group
GO
Create Table ssas_mdx_group
(
	id int identity(1,1),
	mdx_group_name nvarchar(200),
	mdx_group_order_index int,--各个组的排序顺序
	mdx_group_order_index_customized int,--各个组的自定义排序顺序
	is_default int--是否系统默认的mdx分组
)
go
if object_id('dbo.ssas_mdx_expression') IS NOT NULL 
	drop table dbo.ssas_mdx_expression
GO
Create Table ssas_mdx_expression
(
	id int identity(1,1) ,
	measure_name nvarchar(200),--指标的名称
	expression nvarchar(max),--表达式
	expression_type_create0_assign_1_other2 int,--0:新建指标的mdx, 1:重写类型的mdx,2：其他语句
	expression_type_config0_manul_1 int,--0:根据页面配置生成的mdx, 1：手动整个mdx贴进去的
	mdx_group_name nvarchar(200),--mdx 分组
	expression_order_index int,--measure的定义顺序
	expression_order_index_customized int,--measure定制化的定义顺序
	description nvarchar(512),--指标的需求口径描述
	
	/*------以下字段先留着-----*/
	display_folder nvarchar(100),--显示文件夹
	format_string nvarchar(100),--格式化字符串
	is_visiable bit,--是否可显示
	/*------以下字段先留着-----*/
	
	is_enabled bit,
	is_default bit,
	created_at datetime NOT NULL,
	updated_at datetime NOT NULL,
	primary key (id)
)
go
if object_id('ssas_automation_deploy_log_details','U') is not null drop table ssas_automation_deploy_log_details
go
create table ssas_automation_deploy_log_details
(
	id int identity(1,1) primary key,
	message_type nvarchar(100),
	message_result nvarchar(100),
	message_description nvarchar(max),
	create_at datetime
)
if object_id('sp_ssas_automation_deploy_log_details','P') is not null drop proc sp_ssas_automation_deploy_log_details
go
create proc sp_ssas_automation_deploy_log_details(
	@message_type nvarchar(100),
	@message_result nvarchar(100),
	@message_description nvarchar(max)
)
as
begin
	insert into ssas_automation_deploy_log_details
	(
		message_type,
		message_result,
		message_description,
		create_at 
	)
	values(@message_type,@message_result,@message_description,getdate())

end
go
if object_id('sp_ssas_cube_over_view','P') is not null drop proc sp_ssas_cube_over_view
go
create proc sp_ssas_cube_over_view(
	@dimension_name nvarchar(100),
	@measure_group_name_or_measure_name nvarchar(100),
	@cube_type bit=0
)
as
begin

declare @filter_query nvarchar(max)=''
set @filter_query=' mg.is_real_time='+ltrim(@cube_type)+' and dim.dimension_name LIKE ''%'+isnull(@dimension_name,'')+'%'''+
                  ' and (mea.measure_name LIKE ''%'+isnull(@measure_group_name_or_measure_name,'')+'%'''+
				  ' or mg.measure_group_name LIKE ''%'+isnull(@measure_group_name_or_measure_name,'')+'%'')'

declare @cube_over_view_sql nvarchar(max)=''
set @cube_over_view_sql='
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
                       ON module.module_name = Replace(mg.dsv_schema_name, ''olap_'', '''')
                          AND module.is_enabled = 1
        WHERE  '+@filter_query+') AS fact 
'
print @cube_over_view_sql
exec(@cube_over_view_sql)
end
go