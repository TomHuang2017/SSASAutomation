
/*
ssas cube�����Ĭ�ϵ�ֻ�н��ú����ã����߼�ɾ����
			 ���������Ĺ��ܣ�����ɾ����������ɾ��
*/
if object_id('dbo.ssas_dsv') IS NOT NULL 
	drop table dbo.ssas_dsv
GO
/*
ɾ����
	Ĭ�ϵĲ��ᣬ���ţ�ֻ������Ϊ�գ�
	���ƻ��Ļᣬ����ɾ��
�޸ģ�
	Ĭ�ϵĲ��ᣬ
	���ƻ��ģ���
���ӣ�
	���ƻ��ģ���
*/
create table dbo.ssas_dsv
(
	dsv_schema_name nvarchar(100), 
	db_table_name nvarchar(100),--table name and friendly name
	table_type nvarchar(50),--view or table
	is_named_query bit,
	depended_fact_table NVARCHAR(200),
	is_default bit,--�½���Ϊ0��Ĭ�ϵ�Ϊ1
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
ɾ����
	Ĭ�ϻᣬ��is_enable=0���߼�ɾ���������partition ���� and 1=2
	���ƻ��Ļᣬ����ɾ��!
�޸ģ�
	Ĭ�ϵĲ��ᣬ
	���ƻ��ģ���
���ӣ�
	���ƻ��ģ���
*/
create table dbo.ssas_measure_group
(
	measure_group_id nvarchar(100),--Ӣ����
	measure_group_name nvarchar(100),--������
	dsv_schema_name nvarchar(100),
	aggregation_design_name nvarchar(100),
	key_not_found_action nvarchar(100) null,
	is_rolap_mg bit,
	is_real_time bit,--�Ƿ�ʵʱ����
	is_default bit,--�Ƿ�Ĭ�ϣ��½���Ϊ0��Ĭ�ϵ�Ϊ1
	is_enabled bit,--���Խ���һЩû��Ҫ�Ķ���ֵ�飬���cube process�����ܣ����ÿ����ǲ���������Ҫ����measure������������������measure���У����߸���partition��where������1=2
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
ɾ����
	Ĭ�ϲ���
	���ƻ��Ļᣬ�ᣬ����ɾ������ɾ����Ӧ������������attribute�Լ�dim uage!
�޸ģ�
	Ĭ�ϵĲ��ᣬ
	���ƻ��ģ���
���ӣ�
	���ƻ��ģ���
*/
create table dbo.ssas_dimension
(
	dimension_id nvarchar(100),--Ӣ����
	dimension_name nvarchar(100),--������
	dsv_schema_name nvarchar(100),
	dimension_type nvarchar(20),--regular,time,account etc..
	is_default bit ,--Ĭ��Ϊ1���¼ӵ�Ϊ0����֧��ɾ��
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
ɾ����
	Ĭ�ϲ��ᣬҪɾ���ͽ���measure group����partition ��and 1=2
	���ƻ��Ļᣬ�ᣬ����ɾ�����Լ�������Ӧ��depended calcualted measures
�޸ģ�
	Ĭ�ϵĲ��ᣬ
	���ƻ��ģ���
���ӣ�
	���ƻ��ģ���
*/
create table dbo.ssas_measures
(
	measure_name nvarchar(255),--������
	measure_id nvarchar(255),--Ӣ����
	display_folder nvarchar(255),--��ʾ�ļ���
	format_string nvarchar(20),--��ʽ
	is_default bit,--Ĭ�ϵ���1���¼ӵ���0
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
 /*���ñ�*/
if object_id('dbo.ssas_measures_mapping') IS NOT NULL 
	drop table dbo.ssas_measures_mapping
GO
/*
ɾ����
	Ĭ�ϲ��ᣬҪ�Ļ���ͨ������measuregroup ��partition ��1=2
	���ƻ��Ļ�,����ɾ������
�޸ģ�
	Ĭ�ϵĻᣬͨ���޸�customized�ֶΣ�
	���ƻ��Ļᣬͨ���޸�customized�ֶ�
���ӣ�
	���ƻ��ģ���
*/
create table dbo.ssas_measures_mapping
(
	measure_id nvarchar(255),--Ӣ����
	measure_data_type nvarchar(100),--Measure����������
	measure_group_id  nvarchar(100),--��Ӧmeasure_groupID����
	db_column_default nvarchar(255),--��ӦDB�е���
	db_column_customized nvarchar(255),--��ӦDB�е���-���ƻ��޸�
	aggregation_function_default nvarchar(50),--�ۺϷ�ʽ
	aggregation_function_customized nvarchar(50),--���ƻ��ۺϷ�ʽ
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
ɾ����
	Ĭ�ϲ��ᣬҪ�Ļ���ͨ������measuregroup ��partition ��1=2
	���ƻ��Ļ�,����ɾ������
�޸ģ�
	Ĭ�ϵĻᣬͨ���޸�customized�ֶΣ�
	���ƻ��Ļᣬͨ���޸�customized�ֶ�
���ӣ�
	���ƻ��ģ���
*/
create table dbo.ssas_measures_description
(
	measure_id nvarchar(255),--Ӣ����
	customer_name nvarchar(255),--�ͻ�����
	description nvarchar(500),--��ʽ����ΪCore Measure�����Core���ɡ�
	description_customized nvarchar(500),--���ƻ�������ʽ����ΪCore Measure�����Core���ɡ�
	is_default bit,--�¼ӵ�Ϊ0��Ĭ�ϵ�Ϊ1
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
ɾ����
	Ĭ�ϲ���
	���ƻ��Ļ�,����ɾ������
�޸ģ�
	Ĭ�ϵĻᣬͨ���޸�customized�ֶΣ�
	���ƻ��Ļᣬͨ���޸�customized�ֶ�
���ӣ�
	���ƻ��ģ���
*/
create table dbo.ssas_attributes
(
	attribute_id nvarchar(100) ,--Ӣ����
	attribute_name nvarchar(100),--Ĭ�ϵ�������
	attribute_name_customized nvarchar(100),--���ƻ�attribute����ʾ��
	dimension_id nvarchar(100),
	key_column_db_column nvarchar(100),--Ĭ�ϵ�ӳ����,Key Column
	key_column_db_column_customized nvarchar(100),--���ƻ�ӳ�䵽db����У�key Column
	key_column_oledb_type nvarchar(20),--Ĭ�ϵ��������ͣ�key column��type
	key_column_oledb_type_customized nvarchar(20),--���ƻ��������ͣ�key column��type
	attribute_usage nvarchar(20),
	name_column nvarchar(100),--Ĭ�ϵ�ֵ��,typeĬ��Ϊwchar
	name_column_customized nvarchar(100),--���ƻ���ʾֵ������,typeĬ��Ϊwchar
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
ɾ����
	Ĭ�ϵĻᣬ���߼�ɾ��,is_enabled=0
	���ƻ��Ļ�,����ɾ������
�޸ģ�
	Ĭ�ϵĻᣬͨ���޸�customized�ֶΣ�
	���ƻ��Ļᣬͨ���޸�customized�ֶ�
���ӣ�
	���ƻ��ģ���
*/
create table dbo.ssas_dim_usage
(
	dim_usage_type nvarchar(20),
	measure_group_id nvarchar(100),
	fact_fk_dim_column_name nvarchar(100),
	fact_fk_dim_column_name_customized nvarchar(100),--���ƻ�dim_usageʱ��fact�õ��У�
	fact_fk_dim_column_data_type varchar(50),
	fact_fk_dim_column_data_type_customized varchar(50),--���ƻ�dim_usageʱ��fact�õ��е���������
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
ɾ����
	Ĭ�ϵĻᣬ���߼�ɾ��,is_enabled=0
	���ƻ��Ļ�,����ɾ������
�޸ģ�
	���е��޸ģ���ͨ��ɾ��������
���ӣ�
	���ƻ��ģ���
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
ɾ����
	Ĭ�ϵĻᣬ���߼�ɾ��,is_enabled=0
	���ƻ��Ļ�,����ɾ������
�޸ģ�
	���е��޸ģ���ͨ��ɾ��������
���ӣ�
	���ƻ��ģ���
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
--mdx ����
if object_id('dbo.ssas_mdx_group') IS NOT NULL 
	drop table dbo.ssas_mdx_group
GO
Create Table ssas_mdx_group
(
	id int identity(1,1),
	mdx_group_name nvarchar(200),
	mdx_group_order_index int,--�����������˳��
	mdx_group_order_index_customized int,--��������Զ�������˳��
	is_default int--�Ƿ�ϵͳĬ�ϵ�mdx����
)
go
if object_id('dbo.ssas_mdx_expression') IS NOT NULL 
	drop table dbo.ssas_mdx_expression
GO
Create Table ssas_mdx_expression
(
	id int identity(1,1) ,
	measure_name nvarchar(200),--ָ�������
	expression nvarchar(max),--���ʽ
	expression_type_create0_assign_1_other2 int,--0:�½�ָ���mdx, 1:��д���͵�mdx,2���������
	expression_type_config0_manul_1 int,--0:����ҳ���������ɵ�mdx, 1���ֶ�����mdx����ȥ��
	mdx_group_name nvarchar(200),--mdx ����
	expression_order_index int,--measure�Ķ���˳��
	expression_order_index_customized int,--measure���ƻ��Ķ���˳��
	description nvarchar(512),--ָ�������ھ�����
	
	/*------�����ֶ�������-----*/
	display_folder nvarchar(100),--��ʾ�ļ���
	format_string nvarchar(100),--��ʽ���ַ���
	is_visiable bit,--�Ƿ����ʾ
	/*------�����ֶ�������-----*/
	
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